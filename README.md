# profilr

数据剖析与漂移监控服务的离线半成品。主流程读取 CSV、全表扫描、生成行数/缺失率基线，并以批次漂移驱动**带生命周期的告警视图**；分布、相关性、PSI、采样和 HTTP 查询模块均为标准库实现，无需运行期依赖。

```bash
make test     # 单测（含并发、抑制窗口、幂等、HTTP 端到端）
make demo     # 生成两批数据并演示完整告警生命周期
make verify   # 断言 profile、漂移、生命周期与事件审计链
```

`requirements.txt` 记录目标技术栈版本；本地 demo 使用 Python 标准库和 SQLite，避免运行期访问公网或下载依赖。

## 告警生命周期

旧实现 `notify.merge_alerts` 只在单批次内按 `(field, metric)` 去重，同一漂移会在连续批次里反复刷屏。现在每一类漂移在 `profilr/alerting.py` 中对应**一条**持续更新的告警记录，状态机如下：

| 状态 | 含义 | 进入条件 |
| --- | --- | --- |
| `new` | 新漂移 | 该身份首次出现且 `delta >= threshold` |
| `active` | 持续漂移 | 后续批次仍超阈值（`new` 的下一批，或恢复后再次漂移） |
| `suppressed` | 已抑制 | 运维调用抑制接口，窗口结束前不再告警，但仍累计出现次数 |
| `recovered` | 已恢复 | 最新观测低于阈值（抑制中也可恢复），`recovered_at` 留痕 |

同一身份恢复后再次漂移会**重开同一条记录**（`reopen_count+1`，状态回到 `active` 而不是 `new`）。确认（acknowledge）只写 `acknowledged_at` 时间戳，不改变状态，避免“确认即静默”掩盖持续漂移。

### 关键设计决策

- **稳定身份**：自然键 `(table_name, field, metric)`，不含版本号或阈值；`alert_id` 是该键的确定性 SHA-256 哈希，跨基线版本、阈值调整和批次重放保持不变，且可在不同环境间移植。
- **阈值变化不重开抑制告警**：重开只由两个事件驱动——抑制窗口到期且新观测仍超阈值，或已恢复的身份再次漂移。每次观测使用的阈值会快照到告警行和事件表，历史判定可解释。
- **重复批次与并发**：每次写入带 `batch_id`（可显式传入，否则由表名、版本和观测内容确定性派生），`ingest_batches` 上的唯一约束保证重放返回 `duplicate: true` 且不产生任何副作用。所有写入在单个 `BEGIN IMMEDIATE` 事务内完成，自然键上有 UNIQUE 约束；并发写在 SQLite 写锁上串行化，只更新同一行，不会出现多条活动记录。
- **基线版本不一致**：告警行快照产生时的 `baseline_version` / `observed_version`，基线更新不会改写历史告警。查询结果附带 `latest_baseline_version`、`baseline_stale`（基于旧基线计算）和 `baseline_missing`（引用的基线已被清理）三个解释字段；下一批次会自动按当前基线评估并自然恢复/重开。
- **缺表与分页语义**：显式查询不存在的表返回 `404` JSON；表存在但无匹配告警返回 `200` + 空列表。分页用 `limit`（1–200，默认 50）和 `offset`，响应含 `count` / `has_more`，非法参数返回 `400`。
- **审计**：`alert_events` 记录 `opened / repeated / recovered / reopened / suppressed / reactivated / acknowledged` 全部状态迁移，可通过 `GET /alerts/<id>/events` 读取。

## HTTP 接口

`python3 -c "from profilr.api import serve; serve('/tmp/profilr/profilr.db', 9800)"`

| 方法与路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 |
| `GET /tables` | 已知表清单（基线表 + 告警表的并集） |
| `GET /baseline/<table>?version=N` | 基线载荷（兼容旧接口，默认最新版；缺表 404） |
| `GET /alerts?table=&field=&metric=&status=&since=&until=&time_field=&limit=&offset=` | 生命周期查询，`status` 支持逗号分隔多值，`time_field` 可选 `first_seen_at`/`last_seen_at`/`recovered_at` |
| `GET /alerts/<id>` | 单条告警（含版本解释与 `suppression_active`） |
| `GET /alerts/<id>/events` | 状态迁移审计链 |
| `POST /alerts/<id>/acknowledge` | 记录确认时间，状态不变 |
| `POST /alerts/<id>/suppress` | body `{"until": "ISO-8601 时间戳"}`，窗口结束前保持抑制 |

错误响应统一为 `{"error": "...", "status": 4xx}`。查询模块只读；两个 POST 仅用于确认/抑制这两个生命周期动作。

## 存储结构

告警库与基线库共享同一个 SQLite 文件：`baselines` 表结构保持不变（旧数据无需迁移），新增 `alerts`、`alert_events`、`ingest_batches` 三张表，由 `AlertStore` 在建库时自动创建。
