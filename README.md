# profilr

数据剖析与漂移监控服务。主流程：剖析 CSV → 保存**版本化基线** → 在任意两个版本之间做**可重复比较** → 产出带触发原因的告警。分布、相关性、采样模块提供基础能力；本地运行只依赖 Python 标准库和 SQLite。

```bash
make test     # 运行单测
make demo     # 两批数据端到端演示（保存 v1/v2 并比较）
make verify   # 断言版本、混合指标、幂等与可重复性
```

## 版本模型

一次剖析关联四要素：**输入身份**（文件内容哈希或显式 `--input-id`）、**profile 快照**（行数/缺失率/数值样本/类别计数）、**比较口径**（spec：阈值与分箱）、**告警结果**。SQLite 表：

| 表 | 作用 |
| --- | --- |
| `profiles` | 不可变快照，按内容哈希寻址 |
| `baseline_versions` | 每表递增版本；`UNIQUE(table_name, input_identity)` 使同一批次重试幂等 |
| `baseline_heads` | 当前版本指针；回退只移动指针，历史不可变 |
| `baseline_events` | 审计：save / rollback / legacy_migrated |
| `specs` | 比较口径，按内容寻址 |
| `comparisons` | 每次比较一行：版本对 + 口径 + 原始指标 + 告警 + 状态 |

- **并发保存**：写入在 `BEGIN IMMEDIATE` 事务内串行化，多线程/多进程保存得到互不相同的连续版本号。
- **中断恢复**：比较状态为 `running → complete/failed`；进程在计算中途崩溃会留下 `running` 半成品，下次打开库时统一标记为 `interrupted`，再次比较时自动重算。
- **旧库迁移**：存在旧版 `baselines` 表时自动导入为新版本链。

## schema 事件 vs 数据漂移

- **schema 事件**：`field_added` / `field_removed` / `type_changed`。字段缺失不再被静默跳过，而是显式记录；类型变化后分布指标标记为不可比。
- **数据漂移**：对每个共有字段同时计算**混合指标**——缺失率差值、数值 PSI（分箱边界固定取自基线分位数，保证可重复）、类别分布 PSI。`evaluate_alerts` 按口径阈值把指标转成告警，每条告警带 `reason`（实际值、阈值、分箱方式）。

## 使用

```bash
# 保存版本（同一输入重复提交返回已有版本，created=false）
python3 -m profilr.cli --db /tmp/p.db profile orders data/batch_a.csv
python3 -m profilr.cli --db /tmp/p.db profile orders data/batch_b.csv --input-id batch-2026-11-15

# 比较：缺省为 head 与其前一版本；也可显式指定历史版本与最新版本
python3 -m profilr.cli --db /tmp/p.db compare orders --from 1 --to 2

# 阈值调整后，用新口径重新解释历史比较（原始指标保留，无需重新剖析）
python3 -m profilr.cli --db /tmp/p.db reevaluate orders <comparison_id> --spec relaxed.json

# 回退 head 到历史版本 / 查看版本与审计
python3 -m profilr.cli --db /tmp/p.db rollback orders --to 1
python3 -m profilr.cli --db /tmp/p.db versions orders

# 只读 HTTP 查询
python3 -m profilr.cli --db /tmp/p.db serve --port 9800
```

HTTP 路由：`GET /baseline/{table}`、`/baseline/{table}/versions`、`/baseline/{table}/events`、`/baseline/{table}/compare?from=&to=`、`/comparison/{comparison_id}`。

比较结果（CLI 与 API 一致）明确给出 `compared_versions`（比较的是哪两个版本）、`spec`（用的哪套口径）、`schema_events` 与 `field_metrics`（原始指标），每条告警的 `reason` 说明为何触发；同一（表、版本对、口径）的比较重复请求返回已存储结果（`reused=true`），保证可重复。

`requirements.txt` 记录目标技术栈版本；本地 demo 使用 Python 标准库和 SQLite，避免运行期访问公网或下载依赖。
