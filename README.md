# profilr

数据剖析与漂移监控服务的离线半成品。主流程读取 CSV、做类型感知的字段剖析（原始计数 + 类型化摘要 + 显式状态），用上一批 profile 作为 reference 对齐直方图边界并显式记录列集合变化，再按缺失率绝对差与 schema 变化触发合并告警；相关性、PSI、采样和 HTTP 查询模块提供最小接口。

```bash
make test
make demo
make verify
```

`requirements.txt` 记录目标技术栈版本；本地 demo 使用 Python 标准库和 SQLite，避免运行期访问公网或下载依赖。

## 类型感知剖析的设计决策

`profilr.profile` 把每列剖析成「原始计数 + 类型化摘要 + 显式状态」，`profilr.validate.check_profile` 校验三者互相吻合（如 `missing + present == total`、`count + invalid == present`、直方图合计 == 样本数），`make verify` 会对 `data/mixed.csv`（缺失 + 混合类型 + 极端值）强制执行。

- **缺失语义**：`""`、`null`、`none`、`na`、`n/a`、`nan`（大小写/空白不敏感）以及短行产生的 `None` 都计为 missing，不参与类型推断。
- **类型推断与降级**：非缺失值全部可解析为有限浮点 → `number`；全部不可解析 → `string`；混合时降级为多数类（平票选 `string`，文本无损），字段 `status="degraded"`，少数类计入 `invalid` 且不进摘要。`1e309`、`inf` 这类溢出/非有限值算非法数字：计入 `invalid`，不进数值摘要，因此极端值不会污染分位数和直方图。
- **分位数**：R-7 线性插值，位置 `(n-1)*q`；`q=0` 为 min、`q=1` 为 max；空输入返回 `null` 而不是编造的 0。
- **直方图跨批可比**：边界显式存在结果里；传入 `reference`（上一批 profile）时复用基线边界（`edges_source="baseline"`），出界值计入 `underflow`/`overflow`，两批直方图可逐桶对齐。无基线时按本批精确 min/max 等宽划分；常量列向两侧各扩 `max(0.5, |v|*1e-6)`。桶约定 `[e_i, e_{i+1})`，末桶闭区间。
- **内存上限**：每列数值经确定性蓄水池抽样（默认 10 万条，种子按列名固定、跨进程可复现），超限后 `approximate=true`，但 min/max/mean/count 仍精确；字符串基数默认上限 1024，超限后 `distinct` 为下界且 `distinct_exact=false`，top-k（默认 10）只统计已跟踪值。每列内存上界为 O(min(数值数, 10万) + min(基数, 1024))。
- **状态模型**：字段 `status` ∈ `ok` / `degraded`（混合或非法值）/ `all_missing` / `empty`（空文件，表头仍在）/ `dropped`（相对基线消失）；列集合变化写入 `schema.added` / `schema.dropped` / `schema.type_changed`；空文件顶层 `status="empty"`。漂移计算对 dropped/added 列产生 `schema` 告警。

```bash
python3 -m profilr.cli profile data/mixed.csv                      # 单文件剖析 + 一致性校验
python3 -m profilr.cli profile data/batch_c.csv --reference data/batch_a.csv
```
