# profilr

数据剖析与漂移监控服务的离线半成品。当前主流程读取 CSV、全表扫描、生成行数/缺失率基线，并用上一批的绝对差触发合并告警；分布、相关性、PSI、采样和 HTTP 查询模块已提供最小接口，但尚未接入完整生产调度。

```bash
make test
make demo
make verify
```

`requirements.txt` 记录目标技术栈版本；本地 demo 使用 Python 标准库和 SQLite，避免运行期访问公网或下载依赖。
