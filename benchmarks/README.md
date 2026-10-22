# 可复现的本地实验

`run_real_tools.py` 通过正常的 HTTP 和 SQLite 路径运行三个可信的 Process 测试工具。对于短时测试的每个场景及每次重复实验，它都会启动一个新的 Toolane 服务、一个私有状态目录、三个租户的凭据、每个租户两个模拟 Agent，以及一个受控的本机回环查询服务。输出中不保存凭据。运行环境需要 Linux 和 Python 3 标准库。

每次运行会先完成六次不计入测量的预热调用，每个模拟 Agent 一次。预热调用的 ID 和状态保存在 `warmup.json` 中。

先构建本次要测量的确切二进制文件，再选择一个新的输出目录：

```bash
go build -o /tmp/toolane-bench ./cmd/toolane
python3 benchmarks/run_real_tools.py \
  --binary /tmp/toolane-bench --out /tmp/toolane-real-smoke --profile smoke
```

CI 会运行 `smoke` 配置，检查端到端调用链、结果正确性、失败与超时传播，以及独立报告账本。它的样本量很小，**不能据此宣称吞吐能力**。

运行受控的短时测试矩阵时，应在同一台机器上使用固定的二进制文件和随机种子，至少独立重复三次：

```bash
python3 benchmarks/run_real_tools.py \
  --binary /tmp/toolane-bench --out /tmp/toolane-real-short-1 \
  --profile short --repetitions 3 --seed 20261021
```

`short` 配置先测量租户 A、单个 Agent 的直接调用与经 Toolane 串行调用基线，再覆盖按计划到达的 CPU 任务和 50/200 毫秒 I/O 任务、固定并发的 I/O 任务、耗时 1 秒的报告任务、不均衡的租户突发、超载，以及包含失败、超时和取消的混合负载。脚本同时记录计划发送时间与实际发送时间；两者的差值可显示客户端何时未能跟上计划。`summary.json` 还记录了各租户在前 24 次任务启动中的次数，用于观察可比负载下的调度公平性。实验没有预设 QPS 目标；只有主机、二进制文件、工具数据和负载参数一致时，才适合比较结果。

初步持续运行时，每次重复实验使用一个始终运行的服务。默认时长为 1,800 秒：

```bash
python3 benchmarks/run_real_tools.py \
  --binary /tmp/toolane-bench --out /tmp/toolane-real-soak-1 \
  --profile soak --duration-seconds 1800
```

每个输出目录包含 `metadata.json`、`summary.json`、`REPORT.md`，以及记录产物 SHA-256 的 `manifest.json`。`runs/<scenario>-r<repetition>/` 下保存：

- `executions.csv`：每个计划任务的输入、提交时间、响应、最终状态，以及分别计算的排队、运行和端到端时长；
- `resources.csv` 和 `minutes.csv`：服务 CPU/RSS、调度器指标、SQLite 主文件与 WAL 文件大小，以及每分钟完成数和 p95 时长；
- `ledger.jsonl`、成功生成的 `reports/*.json`，以及本地服务日志。

运行器会校验真实工具的输出，并将每份成功报告的 SHA-256 与独立账本核对。出现意外状态，或外部副作用缺失、重复时，实验会失败。`REPORT.md` 列出测试覆盖的边界。只有命令成功退出，才应保留其结果用于分析；失败的运行可能留下不完整的诊断文件。

`run_http_service.py` 和 `http_service.py` 测量的是 Mock 任务，不能将其吞吐量直接与 Process 测试工具比较。当前 HTTP 服务只提供租户轮询调度；如需比较 FIFO，应使用内部调度器基准，并说明两者测量范围不同。
