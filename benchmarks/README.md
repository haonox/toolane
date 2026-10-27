# Toolane 独立 HTTP 压测目录

本目录包含自己的服务配置、租户 Token、四个 Process 工具、混合业务负载、加压阶段和压测程序。压测从业务 HTTP 接口提交调用，轮询执行 ID，校验终态结果；每次调用都经过认证、准入、排队、租户调度、Worker 和真实工具进程。仅需本项目的 Go 引擎源码、Go 与 Python 3 运行环境。

## 目录

| 路径 | 用途 |
| --- | --- |
| `config/profile.json` | 服务容量、1000 个租户、业务与指标端口、四个工具的注册与安全边界 |
| `config/workload.json` | 默认混合负载；四类工具各有不同运行时长、输入或结果大小档位 |
| `config/workload-baseline.json` | 原固定参数负载，用于观察稳定基线 |
| `config/stages.json` | 固定基线、并发阶梯、每租户 100 次调用、热点租户及固定到达率阶段 |
| `tools/` | 四个独立的 Python 工具程序，只接收参数并在标准输出写一个 JSON 值 |
| `prepare.py` | 生成绝对工具路径和随机租户 Token，写入私有运行配置 |
| `http/` | HTTP 压测客户端和结果统计 |
| `run.py` | 编译、启动服务、执行各阶段、保存指标、停止服务 |
| `generated/` | 私有配置、二进制、日志及结果，整个目录不纳入 Git |

## 本机压测

从项目根目录运行：

```bash
python3 benchmarks/run.py
```

首次运行会生成 `benchmarks/generated/server.json`，权限为 `600`。配置变化时，旧私有配置会在 `generated/` 中备份，再生成新的随机 Token。脚本使用独立的 `127.0.0.1:18080` 业务端口和 `127.0.0.1:19090` 指标端口，依次执行 `config/stages.json` 中的全部阶段。每轮保存 JSON 结果、指标端口前后快照和服务日志。运行 `python3 benchmarks/run.py --quick` 会让每个租户各调用一次，用于验证 Token 与 HTTP 链路；它不构成性能测试。

`profile.json` 针对本机可用的 6 个 CPU、约 15 GiB 内存设置 48 个 Worker、4096 个全局排队位置、1000 个租户和 50000 条保留结果。它是加压起点，不代表硬件的理论最大值。默认矩阵计划 198000 次调用；其中 `tenant-volume-100k` 阶段由每个租户提交恰好 100 次工具调用，总计 100000 次。固定参数的 `baseline-fixed` 与混合参数的 `ramp-512` 相邻，均为 512 并发、8000 次调用，便于比较负载组成的影响。`tenant-skew-80-10` 假设 10% 的租户承担约 80% 的调用，用来观察热点租户对其他租户的影响。`open-arrival-150pct` 根据前面 `ramp-512` 阶段的成功完成速率，按其 1.5 倍固定速率安排调用；它验证到达率超过参考吞吐时的准入和排队行为。阶段会打印实际参与的租户数、每租户调用次数范围、成功吞吐量、端到端 p99、接收量、`429`、客户端容量丢弃量和错误数。出现非预期错误或客户端容量丢弃时停止后续阶段，并保留现场数据。

调整服务容量与租户数量请修改 `config/profile.json`；再次运行时脚本会备份旧的私有配置，并按新配置生成 Token。调整工具比例和参数请修改 `config/workload.json`；调整压力量级请修改 `config/stages.json`。普通阶段的 `concurrency` 是压测端同时进行的完整调用数：一次调用结束后，空出的并发位置才会提交下一次。带 `arrival_reference` 的阶段按独立于完成速度的固定速率安排调用，`concurrency` 则是压测端允许的最大在途调用数。若在途数达到上限，请求记为 `client_capacity_drops`，没有提交给 Toolane；这种阶段不能用于判断服务端容量，应增大压测端并发上限或降低到达率后重测。引擎按租户调度工具调用，不配置 Agent 数；本压测也不发送可选的 `agent_id`。请同时查看 p99、完成吞吐量、HTTP `429`、工具失败、排队时间和指标快照，不能只看成功吞吐量。

这里把一个独立 Token 对应的调度主体称为一个租户。真实系统中，一个租户也可能是企业客户，而该企业的许多终端用户共享同一租户；因此压测中的 1000 租户不能直接表述成 1000 个真实终端用户。普通阶段按租户顺序均匀提交；热点阶段使用固定伪随机分布，前 10% 为热点租户，其余为低频租户。`tenant-volume-100k` 会持续运行数分钟，并产生大量服务日志；运行前请确认磁盘剩余空间。

## 四种业务负载

| 工具 | 类型 | 默认压力点 |
| --- | --- | --- |
| `bench.text` | 短、中、长三档文本 | HTTP、调度、进程启动与输入大小 |
| `bench.hash` | 1000、6000、16000 轮 SHA-256 | 不同强度的 CPU 竞争 |
| `bench.lookup` | 等待 10、80、300 ms | 不同时长的 Worker 占用与排队 |
| `bench.report` | 返回 8、64、128 条结构化记录 | 不同大小结果的保存、序列化与 HTTP 传输 |

默认各工具的总权重仍为 40% 文本、20% CPU、30% 等待、10% 报告；每类内部按 `config/workload.json` 的权重选择不同档位。档位和热点租户比例是可调整的业务假设，不代表已测得的生产分布。所有选择都由调用序号确定，同一配置可以重复对照。工具使用确定性的本地逻辑；`bench.lookup` 的等待不包含网络连接、远端限流或远端失败。每次调用都启动真实的 Python 工具进程，因此包含进程启动成本。压测程序校验返回结果中的工具名称和执行 ID；错误或缺失结果会计入 `errors`。

## 结果与解释

每轮结果在 `benchmarks/generated/results/<运行时间>/`。`metadata.json` 记录 CPU 数、服务配置、两份负载配置、Git 修订及工作区是否有改动；`server.log` 记录服务状态转换；每阶段的 `.json` 是客户端摘要，`-before.prom` 和 `-after.prom` 是指标端口快照。

| JSON 字段 | 含义 |
| --- | --- |
| `attempted_per_second`、`succeeded_per_second` | 完整阶段内的提交尝试速率与成功完成速率 |
| `admission_http_status` | 提交 HTTP 状态；`202` 为接收，`429` 为容量限制 |
| `final_status`、`results_present`、`errors` | 最终状态、带结果的数量及各类错误 |
| `per_tenant`、`per_tool` | 各租户及各工具的提交、接收、拒绝和完成数量 |
| `per_scenario`、`per_tenant_group` | 各参数档位及热点/低频租户组的数量、排队与执行延迟 |
| `arrival_rate_per_second`、`arrival_lag`、`client_capacity_drops` | 固定到达率阶段的计划速率、客户端调度延迟与未发出的调用 |
| `submit_latency`、`end_to_end_latency` | 客户端提交延迟与完整调用延迟，含轮询间隔 |
| `queue_latency`、`execution_time` | 服务端排队时间与运行时间 |

延迟提供 p50、p95、p99，仅统计获得相应时间戳的调用。因此报告结论时必须同时列出请求总数、`429`、超时和错误。服务端与压测端在同一台机器上，会争用 CPU、内存和网络；本机结果体现的是同机完整系统表现。若以后要测独立压测机和云服务器，可直接构建本目录的 `http/` 客户端，传入该环境的 `-base-url`、私有 `-tokens-file`、`-workload-file`、`-requests` 和 `-concurrency`。Toolane 仅监听回环地址，跨机器访问需要部署入口代理。

普通阶段采用闭环模式，最后的 `open-arrival-150pct` 使用固定到达率。`attempted_per_second` 按整个阶段的实际 HTTP 提交量计算，包含最后等待在途调用完成的时间；固定到达率请看 `arrival_rate_per_second`，并同时检查 `arrival_lag` 与 `client_capacity_drops`。若日后得到真实工具的耗时、输入输出大小或租户活跃度分布，应据此调整 `workload.json` 与 `stages.json` 后再报告容量结论。
