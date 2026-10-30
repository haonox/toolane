# Toolane 独立 HTTP 压测目录

本目录包含自己的服务配置、租户 Token、四个 Process 工具、混合业务负载、加压阶段和压测程序。压测从业务 HTTP 接口提交调用，轮询执行 ID，校验终态结果；每次调用都经过认证、准入、排队、租户调度、Worker 和真实工具进程。仅需本项目的 Go 引擎源码、Go 与 Python 3 运行环境。

压测场景、判定标准与结果适用范围见 [引擎压测方案](PLAN.md)。

## 目录

| 路径 | 用途 |
| --- | --- |
| `config/profile.json` | 服务容量、1000 个租户、业务与指标端口、四个工具的注册与安全边界 |
| `config/workload.json` | 默认混合负载；四类工具各有不同运行时长、输入或结果大小档位 |
| `config/workload-baseline.json` | 原固定参数负载，用于观察稳定基线 |
| `config/stages.json` | 固定基线、并发阶梯、每租户 100 次调用、热点租户及固定到达率阶段 |
| `config/diagnostic-tools.json` | 可选的 no-op、本地 SQLite I/O 和故障工具定义 |
| `config/stages-diagnostics.json`、`config/workload-{noop,io,fault}.json` | 诊断工具的独立阶段与预期结果 |
| `tools/` | 默认四个和可选三个独立 Python 工具程序；正常完成的工具在标准输出写一个 JSON 值 |
| `prepare/` | Go 配置生成器：生成绝对工具路径和随机租户 Token，写入私有运行配置 |
| `http/` | HTTP 压测客户端和结果统计 |
| `generated/` | 私有配置、二进制、日志及结果，整个目录不纳入 Git |

## 可选诊断工具

默认配置仍只注册原来的四个工具，原有工具参数和负载比例保持不变。需要诊断工具时，生成包含额外三个工具的私有配置：

```bash
go run ./benchmarks/prepare --diagnostics
```

`--diagnostics` 只准备配置，不会启动服务或发压。它会在私有服务配置中额外注册三个工具，并生成权限为 `600` 的本地 SQLite 只读查询样本。`bench.noop` 只输出最小合法 JSON，用于观察 HTTP、持久化、调度和 Python 进程启动的合计成本；`bench.io` 每次打开样本数据库并读取 8/64/256 行，每行 1024 字节，返回读取字节数和摘要；`bench.fault` 分别生成正常结果、非零退出、运行超时和输出超限。SQLite 查询通常会受操作系统页缓存影响，因此 `bench.io` 测的是实际 SQLite 读取路径，不等于物理磁盘随机读或远端网络访问。诊断矩阵结果应与默认四工具矩阵分开比较。

诊断负载使用可选的 `expected_status`、`expected_error_code`、`expected_result` 指定预期终态和结果字段。客户端会把匹配预期的失败计入 `expected_outcomes`，只有不匹配的终态/错误码/结果进入 `errors`；`final_status` 仍保留实际成功、失败和超时数量。结果 JSON 还包含总体及各场景中带结果调用的 `result_size` 字节分位数。故障阶段的 `succeeded_per_second` 只统计真正成功的调用，不能当作故障场景的总处理速率。

## 手动准备与发压

从项目根目录生成默认四工具配置并构建服务端与客户端：

```bash
go run ./benchmarks/prepare
go build -o benchmarks/generated/toolane ./cmd/toolane
go build -o benchmarks/generated/load ./benchmarks/http
```

`prepare` 生成 `benchmarks/generated/server.json`，权限为 `600`；配置变化时会备份旧私有配置并生成新的随机 Token。它不会启动服务、创建每轮独立的数据库或执行阶段。每轮测试应使用全新目录或设置新的绝对 `database_path`，确保数据库不复用，然后手动启动服务、运行客户端并采集日志与指标。配置和结果包含 Token 与调用数据，应按敏感数据保管。默认业务端口是 `127.0.0.1:18080`，指标端口是 `127.0.0.1:19090`。

双机测试时，应在服务端运行 `prepare`，使配置内的 Python 解释器和工具路径指向服务端文件。如果服务端没有 Go，可在相同操作系统及架构的工作站构建可执行文件，再连同 `benchmarks/config/` 和 `benchmarks/tools/` 复制到服务端的压测目录。在工作站的项目根目录构建 Linux 可执行文件：

```bash
CGO_ENABLED=1 go build -tags 'netgo sqlite_omit_load_extension' -ldflags '-linkmode external -extldflags "-static"' -o benchmarks/generated/prepare ./benchmarks/prepare
```

然后在服务端压测目录内运行 `./prepare --root .`；需要诊断工具时加 `--diagnostics`。服务端仍需 Python 3 运行工具。`--root` 指定包含 `config/` 和 `tools/` 的目录；从项目根目录直接运行时默认为 `benchmarks`。

`profile.json` 设置 48 个 Worker、4096 个全局排队位置、1000 个租户和 50000 条保留结果。它是加压起点，不代表硬件的理论最大值。默认矩阵计划 198000 次调用；其中 `tenant-volume-100k` 阶段由每个租户提交恰好 100 次工具调用，总计 100000 次。固定参数的 `baseline-fixed` 与混合参数的 `ramp-512` 相邻，均为 512 并发、8000 次调用，便于比较负载组成的影响。`tenant-skew-80-10` 假设 10% 的租户承担约 80% 的调用，用来观察热点租户对其他租户的影响。`open-arrival-150pct` 应根据此前 `ramp-512` 阶段的成功完成速率，按其 1.5 倍指定客户端 `-arrival-rate`。各阶段需按 `config/stages.json` 手动运行并保存结果；出现非预期错误或客户端容量丢弃时，应停止后续阶段并保留现场数据。

调整服务容量与租户数量请修改 `config/profile.json`，然后重新运行 `prepare`。调整工具比例和参数请修改 `config/workload.json`；调整压力量级请修改 `config/stages.json`。普通阶段的 `concurrency` 是压测端同时进行的完整调用数：一次调用结束后，空出的并发位置才会提交下一次。固定到达率阶段按独立于完成速度的速率安排调用，`concurrency` 则是压测端允许的最大在途调用数。若在途数达到上限，请求记为 `client_capacity_drops`，没有提交给 Toolane；这种阶段不能用于判断服务端容量，应增大压测端并发上限或降低到达率后重测。引擎按租户调度工具调用，不配置 Agent 数；本压测也不发送可选的 `agent_id`。请同时查看 p99、完成吞吐量、HTTP `429`、工具失败、排队时间和指标快照，不能只看成功吞吐量。

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

建议将每轮结果分别保存在 `benchmarks/generated/results/<运行时间>/`，自行记录代码修订、机器资源、配置、网络路径和数据库路径。客户端通过 `-output` 保存每阶段 JSON；服务日志和阶段前后的 Prometheus 指标快照也需单独采集。

| JSON 字段 | 含义 |
| --- | --- |
| `attempted_per_second`、`succeeded_per_second` | 完整阶段内的提交尝试速率与成功完成速率 |
| `admission_http_status` | 提交 HTTP 状态；`202` 为接收，`429` 为容量限制 |
| `final_status`、`results_present`、`errors` | 最终状态、带结果的数量及各类错误 |
| `per_tenant`、`per_tool` | 各租户及各工具的提交、接收、拒绝和完成数量 |
| `per_scenario`、`per_tenant_group` | 各参数档位及热点/低频租户组的数量、排队与执行延迟 |
| `arrival_rate_per_second`、`arrival_lag`、`client_capacity_drops` | 固定到达率阶段的计划速率、客户端调度延迟与未发出的调用 |
| `expected_outcomes`、`result_size` | 与场景预期相符的终态数量、带结果调用的 JSON 字节分位数 |
| `submit_latency`、`end_to_end_latency` | 客户端提交延迟与完整调用延迟，含轮询间隔 |
| `queue_latency`、`execution_time` | 服务端排队时间与运行时间 |

延迟提供 p50、p95、p99，仅统计获得相应时间戳的调用。因此报告结论时必须同时列出请求总数、`429`、超时和错误。服务端与客户端同机运行时会争用 CPU、内存和网络，数值不能直接代表独立服务器的容量。双机测试时，可通过私网地址或经授权的 SSH 转发访问业务端口；`metrics_listen` 继续监听回环地址。在压测机上运行本目录的 `http/` 客户端，指定 `-base-url`、私有 `-tokens-file`、`-workload-file`、`-requests` 和 `-concurrency`，并分别记录两台机器的资源和网络条件。

普通阶段采用闭环模式，最后的 `open-arrival-150pct` 使用固定到达率。`attempted_per_second` 按整个阶段的实际 HTTP 提交量计算，包含最后等待在途调用完成的时间；固定到达率请看 `arrival_rate_per_second`，并同时检查 `arrival_lag` 与 `client_capacity_drops`。若日后得到真实工具的耗时、输入输出大小或租户活跃度分布，应据此调整 `workload.json` 与 `stages.json` 后再报告容量结论。
