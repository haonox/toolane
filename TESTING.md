# Toolane 当前测试说明

本文记录仓库目前实际采用的测试方式。测试分为 Go 自动化测试、调度器微基准，以及通过 HTTP 发起完整工具调用的压测。压测的详细参数和结果字段见 [benchmarks/README.md](benchmarks/README.md)。

## 1. 日常自动化测试

在项目根目录运行。需要 `go.mod` 指定的 Go 1.27.1；项目使用 `go-sqlite3`，构建时还需要可用的 C 编译器。Go 测试会在临时目录中创建 SQLite 数据库，不需要预先安装 SQLite 命令行程序，也不需要先启动 Toolane 服务。

```bash
go test ./... -count=1
go vet ./...
```

`go test ./... -count=1` 运行所有包的测试，并关闭 Go 测试结果缓存；`go vet ./...` 做静态检查。需要额外检查并发访问时，可以在本地运行：

```bash
go test -race ./...
```

测试按代码层次覆盖以下行为：

| 位置 | 当前主要验证内容 |
| --- | --- |
| `internal/tool/` | 工具注册、策略和 JSON 参数校验 |
| `internal/executor/process/` | 可信进程注册、输出及退出码、超时、取消、子进程清理和资源释放 |
| `internal/execution/` | 队列容量、Worker 上限、租户轮询与公平性、超时及取消竞争、结果限制 |
| `internal/storage/sqlite/` | 记录读写、状态条件更新、迁移、数据库独占、权限、同步策略、重新打开后的读取和到期清理 |
| `internal/execution/persistence_test.go` | 重启恢复、终态结果保留、幂等提交、存储故障后的准入行为 |
| `internal/httpapi/` | Token 授权、租户隔离、请求校验、幂等冲突及日志字段 |
| `cmd/toolane/` | 配置校验、HTTP 服务启停、已注册 Process 工具的端到端调用 |
| `internal/observability/` | 指标标签和状态转换耗时 |
| `benchmarks/http/` | 压测客户端的租户分配、热点流量和固定到达率统计 |

这些测试使用临时文件、测试内启动的进程或 HTTP 服务验证行为；其中 `cmd/toolane/` 的测试会实际启动服务并调用工具。它们检查功能正确性，不给出可承载的租户数或吞吐量结论。

## 2. GitHub Actions

[`.github/workflows/test.yml`](.github/workflows/test.yml) 在 `push` 和 `pull_request` 时触发，在 `ubuntu-latest` 上通过 `go.mod` 安装对应 Go 版本，然后依次执行：

```bash
go test ./... -count=1
go vet ./...
python3 -m unittest discover -s benchmarks -p 'test_*.py'
```

Python 测试检查可选压测工具及本地 SQLite 样本生成，不启动服务或发起完整压测。当前 CI 不运行 `-race`、调度器微基准或完整 HTTP 压测。压测结果依赖机器资源和负载环境，需单独执行和记录。

## 3. 调度器微基准

需要观察调度索引的单次取任务开销时，可运行：

```bash
go test ./internal/execution -run '^$' -bench '^BenchmarkTenantClaims$' -benchmem
```

`BenchmarkTenantClaims` 构造多租户积压队列，只计时取任务、占用运行名额和释放名额的过程；准备队列不计入计时。它用于比较调度器改动前后的开销，不包含 HTTP、SQLite、工具进程或真实请求负载，因此不能当作服务吞吐量。

## 4. 完整 HTTP 压测

压测从 HTTP 业务接口提交任务，使用租户 Token 认证，持续查询 `execution_id`，直到得到终态并校验结果。每次调用都会经过准入、持久化、租户调度、Worker 和真实 Python 工具进程。四类工具分别模拟文本处理、CPU 哈希、等待和较大结果输出；负载及其权重定义在 `benchmarks/config/workload.json`。

当前可直接运行的矩阵是**单机压测**：`benchmarks/run.py` 在本机生成私有 Token 和服务配置，编译服务端与 Go 压测客户端，启动服务，依次执行阶段，保存结果，最后停止服务。需要 Go、C 编译器和 Python 3。运行前确认 `127.0.0.1:18080` 与 `127.0.0.1:19090` 未被占用。

```bash
python3 benchmarks/run.py --quick
python3 benchmarks/run.py
```

`--quick` 让配置中的每个租户各完成一次调用，用来检查整条链路；它不是性能数据。完整矩阵使用 1,000 个租户、48 个 Worker、4,096 个全局排队位置，共计划 198,000 次调用。阶段包括 64 至 6,144 并发的阶梯、每租户 100 次调用、固定参数基线、10% 租户承担约 80% 调用的热点负载，以及按参考完成速率 1.5 倍发请求的固定到达率负载。具体顺序和次数以 `benchmarks/config/stages.json` 为准；服务容量以 `benchmarks/config/profile.json` 为准。

每轮结果位于 `benchmarks/generated/results/<运行时间>/`。目录包含运行配置和 Git 修订信息、服务日志、每阶段的 JSON 统计，以及阶段前后的 Prometheus 指标快照；私有配置、Token 和 SQLite 数据库也在 `benchmarks/generated/` 下，这些内容不提交到 Git。每轮使用新的 SQLite 数据库，避免旧任务影响测量。

解读结果时，至少一起看成功吞吐量、端到端 p95/p99、排队时间、HTTP `202`/`429`、终态错误和超时。固定到达率阶段还需看客户端的到达延迟及 `client_capacity_drops`：若客户端本身发不出计划请求，就不能据此判断服务端容量。单机压测的服务端与客户端共享 CPU、内存和网络，数值不能直接代表独立服务器的容量。

## 5. 双机测试的当前状态

项目已经提供可独立编译的 Go HTTP 压测客户端 `./benchmarks/http`，可在另一台机器上指定服务地址、私有 Token 配置、负载文件、请求数和并发数进行测试；操作方法见 [benchmarks/README.md](benchmarks/README.md)。目前 `benchmarks/run.py` 只编排单机测试，**双机部署、启动、指标采集和分阶段矩阵尚未自动化**。因此在实验室服务器上运行服务、由工作站发压时，需要分别启动服务端和客户端，并记录两台机器的资源、网络条件与配置，不能把它标为现有单机脚本的结果。
