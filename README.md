# Toolane

**面向 Agent 的通用工具调度与隔离执行引擎，使用 Go 开发。**

Toolane 负责接收工具任务、限制并发、组织显式依赖、管理执行状态，并通过可替换的 Executor 执行任务。Agent 或调用方负责决定调用什么工具及其参数。

项目名由 Tool 与 Lane 组合。GitHub 仓库名称：`toolane`。

> V0.1 的单机闭环已可运行，Linux 自动化验收已覆盖 ACC-01～07。V0.2 正在分批实现：当前包含进程内 DAG 工作流，租户配额、公平调度及对比实验尚未实现。

## 项目目标

- **调度**：有界队列、并发配额、DAG 依赖与租户公平调度。
- **执行**：统一 Executor，支持 Mock、可信本地进程与 Docker 沙箱。
- **可靠性**：明确的状态机、超时取消、输出限制、资源回收与重启协调。
- **服务接口**：HTTP 提交、查询、取消与 SSE 事件，适配不同语言的 Agent。
- **工程验证**：并发测试、隔离与故障测试、可复现的性能实验。

## 开发路线

| 阶段 | 重点 |
| --- | --- |
| V0.1 | 单机执行闭环：Mock/Process、Worker、状态、超时与取消 |
| V0.2 | DAG、租户配额、公平调度与基线对比 |
| V0.3 | Docker 沙箱、资源与权限限制、清理与隔离验证 |
| V1.0 | HTTP、SQLite、幂等提交、重启协调、SSE 与可观测性 |

默认从 **V0.1 / M0.1** 开始：先完成工具注册、Mock Executor、有界队列、Worker、状态查询、取消、超时与 CLI 演示，再接入可信 Process Executor。

## 当前可运行内容

需要 Go 1.27.1。运行本地 Mock 或可信进程演示：

```bash
go run ./cmd/toolane
go run ./cmd/toolane process
```

当前可在 Go 进程内注册工具并调用 `Submit`、`Get`、`Cancel` 和 `Close`。Manager 使用固定 Worker、有界 FIFO 队列与一个排队期限监视器；运行取消或超时后，等待 Executor 返回才释放槽位。默认限制为 4 个 Worker、256 个待启动任务、30 秒排队期限、10 秒执行期限、256 KiB 参数、64 KiB 结构化结果、stdout/stderr 共享 1 MiB 原始字节预算、256 字节 Agent ID、4 KiB 错误摘要，以及保留 1000 个终态记录。请求可在配置上限内收紧期限和输出预算。

进程内调用方现在也可以使用 `SubmitWorkflow`、`GetWorkflow` 和 `CancelWorkflow` 提交、查询和取消静态 DAG。工作流默认最多 64 个节点、请求编码最多 256 KiB。整批校验并接受；等待依赖的 `blocked` 节点与 `queued` 节点共同占用待启动容量，等待期限均从接受时起算。节点失败会跳过受影响的下游，独立分支继续执行。完成的工作流和全部节点作为一组淘汰，活动工作流的已完成节点保持可查询。接口与限制见 [V0.2 DAG 说明](./docs/V0.2-DAG.md)。

注册工具时可在 `tool.Definition.Policy` 中设置更严格的排队与执行期限、输出预算、参数大小和结果大小。策略字段为零时沿用 Manager 的全局配置；若工具最大值低于全局默认值，未指定的工具默认值会收紧到该最大值。超出全局上限的工具策略会在 Manager 启动前被拒绝；任务记录保存实际采用的期限与预算。

Process Executor 只运行服务端显式注册且标为可信的绝对路径程序，用参数数组启动，不调用 Shell。Linux 下为进程建立独立进程组；取消、超时或输出达到上限时先发送 SIGTERM，宽限期后发送 SIGKILL，并限制等待继承管道的时间。若进程组停止无法确认，Manager 保留运行预留、暂停新任务准入并报告关闭错误。这个机制仅适用于可信程序，不是安全沙箱。CLI 的 `process` 演示运行系统 `printf`；没有任意命令执行入口。

运行检查：

```bash
go test ./...
go test -race ./...
go vet ./...
```

演示提供 `mock.delay` 与 `process.printf`；调用方也可在服务端注册其他可信工具。执行状态和结果仅保存在内存，进程退出后丢失；尚无 Docker、HTTP、租户公平调度或持久化。Process Executor 与真实进程测试以 Linux 为目标；其他平台会返回 `process_unsupported`。

## 项目边界

本项目不处理 LLM 推理或 Agent Planning。初期采用单机部署，不引入分布式消息队列、Kubernetes 或复杂 Web 平台。

Process Executor 只运行可信工具；Docker 版本针对常见资源滥用与权限越界提供受限执行，不承诺抵御所有恶意代码与内核漏洞。默认不自动重试，避免未知结果导致重复副作用。

## 开发文档

| 文档 | 用途 |
| --- | --- |
| [REQUIREMENTS.md](./REQUIREMENTS.md) | 项目范围、版本计划、执行语义、默认限额与验收条件 |
| [DEVELOPMENT.md](./DEVELOPMENT.md) | Go 风格、模块边界、测试要求与 AI 协作流程 |
| [docs/V0.1-ACCEPTANCE.md](./docs/V0.1-ACCEPTANCE.md) | V0.1 自动化验收范围、命令与限制 |
| [doc/V0.1-TECHNICAL-REPORT.md](./doc/V0.1-TECHNICAL-REPORT.md) | 当前实现的架构、执行语义、代码审查路线与已知边界 |
| [docs/V0.2-DAG.md](./docs/V0.2-DAG.md) | V0.2 第一批 DAG 接口、状态与验证范围 |

**协作 AI 开始工作前须读取上述两份文档与本 README。** 未指定阶段时只开发 V0.1；不要提前实现整条路线。

README 会随实际代码更新运行命令、已实现能力和验证结果。目前不提供性能数字或上线声明。
