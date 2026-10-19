# Toolane

**面向 Agent 的通用工具调度与隔离执行引擎，使用 Go 开发。**

Toolane 负责接收工具任务、限制并发、组织显式依赖、管理执行状态，并通过可替换的 Executor 执行任务。Agent 或调用方负责决定调用什么工具及其参数。

项目名由 Tool 与 Lane 组合。GitHub 仓库名称：`toolane`。

> V0.1 的单机闭环已可运行，Linux 自动化验收覆盖 ACC-01～07。V0.2 的 DAG、进程内模拟租户配额、轮询调度与 FIFO 对比实验已实现，ACC-08～10 自动化验收通过。

V0.3 已接入沙箱 CPU/内存的调度预留、固定镜像的 Docker Shell Executor 和按实例归属的有界残留回收。ACC-11～12 的自动化验收已在合并后的 `main` 上通过，记录见 [V0.3 验收](./docs/V0.3-ACCEPTANCE.md)。

V1.0 的第一批增加了 [SQLite 存储基础](./docs/V1.0-SQLITE-FOUNDATION.md)：独占状态目录、版本化 schema，以及任务状态和事件的事务写入。该批先建立存储边界，单任务运行路径由第三批接入。

第二批增加了 [存储层幂等准入](./docs/V1.0-IDEMPOTENCY.md)：同一租户、提交端点和键的并发重复提交只创建一条任务记录；请求不同则冲突，24 小时后可重用键。其单任务 Manager 接入见第三批；HTTP 接口尚未实现。

第三批增加了 [单任务持久化运行路径](./docs/V1.0-DURABLE-SINGLE.md)：`NewDurableManager` 在准入、认领和状态变化时写入 SQLite，重启前先确认资源回收，再续调未认领任务并中断旧的已认领任务。该模式现可通过 Go API 使用；CLI 仍使用内存模式，HTTP/SSE 尚未实现。

第四批补充 [Docker 资源归属持久化](./docs/V1.0-DOCKER-OWNERSHIP.md)：容器创建后、启动前先记录容器 ID 与部署实例 ID；重启恢复回调收到这些记录，同时仍需扫描归属标签来覆盖 ID 尚未落盘的崩溃窗口。真实 Docker 恢复测试需在可访问 Docker daemon 的环境运行。

第五批增加 [Workflow 存储基础](./docs/V1.0-WORKFLOW-STORE.md)：一次事务写入 Workflow、节点关系、各节点 Execution 和首个事件，并可在重启后按提交顺序读取。

第六批接入 [持久化 Workflow 运行与恢复](./docs/V1.0-DURABLE-WORKFLOW.md)：`NewDurableManager` 可执行静态 DAG；节点状态、事件及工作流汇总状态落盘。重启时重建依赖，继续未认领节点，已认领节点标记为 `interrupted`，独立分支继续执行。

第七批增加 [Workflow 幂等提交](./docs/V1.0-WORKFLOW-IDEMPOTENCY.md)：`SubmitWorkflowIdempotent` 将整批 DAG 与幂等键在一个事务中提交；有效键的等价请求在容量已满或服务重启后仍返回原工作流。

第八批增加 [持久化记录保留与容量控制](./docs/V1.0-DURABLE-RETENTION.md)：默认保留终态记录和事件 7 天、最多保存 10000 条 Execution；清理保护活动任务、待清理资源与有效幂等键，Workflow 按整批删除。容量被保护记录占满时拒绝新提交。

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

当前可在 Go 进程内注册工具并调用 `Submit`、`Get`、`Cancel` 和 `Close`。Manager 使用固定 Worker、有界待启动容量、默认按租户轮询的就绪调度与一个排队期限监视器；运行取消或超时后，等待 Executor 返回才释放槽位。默认限制为 4 个 Worker、256 个全局待启动任务、每租户 2 个运行槽位和 64 个待启动节点、30 秒排队期限、10 秒执行期限、256 KiB 参数、64 KiB 结构化结果、stdout/stderr 共享 1 MiB 原始字节预算、256 字节 Agent ID、4 KiB 错误摘要，以及保留 1000 个终态记录。请求可在配置上限内收紧期限和输出预算。

进程内调用方现在也可以使用 `SubmitWorkflow`、`GetWorkflow` 和 `CancelWorkflow` 提交、查询和取消静态 DAG。工作流默认最多 64 个节点、请求编码最多 256 KiB。整批校验并接受；等待依赖的 `blocked` 节点与 `queued` 节点共同占用待启动容量，等待期限均从接受时起算。节点失败会跳过受影响的下游，独立分支继续执行。完成的工作流和全部节点作为一组淘汰，活动工作流的已完成节点保持可查询。接口与限制见 [V0.2 DAG 说明](./docs/V0.2-DAG.md)。

注册工具时可在 `tool.Definition.Policy` 中设置更严格的排队与执行期限、输出预算、参数大小和结果大小。策略字段为零时沿用 Manager 的全局配置；若工具最大值低于全局默认值，未指定的工具默认值会收紧到该最大值。超出全局上限的工具策略会在 Manager 启动前被拒绝；任务记录保存实际采用的期限与预算。

Process Executor 只运行服务端显式注册且标为可信的绝对路径程序，用参数数组启动，不调用 Shell。Linux 下为进程建立独立进程组；取消、超时或输出达到上限时先发送 SIGTERM，宽限期后发送 SIGKILL，并限制等待继承管道的时间。若进程组停止无法确认，Manager 保留运行预留、暂停新任务准入并报告关闭错误。这个机制仅适用于可信程序，不是安全沙箱。CLI 的 `process` 演示运行系统 `printf`；没有任意命令执行入口。

Docker Executor 通过进程内工具注册使用；管理员必须指定已在本地存在的镜像标签、SHA-256 digest 和部署实例 ID。它在启动前检查 Docker cgroup v2、seccomp 和实际容器配置；任务只能提交 Shell 脚本，不能指定镜像、网络、挂载或权限。容器默认无网络，以非 root 运行，使用只读根目录和有大小限制的 tmpfs 工作区。用法、策略与测试命令见 [V0.3 Docker Executor 说明](./docs/V0.3-DOCKER-EXECUTOR.md)。CLI 尚未提供 Docker 演示入口。

运行检查：

```bash
go test ./...
go test -race ./...
go vet ./...
```

演示提供 `mock.delay` 与 `process.printf`；调用方也可在服务端注册其他可信工具或 Docker Shell 工具。V0.2 的租户 ID 只由可信进程内调用方提供，不构成身份认证；可配置 `SchedulerFIFO` 作为实验基线。CLI 使用的 `NewManager` 仍只保留内存状态；单任务与 Workflow 可通过 `NewDurableManager` 使用 SQLite，但尚无 HTTP 服务。Docker 仅面向 Linux 主机；服务启动时可按实例标签回收残留容器，删除失败会有界重试，调用方须保存稳定实例 ID 并在准入前调用恢复接口。Process Executor 与真实进程测试也以 Linux 为目标；其他平台会返回 `process_unsupported`。

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
| [docs/V0.2-SCHEDULER.md](./docs/V0.2-SCHEDULER.md) | 租户配额、轮询调度、测试与 FIFO 对比实验 |
| [docs/V0.2-ACCEPTANCE.md](./docs/V0.2-ACCEPTANCE.md) | V0.2 阶段验收命令、证据与边界 |
| [docs/V0.3-RESOURCE-BUDGET.md](./docs/V0.3-RESOURCE-BUDGET.md) | V0.3 第一批的沙箱资源准入语义与验证范围 |
| [docs/V0.3-DOCKER-EXECUTOR.md](./docs/V0.3-DOCKER-EXECUTOR.md) | V0.3 Docker Executor 策略、进程内接入、真实容器测试与限制 |
| [docs/V0.3-RECOVERY.md](./docs/V0.3-RECOVERY.md) | V0.3 残留回收、故障隔离与启动顺序 |
| [docs/V0.3-ACCEPTANCE.md](./docs/V0.3-ACCEPTANCE.md) | V0.3 阶段验收证据与边界 |
| [docs/V1.0-SQLITE-FOUNDATION.md](./docs/V1.0-SQLITE-FOUNDATION.md) | SQLite 驱动选择、Schema v1、事务边界与测试 |
| [docs/V1.0-IDEMPOTENCY.md](./docs/V1.0-IDEMPOTENCY.md) | Schema v2、事务化幂等准入及验证范围 |
| [docs/V1.0-DURABLE-SINGLE.md](./docs/V1.0-DURABLE-SINGLE.md) | 单任务持久化 Manager 的启动、准入、认领与重启语义 |
| [docs/V1.0-DOCKER-OWNERSHIP.md](./docs/V1.0-DOCKER-OWNERSHIP.md) | Docker 容器 ID 落盘顺序、标签兜底与恢复测试 |
| [docs/V1.0-WORKFLOW-STORE.md](./docs/V1.0-WORKFLOW-STORE.md) | Schema v3、Workflow 整批事务准入与重启读取 |
| [docs/V1.0-DURABLE-WORKFLOW.md](./docs/V1.0-DURABLE-WORKFLOW.md) | 持久化 DAG 的调度、取消与重启恢复语义 |
| [docs/V1.0-WORKFLOW-IDEMPOTENCY.md](./docs/V1.0-WORKFLOW-IDEMPOTENCY.md) | Schema v4、Workflow 幂等键的作用域、规范化与事务准入 |

**协作 AI 开始工作前须读取上述两份文档与本 README。** 未指定阶段时只开发 V0.1；不要提前实现整条路线。

README 会随实际代码更新运行命令、已实现能力和验证结果。目前不提供性能数字或上线声明。
