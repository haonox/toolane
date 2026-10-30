# Toolane

Toolane 是面向 Agent 工具调用的单机、多租户调度与运行管理服务。多个租户共用 Agent 服务时，大量工具调用可能同时涌入；Toolane 控制接收量和并发量，按租户公平调度，并跟踪每次调用直到产生最终状态和结果。工具的具体业务逻辑由服务端注册的工具程序实现。

Linux · Go 1.27.1 · HTTP · [Apache-2.0](LICENSE)

## 解决的问题与调用流程

### Toolane 在 Function Calling 流程中的位置

Agent 程序把工具定义和用户问题交给大模型。模型返回工具名、参数和模型侧的 `call_id`；Agent 程序再把工具调用提交给 Toolane。Toolane 负责接收、调度、执行已注册工具并提供最终状态和结果。Agent 程序取得结果后，使用 `call_id` 将工具结果交还模型，由模型继续回答或提出下一轮工具调用。

```mermaid
sequenceDiagram
    participant U as 用户
    participant A as Agent 程序
    participant M as 大模型
    participant T as Toolane
    participant P as 工具进程

    U->>A: 提出问题
    A->>M: 发送问题和可用工具定义
    M-->>A: 工具名、参数、call_id
    A->>T: 提交工具名和参数
    T-->>A: 返回 execution_id
    T->>P: 调度并执行工具
    P-->>T: 返回执行结果
    A->>T: 用 execution_id 查询最终状态和结果
    T-->>A: 返回终态及结果或错误
    A->>M: 用 call_id 回传工具结果
    M-->>A: 生成回答，或继续提出工具调用
    A-->>U: 返回回答
```

`call_id` 用于 Agent 程序向模型回传本次工具调用的结果；`execution_id` 用于向 Toolane 查询任务。Agent 程序负责保存两者的对应关系，并在任务尚未结束时继续查询。Toolane 不直接与大模型通信。

### Toolane 内部的执行流程

Agent 程序收到模型提出的工具调用后，向 Toolane 提交工具名和 JSON 参数。Toolane 校验租户权限、工具及参数，在全局队列和租户待处理容量内接收请求；容量耗尽时拒绝请求。已接收的任务进入有界队列，由固定数量的 Worker 按租户轮询并遵守每租户运行上限。Toolane 管理排队超时、执行超时、取消和输出限制，保存工具程序返回的结果或错误。

```mermaid
flowchart TD
    A["Agent 程序提交工具调用<br/>POST /api/v1/executions"] --> B{"Token、工具权限与参数校验"}
    B -- 未通过 --> R1["返回请求错误"]
    B -- 通过 --> C{"全局与租户待处理容量"}
    C -- 已满 --> R2["返回 429"]
    C -- 可接收 --> D["持久化 queued 任务<br/>返回 202 和 execution_id"]
    D --> E["有界队列按租户轮询<br/>遵守每租户运行上限"]
    E --> F{"排队期间超时或取消？"}
    F -- 是 --> I["记录最终状态、结果或错误"]
    F -- 否 --> G["Worker 领取任务：running"]
    G --> H["调用已注册的可信工具程序<br/>管理超时、取消和输出限制"]
    H --> I
    D -. "Agent 持 execution_id 轮询 GET /api/v1/executions/{id}" .-> J{"任务是否结束？"}
    I --> J
    J -- 否，稍后继续查询 --> J
    J -- 是 --> K["返回 succeeded、failed、timed_out、cancelled 或 interrupted<br/>以及结果或错误"]
    K --> L["Agent 程序关联模型 call_id<br/>将结果交还模型"]
```

提交成功只表示任务已被接收，结果通过后续查询取得，Toolane 当前不会主动推送结果。调度和配额按租户生效，`agent_id` 只是可选的任务标识，不参与调度。

HTTP 服务会在返回 `202` 前将任务提交到 SQLite。重启后，未过排队截止时间且工具仍已配置的任务继续排队；终态及结果在保留期内仍可用原 `execution_id` 查询。重启前处于 `running` 或 `cancelling` 的任务转为 `interrupted`，其外部副作用可能已经发生，服务不会自动重试。排队超时或工具已移除的任务在恢复时形成相应终态。恢复的排队任务使用重启后的工具定义；修改工具行为时应考虑与旧任务的兼容性。异常杀死服务进程时，进程工具的子进程清理还取决于部署所用的进程管理器。客户端可用 `Idempotency-Key` 处理 `202` 响应丢失后的重试。

## 核心能力

- 固定 Worker 数、有界队列、租户轮询调度与每租户运行上限。
- 排队超时、执行超时、取消、输出与结果大小限制。
- SQLite 持久化已接收任务、状态和结果；内存维护租户调度索引。
- Mock 工具用于学习和测试；Process 工具只运行服务端注册的可信程序。
- Bearer Token 绑定租户和工具权限；业务接口可监听指定私网 IP，指标接口只监听本机回环地址。

调度器在内存中为每个活跃租户维护 FIFO 队列，只把仍有可运行任务的租户放入轮询队列；排队截止时间放在最小堆中。取任务和处理超时无需扫描全部租户、任务或已完成记录。提交、取消和取任务的索引更新为 `O(log N)`（`N` 为排队任务数），租户资格切换为常数时间；SQLite 仍是任务状态的持久化来源。

[独立 HTTP 压测目录与加压方案](benchmarks/README.md)

## 快速开始

```bash
go run ./cmd/toolane demo mock
go run ./cmd/toolane demo process
```

启动 HTTP 服务时，先创建仅自己可读的配置文件。以下 Token 请换成私密随机值：

```json
{
  "business_listen": "127.0.0.1:8080",
  "execution": {
    "workers": 8,
    "queue_capacity": 1024,
    "tenant_running_limit": 2,
    "tenant_pending_limit": 128,
    "retained_results": 5000,
    "result_retention_seconds": 86400
  },
  "tokens": [
    {"token": "replace-with-a-private-random-token", "tenant_id": "local", "tools": ["mock.delay"]}
  ]
}
```

`execution` 是可选的服务容量配置，示例数值仅用于展示格式。管理员可按机器资源和工具耗时调整：

| 字段 | 作用 | 未填写时的默认值 |
| --- | --- | ---: |
| `workers` | 同时执行工具的 Worker 数 | 4 |
| `queue_capacity` | 全局待处理任务数上限，不含运行中的任务 | 256 |
| `tenant_running_limit` | 每租户同时运行的任务数上限 | 2 |
| `tenant_pending_limit` | 每租户待处理任务数上限 | 64 |
| `retained_results` | 内存中保留的终态任务条数上限 | 1000 |
| `result_retention_seconds` | 终态结果和对应幂等键的保留秒数 | 86400（24 小时） |

可以只填写需要覆盖的字段；所有配置值都必须是正整数，显式配置为零或负数会使服务启动失败。`retained_results` 只限制内存中的终态缓存。终态记录从完成时开始保留 `result_retention_seconds`（可配置为 1 秒至 365 天）；到期后查询返回 `404`，后台按批删除 SQLite 记录及幂等键。排队和运行中的记录不会按此保留期清理。SQLite 可能复用已释放的页而暂不缩小文件，请继续监控磁盘容量。

可选的 `database_path` 必须是绝对路径。未填写时，服务使用配置文件同目录下的 `toolane-state/executions.sqlite`。服务会创建数据库目录并要求它仅对当前用户开放（权限 `0700`）；数据库文件权限为 `0600`。请把该目录放在重启后仍保留的磁盘上；备份前先正常停止服务，再保存该目录。同一数据库一次只允许一个服务实例使用。

可选的 `database_synchronous` 控制 SQLite WAL 的同步策略，只接受 `"full"` 或 `"normal"`，默认 `"full"`。`full` 在每次提交后同步 WAL，优先保证操作系统崩溃或断电后的已确认任务不丢失。若业务只要求服务进程崩溃、重启后恢复，可显式设置 `"database_synchronous": "normal"` 来减少同步写入；此模式下最近已确认的提交可能在操作系统崩溃或断电后回滚，但数据库仍保持一致。该取舍见 [SQLite 官方文档](https://www.sqlite.org/pragma.html#pragma_synchronous)。

```bash
chmod 600 /absolute/private/server.json
go run ./cmd/toolane serve --config /absolute/private/server.json
```

提交并查询一次任务：

```bash
curl -sS http://127.0.0.1:8080/api/v1/executions \
  -H 'Authorization: Bearer replace-with-a-private-random-token' \
  -H 'Idempotency-Key: model-call-123' \
  -H 'Content-Type: application/json' \
  -d '{"tool":"mock.delay","arguments":{"delay_ms":100,"value":"hello"}}'
```

响应包含 `execution_id`，可用 `GET /api/v1/executions/{id}` 查询。

同一租户在保留期内用同一 `Idempotency-Key` 重发相同请求时，`POST` 仍返回 `202`，响应包含原 `execution_id` 和当前状态，不会创建第二次执行。请求内容（包括 `agent_id`、工具、参数和生效后的超时及输出限制）不同则返回 `409 idempotency_conflict`。键只接受 1–128 个 ASCII 字母、数字、`-`、`_`、`.` 或 `~`；不发送该请求头时，每次提交都是新任务。幂等键随终态结果到期清理，之后同一键可以用于新任务。

管理员在 `process_tools` 中固定可执行文件、工作目录、环境变量、JSON Schema 和参数位置。参数位置只能使用固定值、必填字段或执行 ID；Toolane 直接构造参数数组，不经过 Shell。Process 工具成功退出时，标准输出必须是一个 JSON 值，它会成为任务结果。只应注册可信程序。

## HTTP API

业务接口使用 `Authorization: Bearer <token>`。租户身份和可调用工具由服务配置决定。

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `POST` | `/api/v1/executions` | 提交任务 |
| `GET` | `/api/v1/tools` | 列出当前 Token 可见的工具 |
| `GET` | `/api/v1/executions/{id}` | 查询任务状态与结果 |
| `POST` | `/api/v1/executions/{id}/cancel` | 取消任务 |
| `GET` | `/healthz`、`/readyz` | 存活与就绪检查 |

服务始终启动业务和指标两个端口。`business_listen` 默认是 `127.0.0.1:8080`，也可指定服务器实际拥有的私网 IPv4/IPv6 地址，例如 `10.0.0.5:8080` 或 `[fd00::5]:8080`，供同一私网的客户端访问；不接受 `0.0.0.0`、`[::]`、公网 IP 或任意域名。云服务器上的私网监听还应在安全组中仅向可信客户端开放该端口。`metrics_listen` 默认是 `127.0.0.1:9090`，仍只允许本机回环地址。指标端口提供 `GET /metrics`，服务向标准错误输出结构化 JSON 日志；成功的执行状态轮询记为 Debug，提交、失败查询和状态转换保留 Info，避免高频轮询放大日志写入。任一端口无法监听时，服务启动失败。

## 代码与验证

当前自动化测试、CI、调度器微基准和 HTTP 压测的运行方式见 [测试说明](TESTING.md)。

| 路径 | 职责 |
| --- | --- |
| `cmd/toolane/` | CLI 演示和 HTTP 服务启动 |
| `internal/tool/` | 工具注册、定义与参数校验 |
| `internal/execution/` | 任务状态、准入、队列、调度、Worker |
| `internal/storage/sqlite/` | 执行记录持久化与 schema 迁移 |
| `internal/executor/` | Mock 与可信 Process 执行 |
| `internal/httpapi/` | Token 授权与 HTTP 接口 |
| `internal/observability/` | 日志和指标 |

```bash
go test ./...
go test -race ./...
go vet ./...
```

Toolane 不提供 LLM 推理、Agent 规划或不可信代码隔离。服务默认只监听本机；对外访问需自行配置受信任的 TLS 入口。
