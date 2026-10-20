# Toolane

**面向 Agent 工具调用的单机调度与受限执行服务。** Toolane 接收任务，管理队列、租户配额与 DAG 依赖，通过统一的 Executor 运行工具，并提供持久化的 HTTP 接口。它负责可靠执行；调用方负责决定使用什么工具。

Linux · Go 1.27.1 · SQLite · HTTP + SSE · 可选 Docker 沙箱 · [Apache-2.0](LICENSE)

## 特性

- **有界调度**：固定 Worker、全局和租户容量限制、租户轮询调度、排队超时与取消。
- **工作流**：原子提交静态 DAG；失败节点的下游跳过，独立分支继续执行。
- **三种执行后端**：Mock 用于开发和测试；Process 仅运行服务端注册的可信程序；Docker 使用固定镜像运行受限 Shell 脚本。
- **持久化与恢复**：SQLite 保存任务、事件和幂等键；重启后续调未认领任务，已认领任务中断并协调其资源。
- **本地服务接口**：Token 映射租户与工具权限，提供提交、查询、取消、SSE 事件、健康检查和可选管理指标。

```mermaid
flowchart LR
    Client[Agent / Client]:::box --> API[HTTP API]:::box
    API --> Manager[Scheduler & Manager]:::box
    Manager <--> Store[(SQLite)]:::box
    Manager --> Executor{Executor}:::box
    Executor --> Mock[Mock]:::box
    Executor --> Process[Trusted Process]:::box
    Executor --> Docker[Docker Sandbox]:::box

    classDef box fill:#f8f9fa,stroke:#333,stroke-width:1.5px,rounded:false
```

## 快速开始

需要 **Linux 和 Go 1.27.1**。克隆仓库后，可以先运行不依赖 Docker 的演示：

```bash
git clone https://github.com/haonox/toolane.git
cd toolane
go run ./cmd/toolane          # Mock 任务
go run ./cmd/toolane process  # 可信本地进程任务
```

### 启动 HTTP 服务

创建一个仅自己可读的配置文件，例如 `/absolute/private/server.json`。将 Token 替换为私密随机值（可用 `openssl rand -hex 32` 生成），并将状态目录改为本机绝对路径：

```json
{
  "listen": "127.0.0.1:8080",
  "state_dir": "/absolute/private/toolane-state",
  "tokens": [
    {
      "token": "replace-with-a-private-random-token",
      "tenant_id": "local",
      "tools": ["mock.delay"]
    }
  ]
}
```

```bash
chmod 600 /absolute/private/server.json
go run ./cmd/toolane serve --config /absolute/private/server.json
```

服务只接受回环地址；状态目录会以私有权限创建，已有目录也必须仅所有者可访问。`/readyz` 在存储及资源恢复完成后才返回就绪。可在配置中加入 `"metrics_listen": "127.0.0.1:9090"`，从独立的本地端口读取 `GET /metrics`。

提交一个 Mock 任务，将下方 Token 替换为配置中的值：

```bash
curl -sS http://127.0.0.1:8080/api/v1/executions \
  -H 'Authorization: Bearer replace-with-a-private-random-token' \
  -H 'Content-Type: application/json' \
  -d '{"tool":"mock.delay","arguments":{"delay_ms":100,"value":"hello"}}'
```

响应中的 `execution_id` 可用于查询状态或订阅事件。仓库也提供只依赖 Python 标准库的[客户端示例](examples/python_client.py)：

```bash
TOOLANE_TOKEN='replace-with-a-private-random-token' \
  python3 examples/python_client.py run --delay-ms 100 --value hello
```

## HTTP API

业务接口使用 `Authorization: Bearer <token>`；租户身份和可调用工具由服务配置决定。

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `POST` | `/api/v1/executions` | 提交任务，支持 `Idempotency-Key` |
| `GET` | `/api/v1/executions/{id}` | 查询任务 |
| `POST` | `/api/v1/executions/{id}/cancel` | 取消任务 |
| `GET` | `/api/v1/executions/{id}/events` | 订阅或回放 SSE 事件 |
| `POST` | `/api/v1/workflows` | 提交 DAG，支持 `Idempotency-Key` |
| `GET` | `/api/v1/workflows/{id}` | 查询工作流 |
| `POST` | `/api/v1/workflows/{id}/cancel` | 取消工作流 |
| `GET` | `/healthz`、`/readyz` | 存活与就绪检查 |

跨租户记录不会通过查询、取消或事件接口暴露。服务向标准错误输出结构化 JSON 日志；管理指标使用独立的回环地址端口。

## Docker 执行

使用 Docker 后端时，服务管理员须先在本机准备镜像，并在配置中固定**镜像标签、SHA-256 digest 和稳定的实例 ID**。以下是可独立使用的 Docker 服务配置，测试镜像为 Alpine 3.24.2：

```json
{
  "listen": "127.0.0.1:8080",
  "state_dir": "/absolute/private/toolane-state",
  "docker": {
    "image_tag": "alpine:3.24.2",
    "image_digest": "sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6",
    "instance_id": "local"
  },
  "tokens": [
    {
      "token": "replace-with-a-private-random-token",
      "tenant_id": "local",
      "tools": ["sandbox.shell"]
    }
  ]
}
```

与前面的 HTTP 配置相同，先生成私密 Token，将配置写入仅自己可读的文件，再用 `serve --config` 启动。服务启动时先回收本实例的残留容器并检查 Docker 隔离条件。容器默认无网络，以非 root 身份运行，使用只读根目录、有界工作区及 CPU/内存限制。请求不能选择镜像、挂载、网络或特权参数。运行服务的用户需要有访问 Docker daemon 的权限。

## 开发与验证

```bash
go test ./...
go test -race ./...
go vet ./...
```

真实 Docker 集成测试需要本地 daemon 权限和已拉取的固定镜像：

```bash
go test -tags=integration ./cmd/toolane ./tests/integration/... -count=1
```

如果 Docker 仅允许通过 `sudo` 访问，请先以普通用户运行 `go mod download`，再复用该用户的模块缓存：

```bash
sudo env PATH="$PATH" GOCACHE=/tmp/toolane-go-cache \
  GOMODCACHE="$(go env GOMODCACHE)" GOPROXY=off \
  go test -tags=integration ./cmd/toolane ./tests/integration/... -count=1
```

`benchmarks/` 包含调度基准与本地 HTTP 负载脚本；对应原始结果保存在 [`benchmarks/results/`](benchmarks/results/) 中。它们分别测量 Mock 路径，不能当作真实容器吞吐数据。

| 路径 | 内容 |
| --- | --- |
| `cmd/toolane/` | CLI 演示与本地 HTTP 服务 |
| `internal/execution/` | 状态机、调度、工作流和恢复 |
| `internal/executor/` | Mock、可信 Process 与 Docker 后端 |
| `internal/httpapi/`、`internal/store/sqlite/` | HTTP/SSE 接口与持久化 |
| `examples/`、`benchmarks/`、`tests/integration/` | 调用示例、实验与真实容器测试 |

欢迎通过 Issue 描述问题或提议改进；提交代码时请附上与改动相关的测试结果。

## 边界与许可证

Toolane 是单机项目，不提供 LLM 推理、Agent 规划、多实例共享数据库或自动重试。Process 后端只适合可信程序；Docker 后端降低常见越权与资源滥用风险，不承诺抵御宿主内核漏洞。服务默认只监听本机；对外暴露时须自行配置受信任的 TLS 入口。

本项目采用 [Apache License 2.0](LICENSE)。
