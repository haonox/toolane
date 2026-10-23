# Toolane

Toolane 是面向 Agent 工具调用的单机调度与执行服务。调用方选择已注册工具并提交参数；Toolane 校验请求、控制队列与租户配额、调用工具，并提供任务状态查询和取消接口。

Linux · Go 1.27.1 · HTTP · [Apache-2.0](LICENSE)

## 核心能力

- 固定 Worker 数、有界队列、租户轮询调度与每租户运行上限。
- 排队超时、执行超时、取消、输出与结果大小限制。
- Mock 工具用于学习和测试；Process 工具只运行服务端注册的可信程序。
- Bearer Token 绑定租户和工具权限；服务监听本机回环地址，可另开端口提供管理指标。

[架构与执行语义](docs/architecture.md)

## 快速开始

```bash
go run ./cmd/toolane demo mock
go run ./cmd/toolane demo process
```

启动 HTTP 服务时，先创建仅自己可读的配置文件。以下 Token 请换成私密随机值：

```json
{
  "listen": "127.0.0.1:8080",
  "tokens": [
    {"token": "replace-with-a-private-random-token", "tenant_id": "local", "tools": ["mock.delay"]}
  ]
}
```

```bash
chmod 600 /absolute/private/server.json
go run ./cmd/toolane serve --config /absolute/private/server.json
```

提交并查询一次任务：

```bash
curl -sS http://127.0.0.1:8080/api/v1/executions \
  -H 'Authorization: Bearer replace-with-a-private-random-token' \
  -H 'Content-Type: application/json' \
  -d '{"tool":"mock.delay","arguments":{"delay_ms":100,"value":"hello"}}'
```

响应包含 `execution_id`，可用 `GET /api/v1/executions/{id}` 查询。也可以运行只依赖 Python 标准库的[客户端示例](examples/python_client.py)。

任务记录只在本次服务运行期间可查询。重新启动服务后，原执行 ID 不再可查询；重发请求会创建新任务。

### 真实 Process 工具

仓库提供 [`text.stats`](examples/tools/text_stats.py) 示例。配置生成器会注册工具并打印两个租户的 Token：

```bash
demo_dir="$(mktemp -d)"
python3 examples/make_process_config.py --config "$demo_dir/server.json"
go run ./cmd/toolane serve --config "$demo_dir/server.json"
```

在另一个终端：

```bash
export TOOLANE_TOKEN='上一步打印的租户 Token'
python3 examples/agent_bridge.py describe
python3 examples/agent_bridge.py invoke --call-id demo-call-1 \
  --tool text.stats --arguments '{"text":"hello Toolane"}'
```

[`agent_bridge.py`](examples/agent_bridge.py) 演示工具描述、提交、轮询以及模型侧 `call_id` 与执行 ID 的映射。

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

配置中可加入 `"metrics_listen": "127.0.0.1:9090"`，从独立本地端口读取 `GET /metrics`。服务向标准错误输出结构化 JSON 日志。

## 代码与验证

| 路径 | 职责 |
| --- | --- |
| `cmd/toolane/` | CLI 演示和 HTTP 服务启动 |
| `internal/tool/` | 工具注册、定义与参数校验 |
| `internal/execution/` | 任务状态、准入、队列、调度、Worker |
| `internal/executor/` | Mock 与可信 Process 执行 |
| `internal/httpapi/` | Token 授权与 HTTP 接口 |
| `internal/observability/` | 日志和指标 |
| `examples/` | 本地调用示例 |

```bash
go test ./...
go test -race ./...
go vet ./...
python3 -m unittest discover -s examples -p 'test_*.py'
```

Toolane 不提供 LLM 推理、Agent 规划或不可信代码隔离。服务默认只监听本机；对外访问需自行配置受信任的 TLS 入口。
