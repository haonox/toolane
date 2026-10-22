# P2 决策：移除 Docker 后端

Toolane 的当前目标是展示单机、多租户 Agent 工具调用的有界调度、可查询状态和可复现的真实工具路径。P0/P1 已通过管理员静态注册的可信 Process 工具覆盖这条路径，因此 Docker 不再是完成核心场景的前提。

Docker 原先提供一条运行受限 Shell 脚本的可选路径。它的独立价值是不可信工具与宿主进程之间的容器边界；这个价值没有被 Process 后端替代。代价是服务必须依赖 Docker daemon、固定镜像和宿主配置，并长期维护容器创建、策略校验、资源预算、崩溃扫描与清理、实例绑定，以及有 daemon 权限的集成测试。当前仓库的真实 HTTP 工具覆盖集中在 Process；在缺少 daemon 的开发环境里，Docker 的真实容器测试不能作为日常门槛运行。对这个阶段的项目，保留该后端会扩大维护和说明成本，而无法加强核心调度证据。因此 P2 移除 Docker 后端、`sandbox.shell`、专用预算和容器指标。

Toolane 现在只运行管理员信任的本地 Process 工具，另有 Mock 工具供开发测试使用。它**不提供不可信代码隔离**。不能将旧 `sandbox.shell` 请求直接映射为可由 Agent 任意指定命令的 Process 工具；如仍需执行不可信代码，应在 Toolane 之外选用并维护独立的隔离服务。

## 从旧 Docker 配置迁移

1. 停止向旧服务提交新任务，等待或取消已接受任务。若旧服务曾被强制终止，先用**旧版本**及原 Docker 配置、状态目录启动，让其执行原有的容器恢复检查。将仍为 `interrupted` 的任务视为结果未知，核对外部副作用。
2. 用原配置的 `instance_id` 检查 `docker container ls -a --filter "label=toolane.instance=原实例ID"`。对仍存在的容器逐个核实所有权、运行状态和副作用，再由管理员处理。新版不会接管或清理这些容器。
3. 备份旧状态目录、私有配置和需要保留的任务记录。创建**新的**私有 `state_dir`；从配置中删除 `docker`，从 Token 白名单中删除 `sandbox.shell`，按可信工具的规则显式注册所需 `process_tools`。新的状态目录不继承旧任务 ID、事件、工作流或幂等键。
4. 启动新版服务并按 README 的真实工具示例验证。保留旧状态备份，直到外部副作用和旧记录完成核对。

新版配置解析会拒绝 `docker` 字段。服务启动时若发现旧状态目录的 `.docker-instance-id`，会在 HTTP 接收任务前拒绝启动；持久化记录中若含旧容器资源引用，也会在恢复前拒绝。这些检查防止“删掉配置后继续使用原目录”绕过尚未完成的容器处置。纯 Process 状态目录没有上述旧资源时，可沿用原有的 SQLite 恢复语义。

监控配置也需同步调整：`toolane_residual_containers`、`toolane_residual_blocked`、`toolane_reserved_cpu_millicores` 和 `toolane_reserved_memory_bytes` 已移除。任务的 `cleanup_status` 仍保留，用于表达无法确认进程停止的故障状态。
