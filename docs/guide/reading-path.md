# 学习路线

这套文档采用“先完成一次调用，再逐步加入复杂性”的顺序。不要一开始从 Docker 或 SQLite 向回猜整个系统；先建立内存执行模型，后续模块就只是给它增加可靠性边界。

## 第一阶段：最小闭环

按以下顺序阅读：

1. [`main.go` 与 Demo](../execution/main-demo.md)：看到一个任务如何被注册、提交和查询。
2. [工具契约与策略](../execution/tool-contract.md)：划清 Manager 与 Executor 的职责。
3. [`Manager.Submit`](../execution/submit.md)：理解接纳、容量和所有权转换。
4. [Worker 生命周期](../execution/worker.md)：完成 `queued → running → terminal`。

完成这一阶段后，你应该能独立解释：为什么 `Submit` 返回 `queued`，而真正的执行发生在另一个 goroutine 中。

## 第二阶段：并发与依赖

5. [多租户调度器](../execution/scheduler.md)：理解“最早提交”为什么不总是“最先执行”。
6. [DAG 工作流](../workflow/dag.md)：理解 `blocked` 节点怎样复用普通任务调度器。

完成这一阶段后，你应该能根据一组租户、资源和依赖条件，推导任务的领取顺序与最终状态。

## 第三阶段：真实执行与可靠性

7. [Process Executor](../executors/process.md)：把抽象 Executor 映射到 Linux 进程树。
8. Durable Manager 与 SQLite：状态事务、幂等和恢复。
9. HTTP 与 SSE：把内部模型暴露成租户隔离的服务接口。
10. Docker Executor：资源所有权、隔离策略和崩溃清理。

后三部分会随着源码学习继续补充，进度见[后续章节](../roadmap.md)。

## 推荐的阅读动作

每读完一章，可以做三件事：

- 在编辑器中打开章节列出的源码函数；
- 运行对应包的测试，并找到章节引用的行为测试；
- 改变一个输入条件，在纸上先推导状态变化，再用测试验证。

!!! tip "不要只盯着函数调用"

    这个项目真正重要的是状态所有权与不变量。例如 `Manager` 持有最终状态裁决权、Executor 返回前必须停止底层工作、资源检查与预留必须在同一把锁下完成。这些约束比单个函数的语法更值得记忆。
