# 回传等待与安全恢复

`ipc.py` 负责把已经结束的 Hermes 作业送回原会话，不重新执行作业。投递固定原事件 ID、输入原字节与五项目标；接收端继续核对目标、进程实例、启动身份和不同投递尝试。

修复覆盖两条故障路径：

- 首次 POST 返回 `dispatching`，随后 GET 得到 `waiting/released` 时，原客户端仍永久只查询，已完成报告无法接续。
- POST 响应丢失时，原客户端直接进入终态 `unknown` 并退出，未等待接收端给出最终回执。

现在响应丢失只令 watcher 转为 GET；找不到回执、查询失败、`dispatching` 均不允许再次 POST。接收端明确 `unknown`、停止或阻断仍结束自动处理。

只有回执与原 payload 完全一致，并具备以下证据，才解除只查询状态：

1. `status=waiting`，有效的 `boot` 与 `attempt_id`。
2. 没有 `claimed_at`、`injected_at` 或 `acknowledged_at`，也不是 `session_stopped`。
3. Queue 路径必须有 `phase=released` 和有效 `instance_id`；同步 steer 路径必须明确 `turn_not_ready` 或 `session_busy_without_live_input`，且没有 queue 元数据。

解除后下一步重新投递原事件；接收端生成新的 attempt，并拒绝旧 attempt 的迟到回调。若目标已切换、实例已替换或服务已重启，接收端仍可阻断，客户端不能改投。`delivered` 或已知已送达但 ack 失败的记录保留正向证据，后续低状态不能重新授权投递。

`reconcile` 始终只 GET 并保存证据；`recover` 恢复已有事件的 watcher。不得删除回执、换事件 ID、修改固定目标或重跑 Hermes 来绕过不明投递。恢复前检查 callback 锁及当前进程身份，历史 PID 本身不足以证明进程归属。

验证包含：真实 Unix socket 上的丢失 HTTP 响应 → 查询释放 → 同事件再次投递 → ack；旧 `query_only` 恢复；有 claim/ack 或证据缺失时禁止重投；receiver unknown 终态；ack 失败保留已送达证据。接收端的 `TestIPCQueue_*` 另覆盖旧 attempt、停止、实例替换及 claim 竞态。
