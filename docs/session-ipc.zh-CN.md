# 本机 IPC：固定聊天与实际接收会话

本机私有 Unix API 支持将新消息投到同一聊天的当前执行会话。`/new` 更换执行上下文，聊天地址保留。原先固定执行会话的输入仍按原规则处理。

## 地址与协议

协议名保持 `cc-connect-session-ipc-v1`。`POST /ipc/event` 可增加 `routing: "chat"`；省略时继续要求原 CC 会话及模型线程完全匹配。

固定地址由 `project`、完整 `session_key`、`cwd` 组成，调用方另固定私有 socket。不要从 `session_key` 中去掉用户或话题部分：飞书的聊天、群内用户和话题隔离沿用现有平台配置。

| 接口或字段 | 用途 |
| --- | --- |
| `GET /ipc/health` 的 `capabilities` | 包含 `chat_routing` 表示接收器支持新路由 |
| `POST /ipc/target` | 严格核对当前实际身份；仍用于来源确认 |
| `POST /ipc/chat-target` | 只读解析固定聊天当前的 CC 会话和模型线程，不创建会话 |
| `input.target` | 不可变的来源登记证据，参与原事件哈希 |
| `delivery_target` | 本次投递前持久保存的实际接收者，包含 CC 会话与模型线程 |
| `POST /ipc/ack` 的 `target` | 必须匹配实际接收者；新会话不能替已收信的前任确认 |

`/new` 后的 CC 会话可能还没有模型线程。空闲投递会按正常流程启动执行器，再核对当前 CC 会话、记录实际线程并发送。仅支持在输入前提供确定线程身份的执行器；暂未就绪时等待，不猜测线程。

## 并发与重试

宿主在接触执行器前检查 CC 会话，避免过期投递关闭新会话的执行器；发送前再次检查并保存实际接收者。保存后本次尝试固定到捕获的执行器，随后的 `/new` 不改投这次尝试。

同一事件的原始 input 和哈希不随聊天切换而改变。已送达、发送结果不明或接收端在发送中重启，均返回原事件回执，不重投。只有明确 `waiting` 且能够证明没有发送的回执可以按同一 ID 再试；启动前会话变更的原因码为 `chat_target_changed_before_send`，此时 `method` 为空。

上下文异常自动恢复仍使用独立的 `context_recovery` 记录，不改原投递回执，也不把新线程冒充原线程的收件确认。

## 验证

关键回归覆盖 `/new` 后线程尚为空、忙时追加、启动期间换会话、旧投递不能关闭新执行器、跨 `/new` 去重、未知结果不重试、实际接收者确认，以及项目、聊天、用户／话题和目录隔离。`TestCUJ_A11_ChatMailboxSurvivesNew` 从普通聊天、`/new`、IPC 后续资料到 `/history` 检查用户可见结果。

```sh
go test ./...
go test ./core/ -run TestCUJ
go test -race ./core -run 'TestIPC_|TestContextRecovery_IPC'
```
