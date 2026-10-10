# 原会话 IPC 接续

协议：`cc-connect-session-ipc-v1`。这是 `ipc-bridge` 分支的扩展，供本机任务程序把结果通知送回预先绑定的 Agent 会话。

Codex 会话空闲时，CC Connect 恢复原 thread 并执行 `turn/start`；正在处理消息时，通过 `turn/steer` 向固定 thread 的当前 turn 追加输入。对于在后续调用边界接收输入的适配器，Core 另外提供可选 `LiveInputQueuer` 与异步进度回执；具体后端须自行实现和验证该能力。程序传递与回执检查不调用模型，Agent 接续处理会产生模型用量。

## 启用条件

- 运行包含此扩展的 CC Connect 二进制，使用单工作区项目；当前接收器拒绝 multi-workspace 项目。
- Codex 使用 `app_server` 后端和 stdio 通道；其他后端须提供已验证的实时或调用边界输入能力。目标平台实现 `ReplyContextReconstructor`，用于空闲唤起后的回复路由。
- 原会话已经存在，并有可恢复的真实后端会话 ID。目标由派发时的真实会话身份固定，不能在完成时选择“最新会话”。

在已有项目的 Agent 配置中设置以下字段，保留其余平台配置和用户选择的权限模式：

```toml
[projects.agent]
type = "codex"

[projects.agent.options]
backend = "app_server"
app_server_url = "stdio"
```

此适配器通过子进程 stdin/stdout 收发 JSON-RPC，启动 stdio 时省略 `--listen`；不支持将 `app_server_url` 配成 WebSocket 或其他 socket listener。配置的 `cmd` 与附加参数继续生效，参见[Codex CLI 查找](usage.zh-CN.md#macos-上的-codex-cli-查找)。

IPC 不变更权限模式。App Server 后端中，`suggest` 对应按需审批与只读沙盒，`auto-edit`／`full-auto` 对应 workspace-write，`yolo` 对应 danger-full-access；使用已经获得授权的配置。

## 传输与接口

接口只注册在 `<data_dir>/run/api.sock` 的私有 Unix API 上，socket 权限为 `0600`。默认数据目录下即 `~/.cc-connect/run/api.sock`。客户端先核对 socket 的当前用户所有权和权限；不需要开启 Web 管理后台、Bridge TCP 端口或浏览器。

| 方法 | 路径 | 请求与用途 |
|---|---|---|
| GET | `/ipc/health` | 返回协议名和 `transport=unix`，只证明接收器存在 |
| POST | `/ipc/target` | 请求体为目标对象；核对已有绑定，返回 `target`、`busy`、`supports_live_input` |
| POST | `/ipc/origin` | 用启动前注入的实例句柄解析真实来源，返回与 `/ipc/target` 相同的目标及能力字段 |
| POST | `/ipc/event` | 提交 `event_id`、`target`、`prompt`，返回持久回执 |
| GET | `/ipc/event?event_id=<id>` | 查询同一事件的回执，不再次发送输入 |
| POST | `/ipc/ack` | 请求体为 `event_id`、`target`，记录原会话收件确认 |

`target` 包含五个字段：

| 字段 | 含义 |
|---|---|
| `project` | 已配置的 CC Connect 项目 |
| `session_key` | 原平台会话键 |
| `session_id` | CC Connect 内部会话 ID |
| `agent_session_id` | 真实后端会话 ID，例如 Codex thread ID |
| `cwd` | 目标 Agent 工作目录的真实绝对路径，已解析软链接 |

初次核对 `/ipc/target` 时可将 `session_id` 留空，由接收器返回当前匹配值。其余四项须来自真实原会话；取得完整目标后持久保存。后续事件与 ack 都使用同一目标，绑定变化时停止投递。

### 启动后才产生会话 ID 的来源

Core 在每次 `StartSession` 前注入 `CC_IPC_ORIGIN`，值为 `cc-origin-v1-` 加 64 位小写十六进制随机字符。客户端向 `/ipc/origin` 提交 `origin_id`、`project`、`session_key`、`cwd`；后三项分别来自原 CC 环境与已解析软链接的工作目录。客户端不提供要绑定的 `agent_session_id`。

接收器核对句柄所属的活动 CC 会话与当前 AgentSession 实例，再读取后端报告的真实 ID。首次 init 已到但 Core 尚未消费对应事件时，只能 CAS 填充空绑定；已有不同 ID 不会被覆盖。成功响应仅包含协议、固定目标、busy 与能力，不返回原句柄。句柄不写入会话存储或 IPC 回执；它是同一操作系统用户内的实例绑定依据，不提供进程权限隔离。

缺项返回 400 `ipc_origin_incomplete`，前缀或格式不符返回 400 `ipc_origin_invalid`；实例停止、关闭、替换或会话切换后返回 409 `ipc_origin_changed`。尚未报告真实 ID 返回 409 `ipc_origin_not_ready`，可短暂再次查询这一只读来源入口。项目和目录错误沿用 `ipc_project_unsupported`、`ipc_cwd_unavailable`、`ipc_cwd_mismatch`。失败时不得猜测 ID、改投最新会话或把其他后端伪装成 `CODEX_THREAD_ID`。

同一实例后续回合保留句柄；失败启动后的替代实例、显式停止后恢复、会话切换及进程重启均不能沿用旧句柄。旧 Codex 客户端的 `/ipc/target` 路径继续兼容；已取得完整目标的后台作业仍保存五字段目标，而不保存来源 capability。

以下是请求形状示例，占位目标须由真实绑定替换，示例事件 ID 不可用于实际任务：

```json
{
  "event_id": "0000000000000000000000000000000000000000000000000000000000000000",
  "target": {
    "project": "my-project",
    "session_key": "platform:original-conversation",
    "session_id": "s1",
    "agent_session_id": "original-codex-thread-id",
    "cwd": "/path/to/workspace"
  },
  "prompt": "本机任务已结束；结果定位与原任务接续信息。"
}
```

`event_id` 必须为 64 位小写十六进制字符串，针对同一逻辑事件保持稳定。`prompt` 必须非空，最多 8192 UTF-8 字节；整个 POST 请求最多 16 KiB，只接受一个 JSON 对象，拒绝未知字段。

## 持久投递与确认

1. 调用方先保存完整输入、固定目标、事件 ID 和内容指纹，再记录发送尝试并 POST。来源提交、文件路径、消息方向及修订关系由调用方的 outbox 管理，无须给通用请求增加 `source_kind` 等字段。
2. 接收端在向 Agent 输入前原子保存并同步回执；空闲时取得会话锁并再次核对目标，活跃时对固定 thread／turn 追加。会话切换或目标变化不会自动改投其他会话。
3. 相同事件与内容返回已有回执；同一事件更换内容会报冲突。投递已经开始后的结果不明，只查询回执，不自动重发或换事件 ID 重试同一动作。
4. 原会话实际收到消息、核对事件与自身身份后才发 `/ipc/ack`。消息中的来源正文仍是任务资料，后续操作依原用户授权判断。

回执保存在 `<data_dir>/session-ipc/<event_id>.json`，含输入、内容哈希、状态、方法、更新时间及可选 `acknowledged_at`：

| 状态 | 含义与处理 |
|---|---|
| `waiting` | 已明确未接受输入，例如会话忙但 turn 尚未就绪；可在原目标等待后再检查／提交同一输入。曾进入 queue 路径的事件还须通过原实例与 boot 校验 |
| `dispatching` | 已持久记录投递尝试，等待输入结果；只查回执 |
| `delivered` | 后端已接受输入；有 `acknowledged_at` 才进一步证明会话明确确认收件 |
| `unknown` | 输入可能已送达，停止自动重发，核对原会话与两端回执 |
| `blocked` | 目标身份、目录或回复路由不满足条件，保留结果并检查原因 |

服务重启后，旧 boot 遗留的 `dispatching` 按 `unknown` 读取。此设计优先避免重复执行，不提供断电或强杀下的必达保证。

调用边界队列保留以上五种状态，使用下列可选字段记录更细的证据：

| 字段 | 含义 |
|---|---|
| `attempt_id` | 每次实际尝试的随机 ID；同事件从安全 waiting 重试也换 ID，旧回调不可覆盖新尝试 |
| `instance_id` | queue 所属 Core 实例的非秘密随机标识，与原 `CC_IPC_ORIGIN` 独立，不可用它解析来源 |
| `phase` | `queued`、`claimed`、`injected`、`released` 或 `unknown` |
| `queued_at` / `claimed_at` / `injected_at` | 对应阶段持久记录的时间；没有该证据时不填写 |
| `turn_id` | 适配器提供的非秘密实际调用边界标识；不一定是 Codex turn ID |

`queued` 与 `claimed` 都保持 `dispatching`。Core 先持久记录尝试再异步调用队列，HTTP 无须等待下一次 hook。适配器在输出注入前必须等 `claimed` 的持久回调成功；hook 响应写出、随后发生一次模型调用或 turn 计数增加，都不能单独证明特定消息已消费。只有适配器取得可核验的输入消费证据并报告 `injected`，或原会话明确 ack，才转 `delivered`。已确认的 delivered/ack 不会被迟到的失败、释放或停止回调降级。

只有原子移除且从未领取的消息可以报告 `released`。自然结束可转 `waiting`；用户停止或实例已被 Core 停止、替换时转 `blocked`。领取后失联或无法证明注入，转 `unknown` 并冻结；不能释放成 waiting 重新发送。晚回调必须同时匹配原 boot 与 `attempt_id`。

曾进入 queue、自然释放为 waiting 的同一事件，仅在原 boot、原活跃实例仍存在且未停止时允许重试。实例死亡、关闭、替换、消失或服务重启后，重试返回 `blocked` / `ipc_queue_instance_changed`；即使五字段 target 仍相同，也不主动恢复实例来抵消用户停止。转为空闲发送时仍固定原实例，在异步处理开始和 `Send` 前分别核对，不能进入创建替代实例的路径。这条限制不改变全新的空闲请求或既有 Codex waiting 语义。此类 blocked 需按原授权检查结果与实例状态，不删除回执或自动换事件 ID 绕过边界。

`ack` 只确认收到这条事件。附件是否已读、建议是否采纳、任务是否处理、检查是否通过及提交推送结果，由业务调用方另记。Unix socket 是同一操作系统用户内的信任边界；持有 socket 访问权不自动赋予业务操作授权，通用服务端也不证明某份来源内容已经得到用户许可。

## 升级、故障与验证

升级时保留本分支扩展与 App Server 配置，并重新检查 `/ipc/health`、完整目标与 `supports_live_input`。运行的二进制、服务启动路径和预期代码版本须一致；仅拉取 Git 分支不表示服务已经更新。

- initialize 超时：先检查适配器实际读取通道与子进程启动参数。stdio 适配器连接到 WebSocket listener 的配置曾导致握手失败。
- 恢复与新建都失败：保留旧 thread 绑定以便基础设施恢复后重试，不删除会话来掩盖启动失败。
- 收件结果不明：同时核对发送方 outbox 与接收端回执，不删除旧回执，不用旧 PID 处理当前进程。
- 重新验收：分别实际观察原会话的空闲 `start + ack` 和活动 `steer + ack`。健康检查、测试替身、本地模型探针及真实平台收件各自记录覆盖范围；模型接受输入也不代表业务完成。

对应实现见 [core/session_ipc.go](../core/session_ipc.go)、[Codex steer 适配](../agent/codex/session_ipc.go) 与[启动回归测试](../agent/codex/appserver_startup_test.go)。无真实模型调用的定向回归：

```sh
go test -race ./agent/codex ./core -run 'TestAppServerStartup|TestIPC|TestLiveInputProgress|TestResumeFallback|TestCUJ' -count=1
```

DSH、Docs 或其他后台任务程序可以复用此接收器；任务退出检查、自动发现来信、通知结束与会话接续的选择，由各调用方实现并单独验收。
