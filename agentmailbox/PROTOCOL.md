# agent-mailbox 协议 v1（实施冻结接口）

本文件先于实现编写，是 Go 服务、Python 客户端、CLI 和 MCP 工具共同的接口约定。
它落实 `DESIGN.md` 的“服务接口 v1”，并固定实际 JSON 包装与客户端返回值。
另一实施者按本文件对接；如需改动，先告知 Atlas，不在代码里另起一套。

本轮修订（2026-10-15，接续 `CORE-FOLLOWUP-01`）：传输状态为五种，显式 `ack` 只写独立的
`received_at`，不再产生第六种状态；`claim` 的 `request_id` 绑定 generation；代次历史不可复活；
回执按 `(state, evidence)` 去重并可通过公开 `GET` 查询。

## 1. 传输与公共约束

- 传输：Unix domain socket 上的 HTTP/1.1，请求与响应正文都是 JSON（`application/json`）。
- 协议名：`agent-mailbox-v1`；`GET /v1/health` 返回 `version: "1"`。
- 所有写操作用 `POST`；读操作用 `GET`。未列出的路径返回 `404 {"error":"not_found"}`。
- 鉴权：`Authorization: Bearer <token>`。
  - 管理令牌：只由受信启动器/接入程序使用，用于 `POST /v1/register`。
  - 邮箱令牌：每个邮箱自己的随机令牌；服务端只保存 SHA-256 摘要。
- 请求体上限 131072 字节（128 KiB）；消息正文上限 61440 字节（60 KiB）且必须是合法 UTF-8。
  服务按原始请求字节校验 UTF-8；含非法字节的请求返回 `400 invalid_request`，不做 U+FFFD 静默替换。
- 服务默认只监听本机私有 Unix socket；不提供客户端任意文件读取接口，不加载模型凭据，不执行用户命令。
- 错误统一为 `{"error":"<稳定 code>"}` 加 HTTP 状态码；不把请求体、令牌或内部异常放进正文或日志。
- 存储 schema 版本为 `2`。遇到旧版本本地数据库时服务拒绝启动并提示重建，不做就地迁移。

### 状态与强度

消息传输状态五种，强度从小到大：

```
pending(0) < dispatching(1) < unknown(2) < offered(3) < accepted(4)
```

- `pending`：服务已持久保存、尚未被接入端领取。
- `dispatching`：领取已持久保存（含 `attempt_id`），引擎调用前/调用中。
- `unknown`：投递结果不明，保留同一消息、同一 attempt 对账；不会自动变回 `pending`。
- `offered`：已交给引擎入口，尚未取得接纳/收件证据。
- `accepted`：引擎明确接纳；不代表业务完成。

显式确认读到（`ack`）不是传输状态：它只写独立的 `received_at`（首次写入，之后幂等），
不改变 `state`，也不覆盖任何更强的传输事实。已经 `ack` 的 `pending` 消息不再被自动 `claim`，
但仍可用 `GET /v1/messages/<id>` 原样查询。`receipt` 只提升传输状态，不降级，并按证据去重。

服务重启后所有事实保留；新的服务启动身份（`instance_id`）不抹掉旧投递事实。
新的 generation 不领取旧 `dispatching` 消息。

## 2. 对象

### mailbox（register / me / activate / heartbeat / deactivate 返回）

```json
{
  "mailbox_id": "mbx_<32 hex>",
  "label": "hermes",
  "kind": "hermes",
  "coordinator": false,
  "scope": "<不透明通信范围>",
  "team": "<协作组，可为空>",
  "parent_id": "",
  "metadata": {},
  "generation": "",
  "native_session_id": "",
  "capabilities": [],
  "active": false,
  "state": "idle",
  "last_seen": 0,
  "created_at": 0
}
```

`state` 由服务派生：`active` 表示当前有活跃 generation，否则 `idle`。令牌与摘要永不返回。

### peer（peers 返回，按 DESIGN 固定字段，不含令牌/scope/父子关系/正文）

```json
{
  "mailbox_id": "mbx_<32 hex>",
  "label": "dorothy",
  "kind": "dorothy",
  "team": "task-1",
  "state": "active",
  "last_seen": 0,
  "capabilities": []
}
```

### message

```json
{
  "message_id": "msg_<32 hex>",
  "client_message_id": "<发送方稳定 id>",
  "sender_id": "mbx_<32 hex>",
  "to_id": "mbx_<32 hex>",
  "body": "<原始 UTF-8 正文>",
  "task_id": "<任务关联>",
  "reply_to": "",
  "state": "pending",
  "attempt_id": "",
  "claimed_by_generation": "",
  "claimed_at": 0,
  "received_at": 0,
  "receipts": [],
  "created_at": 0,
  "updated_at": 0
}
```

- `claimed_by_generation` / `claimed_at` / `attempt_id`：原领取的公开对账信息，随消息一起返回。
- `receipts`：按插入顺序排列的去重回执证据，每项为
  `{"state": "offered|accepted|unknown", "evidence": "<原文>", "created_at": 0}`。
  完全相同 `(message_id, attempt_id, state, evidence)` 的重试只保留一条；不同证据追加。
- `received_at` 是独立收件时间，不由 `receipt` 或传输状态派生。

## 3. 接口

### `GET /v1/health`（无需鉴权）

响应 `200`：

```json
{"protocol": "agent-mailbox-v1", "version": "1", "instance_id": "inst_<16 hex>"}
```

`instance_id` 每次进程启动重新生成，是启动身份，不是持久数据。

### `POST /v1/register`（管理令牌）

请求：

```json
{
  "registration_key": "<受信启动器持久保存的键>",
  "token": "<客户端先生成并持久保存的邮箱令牌>",
  "label": "hermes",
  "kind": "hermes",
  "coordinator": false,
  "scope": "<通信范围>",
  "team": "task-1",
  "parent_id": "",
  "metadata": {}
}
```

- 同 `registration_key`、同请求（含令牌摘要）重复提交：返回原邮箱，`200`。
- 同键换任一字段（含令牌、scope、team、parent_id、metadata）：`409 {"error":"registration_conflict"}`。
- `registration_key`、`token`、`scope` 必填。
- `parent_id` 非空时必须存在、与其同 scope 且该 parent 的 `coordinator=true`，否则 `400 invalid_parent`。
- 响应：`201`（新建）或 `200`（同键幂等）：`{"mailbox": <mailbox>}`。

### `GET /v1/me`（邮箱令牌）

响应 `200`：`{"mailbox": <mailbox>}`。

### `GET /v1/peers`（邮箱令牌）

只返回与自己有通信权限的成员（同 scope 且同非空 team，或父子协调关系，见第 4 节）。
按 `created_at, mailbox_id` 稳定排序。响应 `200`：`{"peers": [<peer>, ...]}`。

### `POST /v1/activate`（邮箱令牌）

请求：`{"generation": "<本次执行的随机身份>", "native_session_id": "", "capabilities": ["..."]}`

- 服务持久保存已用代次。已经结束的 generation 不能被晚到的 `activate` 复活，返回
  `409 {"error":"generation_ended"}`（即使它曾被结束、其他代次运行过也一样）。
- 已有不同且活动的 generation：`409 {"error":"generation_mismatch"}`。
- 同活跃 generation 的完全相同请求幂等；`capabilities` 按给定顺序参与比较，顺序或内容不同都返回
  `409 {"error":"capability_conflict"}`；
  `native_session_id` 在入口去除两端空白后保存：首次可为空、之后可由同代次重复 `activate` 或 `heartbeat` 补齐，已绑定且不同返回 `409 native_session_mismatch`。
- 响应 `200`：`{"mailbox": <mailbox>}`（`active=true`）。

### `POST /v1/heartbeat`（邮箱令牌）

请求：`{"generation": "<当前 generation>", "native_session_id": "..."}`

- generation 必须等于当前活跃 generation，否则 `409 generation_mismatch`。
- 已记录的 `native_session_id` 为空时可补齐；已非空且不同则 `409 native_session_mismatch`。
- 只更新观察（`last_seen`），不因超时抢占旧执行者或重投消息；不依赖 TTL。
- 响应 `200`：`{"mailbox": <mailbox>}`。

### `POST /v1/deactivate`（邮箱令牌）

请求：`{"generation": "<当前 generation>"}`；只关闭自己的代次，generation 不符 `409 generation_mismatch`。
该代次就此结束，之后不能再被 `activate`。未发出的消息保留；已领取而未定论的消息保持
`dispatching` 待对账，仍可用原 `attempt_id` 补 `receipt`。响应 `200`：`{"mailbox": <mailbox>}`（`active=false`）。

### `POST /v1/send`（邮箱令牌）

请求：`{"client_message_id": "...", "to": "mbx_...", "body": "...", "task_id": "...", "reply_to": ""}`

- `sender_id` 取自已鉴权邮箱，请求体不提供也无法冒用。
- 请求先通过校验：`client_message_id`、`to`、`body` 必填；`body` 超限 `413 payload_too_large`，
  非 UTF-8 或字段缺失 `400 invalid_request`；目标必须存在且可通信；`reply_to` 必须属于双方原链。
  只有通过全部校验的请求才进入去重判断；因此非法请求返回各自的错误，不保证返回 409。
- 去重键是 `(sender_id, client_message_id)`：同键、合法且 `to/body/task_id/reply_to` 全同 → 返回原消息；
  同键且合法但任一字段不同 → `409 idempotency_conflict`。
- 目标不存在：`404 not_found`；不同 scope/team 且非父子协调：`403 peer_not_allowed`。
- `reply_to` 非空时必须存在，且原消息的双方与本次双方是同一对（不区分方向：原消息可以是 A→B 或 B→A），
  第三方不合法；否则 `403 reply_chain_invalid`。
- 新消息 `state=pending`。响应 `201`（新建）或 `200`（幂等）：`{"message": <message>}`。

### `GET /v1/messages/<message_id>`（相关邮箱）

可读：发件人、收件人；或同 scope 且是发件人/收件人 `parent_id` 的协调者（`coordinator=true`）。
其余 `404 not_found`（不暴露存在性）。响应 `200`：`{"message": <message>}`，
其中包含原领取信息和去重后的 `receipts`，用于公开对账。

### `GET /v1/inbox?after=<cursor>&limit=<n>`（邮箱令牌）

- 返回发给自己的消息，按 `seq` 升序，`seq > after`；`after` 缺省为 `0`，非法 `400 invalid_cursor`。
- `limit` 缺省 50，最大 200。
- 只读：不领取、不改状态、不引发自动重投；返回内容同样带 `receipts`。
- 响应 `200`：`{"messages": [<message>, ...], "cursor": "<最后一条 seq；无新消息时回传输入的 after>"}`。

### `POST /v1/claim`（邮箱令牌）

请求：`{"generation": "<当前 generation>", "request_id": "<调用方稳定 id>"}`

- 原子领取至多一条自己最早、尚未 `ack` 的 `pending` 消息，置为 `dispatching`，生成稳定 `attempt_id`。
- 同 `(mailbox_id, request_id)` 重试返回原结果（含空结果），即使响应丢失也可幂等重试；
  该 replay 会先于活跃性检查返回历史结果。
- 同一 `request_id` 被不同 generation 使用时返回 `409 {"error":"request_id_conflict"}`。
- 新请求只允许本次活跃 generation；不符 `409 generation_mismatch`。
- 无 pending：`{"message": null, "attempt_id": null}`；有：`{"message": <message>, "attempt_id": "att_..."}`。
- 重试返回的 `message` 是当前记录（状态可能已前进），`attempt_id` 是原 attempt。

### `POST /v1/receipt`（邮箱令牌）

请求：`{"generation": "...", "message_id": "...", "attempt_id": "...", "state": "offered|accepted|unknown", "evidence": ""}`

- 只能更新自己邮箱原领取的消息：消息不存在或 `message.to_id` 不是自己 → `404 not_found`；
  `attempt_id` 必须等于该消息的领取 attempt，`generation` 必须等于该 attempt 的 generation；
  否则 `409 attempt_mismatch` / `409 generation_mismatch`。代次结束后仍可用原 attempt 补对账。
- `state` 只允许 `offered`、`accepted`、`unknown`；其他 `400 invalid_request`。
- 相同 `(state, evidence)` 重复提交只保留一条审计证据；新的证据追加；只提升传输状态，不降级。
- 响应 `200`：`{"message": <message>}`（含去重后的 `receipts`）。

### `POST /v1/ack`（邮箱令牌）

请求：`{"message_id": "..."}`；只允许收件人。显式记录读到：只写独立 `received_at`（首次），
不改传输 `state`；重复调用幂等。不是任务完成。已经 `ack` 的 `pending` 不再被自动 `claim`，
但原文仍可查询。不存在或非自己收件 `404 not_found`。响应 `200`：`{"message": <message>}`。

### 错误 code 表

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | `invalid_request` | 缺字段或字段非法 |
| 400 | `invalid_cursor` | inbox 游标非法 |
| 400 | `invalid_parent` | parent_id 不存在、scope 不符或 parent 不是协调者 |
| 401 | `unauthorized` | 缺令牌或令牌错误 |
| 403 | `peer_not_allowed` | 目标不可通信（异 scope/异 team/非协调父子） |
| 403 | `reply_chain_invalid` | reply_to 不属于双方原链 |
| 404 | `not_found` | 路径、邮箱或消息不存在/不可见 |
| 409 | `registration_conflict` | 同登记键但请求内容不同 |
| 409 | `idempotency_conflict` | 同 `(sender, client_message_id)` 但内容不同 |
| 409 | `request_id_conflict` | 同 `(mailbox_id, request_id)` 被不同 generation 使用 |
| 409 | `generation_mismatch` | generation 不是当前活跃代次 |
| 409 | `generation_ended` | generation 已结束，不能再次 activate |
| 409 | `capability_conflict` | 同活跃 generation 的 capabilities 不同 |
| 409 | `native_session_mismatch` | 已有 native_session_id 且不同 |
| 409 | `attempt_mismatch` | receipt 的 attempt 不是原领取 |
| 413 | `payload_too_large` | 请求体或正文超限 |
| 500 | `internal` | 内部错误（不回传异常原文） |

## 4. 通信权限（服务端唯一判定）

`canCommunicate(a, b)`：`a.scope == b.scope` 且满足其一：

1. `a.team != ""` 且 `a.team == b.team`（同协作组成员）；
2. `a.coordinator == true` 且 `b.parent_id == a.mailbox_id`（我是 b 的协调者）；
3. `b.coordinator == true` 且 `a.parent_id == b.mailbox_id`（b 是我的协调者）。

权限只看 `coordinator` 布尔和父子关系，不看 `kind`。`peers` = 除自己外所有满足
`canCommunicate(me, x)` 的成员。

## 5. binding（私有客户端配置）

```json
{"schema": 1, "socket": "/abs/api.sock", "token": "...", "mailbox_id": "mbx_...", "generation": "..."}
```

CLI/MCP 解析顺序：显式 `--binding` > 环境变量 `AGENT_MAILBOX_BINDING` >
延迟导入 `host_runtime.binding_for_current()`（由桃乐丝提供，不在本模块内）。

- `host_runtime.binding_for_current()` 返回**绑定文件路径（字符串）**；客户端读入该文件后校验。
  为兼容也接受已解析的 `dict`。返回路径为空、文件不存在或校验失败时给出清楚的 `ValueError`。
- 显式 `--binding` / 环境变量路径完全不导入 `host_runtime`。
- `socket` 必须是绝对路径；`schema` 必须为 `1`；`token` 必须非空。

## 6. Python 客户端（`skill/scripts/client.py`）

`Client(socket, token, timeout=12)`；`request(method, path, payload=None)` 返回解析后的 JSON `dict`。
网络失败抛 `MailboxTransportError`，超时抛 `MailboxTimeout`，服务端错误抛 `MailboxError(code, status)`。
客户端不隐藏重试；稳定 id 由调用方生成（`new_id(prefix)`）。

| 方法 | HTTP | 返回 |
| --- | --- | --- |
| `me()` | `GET /v1/me` | `mailbox` dict |
| `peers()` | `GET /v1/peers` | `peers` list |
| `send(payload)` | `POST /v1/send` | `message` dict |
| `get(message_id)` | `GET /v1/messages/<id>` | `message` dict |
| `inbox(after=None)` | `GET /v1/inbox` | `{"messages": [...], "cursor": "..."}` |
| `claim(payload)` | `POST /v1/claim` | `{"message": dict|None, "attempt_id": str|None}` |
| `receipt(payload)` | `POST /v1/receipt` | `message` dict |
| `ack(message_id)` | `POST /v1/ack` | `message` dict（`state` 不变，`received_at` 独立） |
| `activate(payload)` | `POST /v1/activate` | `mailbox` dict |
| `heartbeat(payload)` | `POST /v1/heartbeat` | `mailbox` dict |
| `deactivate(payload)` | `POST /v1/deactivate` | `mailbox` dict |

`Client` 也提供 `health()`（无需令牌）和原始 `request()`，供受信启动器以管理令牌调用 `register`。

## 7. CLI（`skill/scripts/mailbox.py`）

```
mailbox.py [--binding PATH] doctor
mailbox.py [--binding PATH] peers
mailbox.py [--binding PATH] send --to ID --id ID --task-id ID --file PATH [--reply-to ID]
mailbox.py [--binding PATH] show --id ID
mailbox.py [--binding PATH] ack --id ID
mailbox.py [--binding PATH] mcp
```

机器可读 JSON 输出到 stdout；错误提示（中文解释稳定 code）到 stderr，退出码非 0。
`send` 的正文由客户端读文件，服务端不读文件。`show` 输出含原领取与去重回执证据。

## 8. MCP 工具（`skill/scripts/mcp_server.py`，规范 stdio JSON-RPC）

启动时固定一份 binding，模型参数不能换 socket/token/sender。日志走 stderr。
工具只读/发信/确认，不含注册、activate、receipt 管理功能：

| 工具 | 参数 | 结果 |
| --- | --- | --- |
| `list_peers` | `{}` | `{"peers": [...]}` |
| `send_message` | `{"to","id","task_id","body","reply_to"?}` | `{"message": {...}}` |
| `read_message` | `{"message_id"}` | `{"message": {...}}`（含领取与 `receipts`） |
| `ack_message` | `{"message_id"}` | `{"message": {...}}`（只写 `received_at`，不改 `state`） |
| `message_status` | `{"message_id"}` | `{"message": {...}}`（含领取与 `receipts`） |
| `wait_for_message` | `{"after"?, "timeout"?}` | `{"messages": [...], "cursor": "..."}` |

`send_message` 强制要求稳定 `id` 和 `task_id`。`wait_for_message` 只读自己的 inbox 有界等待，
`timeout` 上限 30 秒；显式读取不算重复自动投递。工具错误返回 `isError: true` 与稳定 code。
单次 `wait_for_message` 至多返回一页 inbox（默认 50 条，单条正文至多 60 KiB）；调用方应复用返回的
`cursor` 继续读取，不要把积压全部一次性灌入模型上下文。
