# 独立部署

这里的两个 launchd 模板分别运行消息服务和 CC 接入程序。它们使用自己的服务名，不操作现有 CC Connect 的 launchd 项。CC Connect 需要已经提供 `cc-connect-session-ipc-v1`、`/ipc/chat-target` 和按聊天投递的 `/ipc/event` 接口。

## 安装材料

先在模块目录构建并记录源码版本、二进制 SHA-256：

```sh
go build -trimpath -o build/agent-mailbox ./cmd/agent-mailbox
build/agent-mailbox version
```

把 `skill/` 的内容安装为工作区 `skills/cc-agent-ipc/`。其中 `references/protocol.md` 在源码树中引用唯一协议正文；安装为独立技能时复制其实际内容。现有 Hermes 和桃乐丝调用入口需要同步本版本对应的接入改动，继续使用原来已核实的模型配置和独立目录规则。

以交付的 CC 配置示例填写消息服务 socket、管理员令牌文件路径、CC socket、项目、宿主目录及该项目的会话索引路径，保存为运行目录中的 `cc-adapter.json`。配置只引用令牌文件，不嵌入令牌正文。自动发现读取指定索引，再通过 CC 的公开接口验证完整聊天地址。

替换模板的占位符：

| 占位符 | 内容 |
| --- | --- |
| `@PREFIX@` | 本版本二进制安装目录 |
| `@RUNTIME@` | 消息服务私有运行目录，默认 `CC_DATA_DIR/mailbox` |
| `@PYTHON@` | 已核实的 Python 3.11+ 可执行文件绝对路径（任务启动器使用 `hashlib.file_digest`） |
| `@WORKSPACE@` | 安装技能及已有任务入口的工作区 |
| `@CC_DATA@` | 当前 CC Connect 数据目录 |

路径写入 plist 时使用 XML 转义或 `plistlib`，不要把路径拼成 Shell 命令。运行目录设为0700，配置、日志、令牌和绑定设为0600。模板放到当前用户的 `Library/LaunchAgents`，先用 `plutil -lint` 验证，再依次 bootstrap 消息服务和接入程序。保存原文件摘要及恢复副本；升级只替换清单中的文件。

## 安装验收

1. 核对两个新 launchd 服务的实际 PID、退出状态及服务健康接口。
2. 当前 Atlas 执行 `mailbox.py doctor`，核对自身绑定；同组新建 Hermes 和桃乐丝应互相可见。
3. 实际执行中交换只有各自已知的标记，检查消息、原生接纳证据和双方收件确认。向当前 Atlas 投递并由 Atlas 实际取件、确认。
4. 新消息服务停启后原消息仍可查询，已尝试投递的消息不会自动重发。两个新服务的停止、恢复仅使用它们自己的 label。
5. 安装前后比较原 CC Connect PID、启动时间、二进制 SHA-256 和 socket inode，确认它没有重启或替换。

## 恢复

停止接入程序，再停止消息服务，恢复已记录的源码/二进制和配置版本，然后依次启动消息服务、接入程序。数据库、管理员令牌、登记、绑定与投递记录保留，不以删除运行资料来回退。已经执行的模型任务也不因回退重跑。

协议的数据库 schema 与发布版本分开记录。本首版使用 schema2；未发布的旧试验库不做就地迁移，首次部署使用新私有运行目录。以后如需变更存储格式，应先提供对应迁移方法，不能对已投入使用的数据库套用“重建”步骤。

CLI和故障对账的具体操作见[维护说明](../skill/references/operations.md)。
