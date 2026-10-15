# agent-mailbox（本机公共 Agent 消息服务）

本目录是独立的 Go 模块，提供只在本机运行的公共消息服务，以及 Python 标准库
客户端、命令行和 MCP 工具。Atlas、Hermes和桃乐丝通过各自的公开接口接入，同一任务组的成员可以在执行中直接交流。
接口契约见 [PROTOCOL.md](PROTOCOL.md)，组件与消息恢复设计见 [DESIGN.md](DESIGN.md)。

它只保存通信状态：不加载模型凭据、不启动模型或用户命令、不提供任意文件读取接口。
它不导入任何 CC Connect 包，可以单独构建和运行。

CC Connect 使用独立接入程序，无需为安装本服务重启。任务启动器自动登记成员，模型使用自己的固定邮箱；执行完成后释放模型任务槽，邮箱与未读消息保留。操作入口见[技能说明](skill/SKILL.md)，macOS安装与恢复见[部署说明](deploy/README.md)。

## 构建与运行

```bash
go build ./...
go run ./cmd/agent-mailbox serve --runtime /path/to/private-runtime
```

服务在 `--runtime` 目录生成 `api.sock`、`mailbox.sqlite3`、`admin-token`，并持有
独占运行锁；目录 0700，socket 与令牌 0600。SIGTERM 只停止接收并关闭数据库，
不清除消息。管理令牌只给受信启动器登记成员使用。

## 测试

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Python 测试（会自行构建上面的服务二进制，并用两个独立客户端跑真实往返）：

```bash
cd skill/tests
python3 -m unittest test_client_unit test_mailbox_e2e
```

## 客户端

```bash
python3 skill/scripts/mailbox.py --binding <binding.json> doctor
python3 skill/scripts/mailbox.py --binding <binding.json> peers
python3 skill/scripts/mailbox.py --binding <binding.json> send --to ID --id ID --task-id ID --file body.txt
python3 skill/scripts/mailbox.py --binding <binding.json> show --id ID
python3 skill/scripts/mailbox.py --binding <binding.json> ack --id ID
python3 skill/scripts/mailbox.py --binding <binding.json> mcp
```

binding 解析顺序：显式 `--binding` > 环境变量 `AGENT_MAILBOX_BINDING` >
延迟导入 `host_runtime.binding_for_current()`（本模块的宿主接入组件）。
显式 binding 的客户端不依赖宿主运行时。
