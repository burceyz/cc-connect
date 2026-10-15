# 任务启动器接入

`workspace/skills/` 中发布的是本版本对应的 Hermes 和桃乐丝启动器源码、必要依赖与回归测试。安装位置分别为工作区的 `skills/cc-dsh-collaboration/scripts/` 和 `skills/cc-dorothy-collaboration/scripts/`；共享通信组件来自本模块的 `skill/scripts/`，安装到 `skills/cc-agent-ipc/scripts/`。

这些入口继续使用已有的本机模型配置、认证文件和任务运行目录。公开源码不包含认证、模型订阅配置、私人工作区规则或历史任务快照。部署时按文件清单更新已配置的入口，保留原运行资料；不要把开发目录整个覆盖到生产工作区。

## 职责

- Hermes 的 `bridge.py` 执行一次任务，`jobs.py` 管理异步任务，`offline.py` 先核对真实执行权限再受理；`instances.py` 管理六个主任务槽及写目录互斥。
- 桃乐丝的 `dorothy.py` 管理六个任务槽、任务回执与准确续谈，`native.py` 使用原生 CLI 并限制文件写入范围。槽0沿用原锁文件。
- `ipc.py` 复用 CC Connect 已有的固定聊天接口，`notify.py` 保留原可选完成通知。它们与本次成员执行中交流分别记录结果。
- `read_sources.py` 及对应策略文件保留研究任务的只读材料规则；现有咨询/研究策略组件属于启动器兼容依赖。

新任务的启动器保存原聊天归属、协作组及登记资料，模型只使用自己的绑定。相同 `--team` 的成员可以互相通信；任务目录仍保持独立。旧任务的记录按原含义读取，安装不会改写已运行进程的配置。

## 临时订阅路由与无工具调用

Hermes运行目录可放置 `routing.json`，例如：

```json
{"default_profile":"happy2","disabled_profiles":["opencode-go"]}
```

普通新任务省略profile时使用这里的默认值；禁用项在同步、受理、预检和worker真正启动前检查。缺少配置文件保留旧Go默认，现存配置非法或为软链接则拒绝执行。`doctor`显示默认值和禁用项。旧记录缺profile仍按Go解释，查询和导出不改历史；恢复旧Go任务需要沿原授权另建执行记录，不能改旧profile或配置指纹。

`--tool-policy none`只接收内联文本，关闭全部工具和运行环境上下文。显式 `--profile happy2` 或 `--profile happy1`可用，仍限DeepSeek/High并核对原生证明；无工具任务也不能启动子Agent。隐式none仍选Go，因此Go被暂停时必须明确选择Happy。`research`继续只接受Go/High。普通来源与工作目录规则不构成系统读取白名单，供应商选取仍须满足调用方已确认的材料边界。

## 调用失败的原因与可见正文

Hermes的新失败回执分别记录 `stream_status`（调用流状态）和 `native_outcome`（对应原生轮次的结束原因）。合法的空正文不再统称为 `invalid_event`；能核对到原生证据时，分别显示 `max_tokens`、`context_window_exceeded`、`provider_error` 等原因。错误流仍保持失败。旧回执不改写，可在安装后的工作区运行：

```sh
python3 skills/cc-dsh-collaboration/scripts/bridge.py diagnose --run /绝对路径/运行目录
```

诊断核对原session、执行目录、任务正文和该轮事件范围，不把后来的成功算到旧失败上；输出仅含原因、数字和哈希，不导出原生错误原文、思考或工具内容。任务文件字节上限不能代替模型容量，材料、历史和回答预留须一并计算。

桃乐丝的 `status` 展示原生状态、错误类别、错误步骤和正文哈希。已验证的原生1.3.3行为是：中途错误重试后或下一轮正常答复时，最终 `ERROR` 仍可能携带此前错误。入口继续要求真实 `SUCCESS` 和正常退出才报告完成；失败终态也等待实际退出，避免主动终止把原退出码覆盖。

需要取回失败轮的可见答复时使用：

```sh
python3 skills/cc-dorothy-collaboration/scripts/dorothy.py export-visible --job 原任务编号
```

该命令核对进程已退出、原生快照哈希、最后一轮任务和完整回复步骤，再把调用流正文与原生JSONL逐字比对。返回 `visible_reply` 与本轮可见消息；`status` 和 `native_success` 保留原事实，不创建 `report.md`，不把取件成功当作任务通过。缺证或正文不一致时明确失败，不重新调用模型。它不读取SQLite、思考字段或工具输出，不自动续谈、换订阅或重投任务。

## 合并与验证

本版本合并了六槽、公共父目录保护和通信接入。公共父目录不能作为单个任务的独占执行目录，具体写目录相同或嵌套时仍拒绝并发；只读来源不占写目录租约。

源文件应在正确安装布局下验证，并用真实消息服务检查登记、连续续谈、收发与恢复。单独的假执行器测试用于核对启动器行为，不能替代原生工具和模型执行中的实际收件。完整版本的验证结果见发布记录，部署步骤见[部署说明](../deploy/README.md)。
