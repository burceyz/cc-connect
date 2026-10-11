# Dorothy communication backend

## Native CLI integration

On macOS, enable the Antigravity adapter's `communication_mode` with an absolute
`work_dir`, a dedicated `communication_runtime_dir`, and a fixed
`communication_command` argument array. `communication_auth_file` can point to
the host's existing authentication file. A minimal agent options block is:

```toml
[projects.agent.options]
cmd = "/Users/example/.local/bin/agy"
work_dir = "/Users/example/workspace/workdir/dorothy"
model = "gemini-3.8-flash-high"
effort = "high"
communication_mode = true
communication_runtime_dir = "/Users/example/workspace/.local/dorothy-native"
communication_auth_file = "/Users/example/.gemini/jetski-standalone-oauth-token"
communication_command = ["/absolute/python3", "-B", "/absolute/communication.py", "--policy", "/absolute/private/policy.json"]
```

Use a separate CC Connect `data_dir` for the new mode. Existing global/native
conversations and old review jobs are not imported. The mode rejects
`review_command` and extra CLI arguments, fixes its directory and permission
mode, and does not load parent/global skills or memory. Local rule aliases must
point to the same local `AGENTS.md`.

The adapter creates a host-managed hooks/MCP overlay and exposes only the
`cc_communication` server. CLI 1.3.3 uses `call_mcp_tool` with a server and tool
name; both are checked. Its lazy schema reads are limited to the eight known
tool JSON files in the dedicated native cache, including canonical-path and
symlink checks. `search_web` and `read_url_content` remain available; web reads
reject file URLs, embedded credentials, and literal local or private control
endpoints. CLI 1.3.3 returns a `content.md` cache path from `read_url_content`.
Use `view_file` on that exact path to read the body. The host registers only
successful web-tool outputs from the current conversation, checks the body hash
on every read, and rejects other files or symlink escapes. Fetch the URL again
after a host restart. A cache can still contain truncated source content; check
the actual body before claiming that a page has been fully read.

The initial OAuth file is copied only when absent to
`<communication_runtime_dir>/antigravity-cli/antigravity-oauth-token`. Native
refreshes stay in that dedicated copy. Native project state is also stored in
the dedicated runtime. The CLI normalizes and writes its private `settings.json`
on load, so that file is writable; host hooks/MCP configuration, binaries and
workspace rules remain protected. Changing native settings or selecting
"allow all" cannot override the host dispatcher or the Seatbelt process policy.
See [process boundary](../../agent/antigravity/communication_sandbox_README.md).
The CLI's own RPC listeners were observed on loopback; the macOS inbound rule
does not itself prove wildcard binding is impossible.

The trusted host starts one fixed command per request, outside the model's
workspace sandbox:

```text
python3 -B /absolute/path/communication.py --policy /absolute/private/policy.json
```

The backend reuses the existing `peer_ipc`, `ipc`, `jobs`, and capacity check.
It does not expose a general HTTP, socket, shell, file, contact-registration,
or target-rebinding interface. The host must deny those native model tools;
this program is not an operating-system sandbox by itself.

Install the companion `communication.py`, `ipc.py`, `bridge.py`, `jobs.py`, and
both consultation/research policy `.mjs` and `.patch.yml` files in the existing
integration's scripts directory. Consultation uses the fixed `none` or `research`
tool policy supported by `jobs.submit`; the host chooses from these modes,
never from a caller-supplied policy path. The native guard enforces each policy.
Enabling consultation requires the policy-verification helpers and the
`tool_policy` submission parameter; an older integration fails closed.

## Host policy

The policy is an absolute, canonical, owner-only regular file. Model arguments
cannot choose it or override its fields. An example with consultation disabled:

```json
{
  "scripts_dir": "/Users/yz/WorkSpace/cc-connect-demo/skills/cc-dsh-collaboration/scripts",
  "peer_runtime": "/Users/yz/WorkSpace/cc-connect-demo/.local/agent-ipc",
  "dsh_runtime": "/Users/yz/WorkSpace/cc-connect-demo/.local/dsh-collaboration",
  "work_dir": "/Users/yz/WorkSpace/cc-connect-demo/.local/dorothy-communication",
  "source_cwd": "/Users/yz/WorkSpace/cc-connect-demo/workdir/dorothy",
  "source_prefix": "dorothy",
  "allowed_contacts": {"atlas": "atlas"},
  "contact_labels": {"atlas": "本次接入维护与问题处理"},
  "enable_consultation": false,
  "consultation_timeout": 300,
  "materials": {}
}
```

`work_dir` is the backend's private state directory, separate from Dorothy's
native `source_cwd`. The host provides `CC_IPC_ORIGIN`, `CC_PROJECT`,
`CC_SESSION_KEY`, `CC_DATA_DIR`, and `CC_IPC_CWD=source_cwd`. Every event/job
operation obtains its identity from `ipc.capture()`. The request context is
not an identity source. `tools/list` and `list_contacts` need no live origin.
`list_contacts` performs only receiver identity checks, with at most four
concurrent lookups and a two-second socket timeout per lookup.

The IPC library accepts the existing workspace root and canonical, existing
directories below it. The receiver still verifies the actual origin/target.
It rejects noncanonical paths, missing directories, and symlink escapes.
`CC_IPC_CWD` is propagated by the host; changing a model argument cannot set it.

Contact keys are the model-visible aliases; values are pre-registered contact
names. An equivalent list uses each name as its own alias. Sending registers
only a deterministic source name derived from the actual binding, without
`replace`. Existing canonical contacts remain unchanged. Replies retain the
original sender's pinned binding and require its name to remain allowed.

Optional `contact_labels` maps allowed aliases to host-verified, single-line task
descriptions of at most 256 UTF-8 bytes. Unknown aliases, blank descriptions,
surrounding whitespace and control characters are rejected. Missing labels are
shown as `任务主题未核实`; no task is inferred from the contact name or a live session.
Different Atlas sessions should have distinct aliases and verified descriptions.

`list_contacts` returns `{"contacts":[{"alias":"atlas","purpose":"...","status":"available"}]}`.
The fixed status values are `available` (the registered identity still matches
and supports live input), `changed` (a different identity or workspace),
`offline` (receiver connection unavailable), `unknown` (check inconclusive), and
`unregistered` (no registered contact file). An optional `busy` boolean appears
only when the matching receiver actually provides it; `available` alone does
not mean idle. Results contain no socket, path or raw session identifiers.
Checks neither scan the contacts directory nor register, rebind, send an event,
or select a latest session. A changed binding needs an explicit host update.

Optional material IDs map to objects containing `path` and `sha256`. Paths must
point to controlled text copies under `work_dir/materials/`; the backend checks
their original bytes before freezing them. The model can supply only material
IDs. A consultation embeds the question and complete selected text in a data
section and uses Go / High. Its optional `mode` selects `text` (the default,
`tool_policy='none'`) or `research` (`tool_policy='research'`). Explicit `text`
preserves legacy operation IDs and task bytes; old records without a mode remain
text consultations. Neither mode permits subagents or repository operations.

Research exposes only `mcp__duckduckgo__search`, native `web_fetch`, and any
available four fixed `mcp__research_status__status_*` tools. Search and fetch
must be present; a missing status server does not prevent research. All other
MCP, file, shell, runtime-loader, attachment and automatic workspace readers are
disabled. The host checks the exact tool definitions and assembled schemas,
including scope-local shadows and later changes, and installs a monotonic
execution guard. A changed tool generation fails closed; it cannot widen a
running consultation. The native proof binds the actual tool names to the
accepted model/plugin/overlay hashes and session before a result can complete.

The fetch interface accepts ordinary public HTTP(S) domain URLs, without
credentials, IP literals, local hostnames or alternate ports. The native HTTP
backend also bounds responses and validates redirects. Its configured proxy
performs domain resolution on the proxy path; direct requests validate and pin
public DNS results. Research does not disable those checks or change the normal
Hermes profiles. Search snippets, retrieved pages and supplied text are data,
and the task asks Hermes to distinguish them, cite sources and keep disagreements.

Dorothy can request an independent search, compare the returned evidence with
her own, and send the resulting material to the original Atlas requester or an
appropriate configured contact. Atlas selects the authorized destination and,
where the existing repository workflow delegates ingestion, passes the material
to Hermes. The research interface exposes no repository selector, arbitrary
job launcher or filesystem access. A request for research does not itself
authorize writing a repository.

## Request and result

Stdin contains exactly one JSON object:

```json
{
  "protocol": "cc-dorothy-communication-v1",
  "method": "tools/call",
  "params": {
    "name": "send_message",
    "arguments": {"contact": "atlas", "text": "请核对这段资料。"}
  },
  "context": {"operation_scope": "host-pinned-message-or-run-id"}
}
```

`tools/list` uses `params: {}` and returns `{"tools": [...]}`. Calls return one
MCP result object with `content: [{"type":"text","text":"<JSON result>"}]`
and `isError: false` or `true`. Validation failures use an error result with a
fixed code and exit 0; raw exceptions and private paths are not returned.
Complete stdout is capped at 512 KiB, including both JSON layers and escaping.
Text reads use smaller inline pages as described below; other oversized results
return `result_too_large` without a partial or truncated body.
Process-launch errors and transport failures remain host responsibilities.
The optional MCP `_meta` object is accepted and ignored. In particular, native
conversation/artifact metadata cannot select a sender, path, or operation scope;
those continue to come from the host context and verified binding.

The fixed tool names are:

- `list_contacts()`
- `send_message(contact, text)`
- `reply_message(event_id, text)`
- `read_message(event_id, cursor?)`
- `message_status(event_id)`
- `ack_message(event_id)`
- `consult_hermes(question, material_ids?, mode?)`, only when enabled
- `consultation_result(job_id, cursor?)`, only when enabled

Each schema rejects additional fields. Message/question text is limited to
64 KiB of UTF-8. Reads return only events the actual current session sent or
received. Acknowledgement requires the current recipient. Message results omit
socket, filesystem and raw target fields. Received text is returned as data;
an acknowledgement is not a claim that any requested work is complete.

`read_message` and completed `consultation_result` keep the original result shape
for short text. Long text is returned in pages of at most 4 KiB, counting both
JSON layers, UTF-8 and escaping. Each page includes `pagination` with `offset`
(a Unicode character offset), `total_chars`, `text_sha256` for the complete body,
and `next_cursor`. Call the same tool with the same event/job ID and the exact
cursor to continue; concatenate each page's `text` in order until `next_cursor`
is `null`. A completed job can still have unread pages. Omitting the cursor
starts at the beginning and never reruns a consultation or resends a message.

Every page repeats ownership, original-record and integrity checks; consultation
pages also require the accepted request, tool-policy proof and released
processes. Cursors bind the complete result and event/job metadata and reject
changed content or a different result. They are read positions, not file paths
or capabilities. Research results and long incoming material stay inline in the
native client. The client may still record `output.txt` internally, but the
model does not need to read that file. No cache directory or additional
file-read permission is granted.

The host's operation scope and the arguments determine the stable operation
key. Retries reuse the original event and pinned recipient. Unknown delivery
only queries that event, including after a lost response; it never changes
the number or target to obtain a fresh POST. Identical pre-send partial records
may be recovered by the existing peer implementation. Source, body and input
hash checks remain in force.

Consultations use one fixed job per source/scope/question/material-ID set/mode.
Capacity exhaustion returns `busy`, without a queue or automatic retry. A
repeated busy operation remains busy; a new user operation has a new scope.
Unknown submissions are queried under the original job ID and never resubmitted.
Results verify ownership, accepted request, policy, original task and process
release before reporting completion. These tools do not alter historical
Dorothy publication-gate receipts.
Consultation completion callbacks carry the original job ID and direct Dorothy
to `consultation_result`; they do not require general file access or inject the
consultation answer into the conversation.

Run deterministic validation in an isolated integration containing its existing
support modules:

```text
python3 -B -m unittest test_communication test_consultation test_ipc
```

The communication tests use real temporary peer outboxes and mock transport,
watchers and model-job submission, plus bounded concurrent identity checks.
The IPC tests include local fixture sockets
and fresh subprocess imports; they do not call the real host or a model.
The consultation tests also exercise the installed DSH registry, schema assembly,
and headless startup with an offline stop before the model/network boundary.
Deployments must additionally verify a real research request, its native search
and page-reading events, the original-session callback and released processes.
