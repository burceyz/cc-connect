# Dorothy communication backend

The trusted host starts one fixed command per request, outside the model's
workspace sandbox:

```text
python3 -B /absolute/path/communication.py --policy /absolute/private/policy.json
```

The backend reuses the existing `peer_ipc`, `ipc`, `jobs`, and capacity check.
It does not expose a general HTTP, socket, shell, file, contact-registration,
or target-rebinding interface. The host must deny those native model tools;
this program is not an operating-system sandbox by itself.

Install `communication.py` and the companion `ipc.py` in the existing integration's
scripts directory. The installation also needs the consultation implementation
of `jobs.submit(..., tool_policy='none')` when consultation is enabled. That
implementation, rather than a prompt alone, enforces the zero-tool policy.

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

The IPC library accepts the existing workspace root and canonical, existing
directories below it. The receiver still verifies the actual origin/target.
It rejects noncanonical paths, missing directories, and symlink escapes.
`CC_IPC_CWD` is propagated by the host; changing a model argument cannot set it.

Contact keys are the model-visible aliases; values are pre-registered contact
names. An equivalent list uses each name as its own alias. Sending registers
only a deterministic source name derived from the actual binding, without
`replace`. Existing canonical contacts remain unchanged. Replies retain the
original sender's pinned binding and require its name to remain allowed.

Optional material IDs map to objects containing `path` and `sha256`. Paths must
point to controlled text copies under `work_dir/materials/`; the backend checks
their original bytes before freezing them. The model can supply only material
IDs. A consultation embeds the question and complete selected text in a data
section, uses Go / High with `tool_policy='none'`, and requests no subagents.
Operational or code-implementation requests are directed to Atlas.

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
Process-launch errors and transport failures remain host responsibilities.

The fixed tool names are:

- `list_contacts()`
- `send_message(contact, text)`
- `reply_message(event_id, text)`
- `read_message(event_id)`
- `message_status(event_id)`
- `ack_message(event_id)`
- `consult_hermes(question, material_ids?)`, only when enabled
- `consultation_result(job_id)`, only when enabled

Each schema rejects additional fields. Message/question text is limited to
64 KiB of UTF-8. Reads return only events the actual current session sent or
received. Acknowledgement requires the current recipient. Message results omit
socket, filesystem and raw target fields. Received text is returned as data;
an acknowledgement is not a claim that any requested work is complete.

The host's operation scope and the arguments determine the stable operation
key. Retries reuse the original event and pinned recipient. Unknown delivery
only queries that event, including after a lost response; it never changes
the number or target to obtain a fresh POST. Identical pre-send partial records
may be recovered by the existing peer implementation. Source, body and input
hash checks remain in force.

Consultations use one fixed job per source/scope/question/material-ID set.
Capacity exhaustion returns `busy`, without a queue or automatic retry. A
repeated busy operation remains busy; a new user operation has a new scope.
Unknown submissions are queried under the original job ID and never resubmitted.
Results verify ownership, accepted request, policy, original task and process
release before reporting completion. These tools do not alter historical
Dorothy publication-gate receipts.

Run deterministic validation in an isolated integration containing its existing
support modules:

```text
python3 -B -m unittest test_communication test_ipc
```

The communication tests use real temporary peer outboxes and mock transport,
watchers and model-job submission. The IPC tests include local fixture sockets
and fresh subprocess imports; they do not call the real host or a model.
