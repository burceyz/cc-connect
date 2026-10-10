# Antigravity CLI adapter

The adapter uses one `agy -p` process for each foreground input and resumes only
the conversation ID received from `--output-format stream-json`. It does not
scan a user's conversation directory to guess the first ID. `Send` returns
success after the primary stream reports its initial `user_input` acceptance,
not merely after a process starts.

The official CLI 1.3.3 protocol was the implementation reference. The repository
tests use local helper executables and loopback bridges; they do not call a
Google model or prove a real Hermes review has completed.

## Options

```toml
[projects.agent.options]
cmd = "agy"
work_dir = "/absolute/workspace"
mode = "default"
model = "gemini-3.8-flash-high"
effort = "high"
review_command = ["python3", "/absolute/path/to/dorothy_review.py"]
```

`model` is passed to `--model`. Optional `effort` accepts `low`, `medium`, or
`high` and is passed to `--effort`; no effort is invented when it is unset.
`review_command` is an optional executable/argument array, invoked directly with
JSON on stdin and stdout. A string containing a shell command is rejected.
Normal CLI streaming remains enabled when this option is absent.

For the Dorothy workflow, configure the fixed review script and ensure the
host supplies `CC_PROJECT`, `CC_SESSION_KEY`, and its pinned IPC origin handle.
The review process inherits those host variables. Each CLI run also receives
`CC_REVIEW_TASK_ID`, `CC_REVIEW_TASK_DIR`, and `CC_REVIEW_MANIFEST_PATH` from its
validated `begin` response. Instructions are appended to the CLI prompt only.

The `cc-connect-review-gate-v1` contract has two operations:

- `begin` pins the request, origin, and review task before starting the CLI.
  A normal request must keep the host-proposed task ID. A registered IPC event
  may resolve to its existing task. Unknown events get a deterministic proposed
  UUID and the gate's durable registry decides their final task identity.
- `check` receives the exact UTF-8 candidate and its SHA-256. Publishing requires
  a successful process, matching protocol/task/candidate/manifest hashes,
  `status=verified`, and `allow_publish=true`. The manifest is read before the
  call and checked again afterward. Candidate deltas remain buffered throughout
  a gated run. Pending, blocked, and invalid results produce `EventError` while
  retaining the session for a later callback; they never publish the candidate.

The gated candidate is the final `agent_response` step, assembled from that
step's exact `text_delta` bytes through `DONE`. Earlier progress messages and
the CLI's cumulative `result.response` are excluded. A missing step identity,
unfinished final response, or a later tool/input step without a final response
fails closed before review; it does not fall back to an earlier message.

## Live input

All permission modes receive independent `PreInvocation` and `PostInvocation`
hooks. The default mode additionally retains its tool permission bridge. The
temporary config overlay preserves existing user hooks; an owned hook name
collision is reported instead of silently replacing a user hook. Original
configuration files are not edited.

`LiveInputQueuer` accepts generic sender text. Each process has a fresh public
run generation and a separate secret hook token. Only the bound primary
conversation can claim an input; subagent conversations and old process tokens
cannot consume it. External text is injected as `userMessage`, never as system
instructions or a tool call. Only a post hook that claims input requests
`force_continue`.

Queued and claimed receipts must be persisted before the corresponding next
step. A successful hook write and body-less `user_input` events do not prove
which input the CLI consumed. This adapter therefore reports a claim with its
observed invocation boundary, and leaves unconfirmed claims unknown at process
end. The original agent's explicit acknowledgement can establish delivery.
Unclaimed input is released for an idle retry on natural completion; explicit
stop releases it with `session_stopped`. Claimed input is never automatically
retried.

With a review gate, only callbacks registered to the active review task can be
injected while it is busy. A different task waits for its own foreground run
and candidate. The gate's precise `review_gate_busy` response, before acquiring
any task and before queueing, also returns `ErrNoActiveInput`; arbitrary pending
or failed responses are not generalized into retry permission.

Official protocol references:

- <https://antigravity.google/docs/cli/headless>
- <https://antigravity.google/docs/hooks>

## Tests

```sh
go test ./agent/antigravity ./agent/antigravityhook ./cmd/cc-connect
go test -race ./agent/antigravity ./agent/antigravityhook
go test ./...
go build ./...
```

Tests isolate the hook configuration under a temporary home. They cover first
input acceptance, resume IDs, model/effort propagation, hook ownership and
deduplication, receipt failures, stop races, review identity and byte hashes,
manifest changes, callback task routing, aggregate-response boundaries, and
literal argv handling. Full builds
require the repository's existing frontend build assets under `web/dist`.
