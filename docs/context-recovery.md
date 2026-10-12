# Local context recovery

`context_recovery` is an opt-in Codex `app_server` capability. It handles a structured terminal `invalid_encrypted_content` error from resume, input submission or a running turn. Ordinary assistant text, other HTTP 400 responses and retrying notifications do not trigger it. The default remains disabled:

```toml
[projects.agent.options]
backend = "app_server"
context_recovery = false
```

An operator can enable it for a separately approved rollout. Enabling it with `backend = "exec"` is a configuration error. Recovery requires a persistent session store and a local executor whose shutdown can be confirmed; it does not recover an externally owned WebSocket executor.

## Continuing the task

Normal conversations use their existing visible history and host input records. The model does not need to write periodic summaries, task tables or checkpoints. After a qualifying fault the host:

1. Reserves the one allowed recovery attempt durably, tied to the logical host session.
2. Confirms the old executor has exited and its transport readers have stopped.
3. Exports visible native history and gathers the original input, host history, invocation source and known IPC input attempts.
4. Starts a clean native context with the old executor's actual model, reasoning effort, cwd, permissions, preamble, executable and environment snapshot.
5. Saves the exact old-to-new binding, includes queued user corrections and sends a continuation instruction once.

Preparation is capped at 90 seconds and by the remaining original turn deadline. `/stop`, `/new` and `/switch` invalidate a pending recovery. A late startup result is closed rather than installed into a replaced conversation. `/ps` corrections are retained as user input and queued while recovery is preparing or its first input is pending. A qualifying `/ps` submission failure enters the same recovery path, including a response that arrives after the original turn's result; a response from a replaced invocation or executor cannot interrupt the new one.

Each invocation owns its recovery state and any queued inputs claimed for the continuation. A completed recovery record does not change how later ordinary failures drain the queue. Claimed inputs retain their original reply destinations until a result is delivered or a failure/cancellation is reported. Corrections received while recovery storage is being saved are incorporated before the first send, within the existing queue limit and preparation deadline. Material generation and storage waits do not hold the engine-wide or interactive-state lock.

The continuation instruction tells the model that evidence can be incomplete. It must inspect relevant authorized instructions, artifacts and receipts, respect later user changes or cancellation, and continue the remaining work. Completed actions must not be repeated. Unknown outcomes require reconciliation before retrying the affected action; other independent work can continue. Existing background jobs are queried or awaited rather than dispatched again. No new access, upstream investigation or deployment is authorized by recovery.

There is no deterministic business-state reconstruction: the host supplies evidence and execution facts, while the model checks task meaning against the actual workspace. Missing evidence is reported instead of replaced with a guessed completion state.

## Materials and privacy

The host writes a private `context-recovery/<recovery-id>/` directory beside the session store. `visible-material.json` holds filtered source evidence and the preparation snapshot; `continuation.txt` holds the exact recovery input. Directories use mode `0700`, files `0600`, and symlink material directories are rejected. Keep these runtime artifacts out of source control; they can contain ordinary conversation and tool data.

Native export uses the failed executor's captured home environment and actual cwd, and checks the exact session ID, canonical cwd, unique source file and non-subagent metadata. Only visible user/assistant messages (including `final_answer`) and explicit tool input/output are exported. Analysis, reasoning, hidden summaries, encrypted content and raw system/developer payloads are excluded. The native source path is provenance only; the continuation directs the model to the filtered material rather than raw session logs. Original and queued inputs retain the sender context already enabled by `inject_sender`, separately from their unchanged content and digest.

Native limits are 4 MiB per raw line, 256 KiB per visible item, and 16 MiB / 8192 visible items overall. Oversize lines do not stop later user corrections from being read. The shared budget retains opening history, recent user changes and recent evidence, so a long tool-output tail cannot evict all recent user changes. Omissions, incomplete reads, duplicate log representations and missing media are stated explicitly. The inline continuation is capped at 96 KiB, prioritizing the failed input and latest user updates. The bundle retains the available full exported evidence. Current in-memory attachments are supplied to the new turn; the JSON contains only their names, MIME types, sizes and counts.

## Attempts and receipts

`Session.context_recovery` is the current host-owned ledger. Its `id`, source/target native IDs, original input digest, `origin`, timestamps, `phase`, `input_status`, `turn_status` and `error_code` are persisted. A completed model turn is recorded as `model_turn_completed`, not as verified business completion. An explicit result is stronger evidence of input acceptance than a later lost Send response.

The attempt is not reset by a native ID change, another user message, a process restart or a successful model response. Idle auto-reset does not replace a host session that has a recovery record, and stale-lock cleanup does not break a recovery in progress. Selecting a known ancestor native ID keeps the exact requested ID and inherits the same spent budget within the owning session key. `/new` creates an explicit new host session and a new budget. Without a separate application task ID, this host-session chain is the conservative task boundary.

An interrupted preparation/send is never automatically replayed after restart. An unresolved shutdown, persistence failure or cancelled send with unknown delivery blocks normal reuse as well. A receipt write failure preserves any known model-completion evidence while retaining the reuse gate. Review the saved materials, process state, binding and action receipts before selecting an appropriate context; creating a new session alone does not establish what business actions already happened.

The same admission check applies to foreground input, consecutive queued turns and late queue pickup. A blocked input receives the specific reason, recovery ID, snapshot path and material directory, with instructions to reconcile the evidence before using `/new` for an appropriate new context.

All session snapshot saves use the same replacement contract, including ordinary saves when recovery is disabled. A temporary file in the destination directory is written and synced before replacement; there is no in-place overwrite fallback. Unix also syncs the directory. Windows uses `MoveFileExW` with replacement and write-through flags instead of syncing a read-only directory handle. Write failures are reported. An existing snapshot that cannot be read or decoded is preserved and reported as unavailable, rather than treated as a first start. Engine startup, workspace input, relay and session-pruning commands reject that storage error.

Timer and cron inputs carry their original occurrence metadata. Recovery stays in that invocation; it does not refire or reschedule a trigger. A failed prompt turn returns an error to the scheduler, including for muted jobs. A shell timer still runs its original shell command once; when that command injects IPC input, recovery records the input as IPC and retains its supplied task instructions.

The scheduler explicitly marks `new_per_run` sessions as independent. Reused timer/cron sessions remain subject to the active conversation's ownership checks; a session-key suffix does not grant independence.

IPC adds a separate `context_recovery` receipt field. Original target, input/hash, delivery status and acknowledgement are unchanged. Current accepted input and uncertain steer attempts are visible evidence for continuation. Future events or ACKs addressed to the old native ID still fail their exact target check. Callback migration and a general IPC identity redesign are outside this feature.

## Local validation

Regression tests use fake agents, temporary stores and a fake app-server process, without real model requests. They cover startup/send/terminal rejection, current queued input, late corrections, stop/new/switch, close/write failures, persistent attempt budgets, ancestor selection, receipt ordering, timer outcomes, fixed IPC targets, history filtering and private material storage. Storage regressions include unreadable snapshots, concurrent saves, relay rejection, failed replacements and long paths. Queue regressions include persistence failure during late pickup and ordinary failures after successful recovery. CUJs `A9` and `A10` exercise task/scope/continue and stop-during-recovery through `ReceiveMessage`.

Run ordinary repository tests with real-provider integration tests disabled in environments where CLIs or credentials are present. The feature has no installation or service-restart action.
