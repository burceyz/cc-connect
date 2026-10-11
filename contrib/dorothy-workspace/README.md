# Dorothy workspace deployment sources

This directory versions the companion Python files for Dorothy communication
mode and the retained review-gate integration, together with the Antigravity adapter. These are source
copies for the existing `cc-dsh-collaboration` workspace integration, not a
standalone review service or a default CC Connect installation.

Install the files under `scripts/` into the existing workspace's
`skills/cc-dsh-collaboration/scripts/` directory, keeping the surrounding
`instances`, `jobs`, `peer_ipc`, and notification modules and the review
protocol reference. The bridge retains that integration's fixed workspace and
private runtime configuration. Credentials, provider configuration, conversation
data, task receipts, and native logs are deliberately absent from this source
distribution.

## Communication mode

For ordinary conversation, web research, fixed-contact messaging, and optional
Hermes consultation, configure `communication_mode = true` as described in
[communication deployment](COMMUNICATION.md). This mode rejects `review_command`
and does not require review before each reply. Use a dedicated work directory,
CC Connect data directory, and native CLI runtime; do not import old tasks or
global hooks into that runtime.

Back up the installed binary, selected scripts and private configuration before
switching the dedicated Dorothy service. Preserve the existing support modules
and unrelated running jobs. Consultation uses `tool_policy='none'` for text and
`tool_policy='research'` for optional public search and page reading. Each policy
has separate frozen host artifacts; ordinary Hermes tasks and global provider
settings keep their existing capabilities. Dorothy hands material to Atlas for
authorized repository ingestion; consultation never grants repository access.

The callback-only repair in `ipc.py` works with the existing receiver. It does
not require a service restart. New callback workers load the fixed client;
existing workers require an explicit, ownership-checked recovery. See
[callback recovery](IPC-RECOVERY.md) for receipt evidence and retry boundaries.

Validate from the existing integration's script directory (or an isolated copy
with its existing support modules):

```sh
python3 -B -m unittest test_bridge test_dorothy_review test_ipc test_peer_ipc test_communication test_consultation
```

These tests exercise protocol and recovery behavior with external boundaries
mocked. Verify the real native CLI, search, web reading, message round trip,
consultation callback, and process cleanup when deploying communication mode.

## Retained legacy review gate

The remaining review scripts and historical receipts support the explicitly
configured `review_command` integration. They are not loaded by communication
mode. That older adapter/gate pair uses `bind`, `refresh`, and `finalize` and
must be deployed together when retaining that configuration.

Review judgment and disposition are separate. A passing Hermes report may retain
a noncritical `risk` or optional `preference` as confirmed or unresolved when Hermes explicitly assigns
`no_action` with evidence and Dorothy actually acknowledges it. This records an
accepted limitation, not a repaired defect. Unresolved defects, critical risks,
`needs_change` / `unverified`, missing author responses and non-passing verdicts
still block publication. Prior critical findings, defects and findings that
required action (`needs_change` / `unverified`) must be verified closed or refuted
with evidence (`not_confirmed` + `no_action`). They cannot be closed by relabeling
them as a noncritical risk or preference in a later revision.

Acceptance criteria describe the user's deliverable. The host separately checks
manifest binding, real review participation and publication; those future review
steps must not become circular preconditions for reviewing the candidate.

A completed, authentic Hermes response containing malformed JSON is preserved
unchanged and returns `hermes_report_invalid_json`. Recovery requires Dorothy
to bind a `retry_review` request to that job in the manifest; it creates a new
independent review with the original report and unresolved findings included.
It never fixes JSON locally or extracts a passing verdict from invalid text.
Three consecutive format failures stop recovery with
`review_format_attempts_exhausted`. Unknown submissions, unsuccessful exits and
missing native reviewers remain outside this recovery path.
