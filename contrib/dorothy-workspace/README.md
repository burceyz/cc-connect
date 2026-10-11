# Dorothy workspace deployment sources

This directory versions the companion Python files changed by the Dorothy
recovery repair together with the Antigravity adapter. These are exact source
copies for the existing `cc-dsh-collaboration` workspace integration, not a
standalone review service or a default CC Connect installation.

Install the four files under `scripts/` into the existing workspace's
`skills/cc-dsh-collaboration/scripts/` directory, keeping the surrounding
`instances`, `jobs`, `ipc`, `peer_ipc`, and notification modules and the review
protocol reference. The bridge retains that integration's fixed workspace and
private runtime configuration. Credentials, provider configuration, conversation
data, task receipts, and native logs are deliberately absent from this source
distribution.

The adapter and these scripts must be deployed together: the adapter now uses
the gate's `bind`, `refresh`, and `finalize` operations. Back up the installed
binary and scripts before switching a running service. Only the dedicated
Dorothy service needs restarting.

Validate from the existing integration's script directory (or an isolated copy
with its existing support modules):

```sh
python3 -B -m unittest test_bridge test_dorothy_review test_ipc test_peer_ipc
```

These tests exercise protocol and recovery behavior with external boundaries
mocked. A real Google model, Hermes review, author response and platform delivery
still require a separate live check.

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
