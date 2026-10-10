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
