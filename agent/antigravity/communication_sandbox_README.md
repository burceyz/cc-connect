# Communication process sandbox

`CommunicationSandboxCommand` starts the entire Antigravity process inside
macOS Seatbelt. MCP subprocesses and lifecycle hooks inherit the boundary.
Unsupported platforms return an error instead of starting an unrestricted
process. This component does not deploy a service or edit global CLI settings.

## Host integration

The trusted host supplies `CommunicationSandboxOptions`:

- `WorkDir`: an existing, dedicated agent workspace.
- `RuntimeDir`: a separate existing directory for that agent's mutable state.
- `ReadOnlyPaths`: the exact host-created configuration paths and executables
  needed by this invocation. The main executable is added automatically.
- `LoopbackAddresses`: the host's permission, invocation, and communication
  bridge addresses, each with an explicit loopback IP and fixed port.
- `AllowHTTPS`: permit outbound TCP 443 outside localhost, the system DNS
  resolver, and certificate trust services. Other localhost ports stay denied.

Build the command before merging the explicit session environment. Use
`cmd.Environ()` as the base so `PWD` matches `cmd.Dir`. Place `TMPDIR` under
`RuntimeDir`. This helper does not change `HOME` or copy credentials.

The profile is passed with `sandbox-exec -p` rather than stored in a file the
agent could replace. Path grants use resolved filesystem paths. A configured
read-only symlink protects both its target and its directory entry. Read-only
paths within the runtime also prevent replacement of their ancestor directories;
unrelated SQLite/WAL, cache, and log files remain writable. Writable roots
themselves cannot be removed, renamed, or have their permissions changed.

Use a host-created read-only CLI overlay containing `config/hooks.json` and
`config/mcp_config.json`. Its `antigravity-cli` entry may point to this agent's
dedicated mutable runtime. Include that runtime's `settings.json` path in
`ReadOnlyPaths`, including when it is a symlink to a host-owned file. Do not
mirror the user's global `.gemini` tree, conversation stores, plugins, or rules.

The host still enforces the communication tool allowlist. Seatbelt is not a
replacement for binding a bridge request to the owning conversation and token.
The caller remains responsible for child-process lifecycle and final transport
and authentication verification.

## Explicit system exceptions

The profile permits system library reads and the configured executables. It
does not import Apple's general `system.sb`, grant the entire `/System` tree
(which includes data-volume aliases), or grant a user home or shared temp tree.
macOS dyld requires a read of the literal root directory `/`; this is not a
recursive read grant. Metadata for `/var`, `/tmp`, and `/etc` resolves the normal
macOS aliases. `/private/var/select/sh` supplies the system shell selection.

Seatbelt only accepts `localhost` or `*` in endpoint expressions. Validated
`127.0.0.1:port` and `[::1]:port` inputs therefore become a localhost-port grant
covering loopback address families. No other port is granted implicitly.
Unix-domain sockets are denied, even when the socket file is in `WorkDir`.
The only optional Unix socket exception is the system DNS resolver when HTTPS
is enabled. The host's raw CC Connect Unix socket must never be added.

## CLI configuration evidence

On 2026-10-11, installed Antigravity CLI 1.3.3 successfully ran `--version`,
`--help`, and an isolated `mcp add` under this profile, without a model call.
`--gemini_dir=<root>` places MCP configuration at
`<root>/config/mcp_config.json`, with the standard structure:

```json
{"mcpServers":{"cc_communication":{"command":"/absolute/cc-connect","args":["_agy-communication-tool"]}}}
```

The official permissions guide supports exact resources such as
`mcp(cc_communication/list_contacts)` under `permissions.allow`. Configure only
the host's fixed tool names; do not allow every MCP server. The hook's emitted
tool-name representation must be verified against an actual invocation instead
of inferred from the resource syntax.

The installed CLI contains the Antigravity credential-file name
`jetski-standalone-oauth-token`. Metadata inspection found that file separately
from Gemini CLI's `oauth_creds.json`; no credential contents were read by these
tests. The host can stage only the required Antigravity credential in its
dedicated runtime. Login, token refresh, and model transport require a final
real-session check; the metadata tests do not establish those behaviors.

Sources: [MCP](https://antigravity.google/docs/mcp/),
[permissions](https://antigravity.google/docs/permissions/),
[hooks](https://antigravity.google/docs/hooks/),
[installation and auth](https://antigravity.google/docs/cli/install/).

## Validation

`go test ./agent/antigravity -run '^TestCommunicationSandbox'` checks actual
macOS process enforcement: allowed reads/writes, outside paths, symbolic and hard
links, parent replacement, read-only settings aliases, child inheritance,
denied Unix sockets, denied unlisted TCP ports, and successful named bridge I/O.
All operations use temporary markers and local fixture sockets.

Optionally set `CC_AGY_METADATA_BINARY` to an installed `agy` executable for the
no-model CLI metadata/configuration test. It writes only a sandbox-contained
temporary MCP fixture. Ordinary test runs skip that installed-binary check.
