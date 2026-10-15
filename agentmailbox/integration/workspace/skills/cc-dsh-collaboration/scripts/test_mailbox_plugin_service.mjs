/**
 * Plugin recovery against the real installed Go service.
 *
 * Proves the item-4 contract end to end: an interrupted claim is replayed with
 * its original generation, an attempted delivery is never re-injected, and an
 * accepted receipt is retried. Run: node --test test_mailbox_plugin_service.mjs
 */

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { execFileSync, spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

const BINARY = process.env.AGENT_MAILBOX_BIN || fileURLToPath(new URL('../../../../../build/agent-mailbox', import.meta.url));
const SESSION = 'session-' + '1'.repeat(8);
const SCOPE = 'cc:service-test';

function resolveDshModules() {
  const which = execFileSync('bash', ['-lc', 'command -v dsh'], { encoding: 'utf8' }).trim();
  if (!which) return null;
  const real = fs.realpathSync(which);
  if (path.basename(real) !== 'bin.js') return null;
  const pkgdir = path.dirname(path.dirname(real));
  const require2 = createRequire(path.join(pkgdir, 'package.json'));
  return { tools: require2.resolve('@deepseek-ai/dsh-tools'), llm: require2.resolve('@deepseek-ai/dsh-llm') };
}

const modules = (() => {
  try {
    return resolveDshModules();
  } catch {
    return null;
  }
})();

const skip = !fs.existsSync(BINARY)
  ? 'agent-mailbox service binary is not built'
  : (modules ? false : 'installed @deepseek-ai/dsh packages not found');

async function startService() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-svc-'));
  const proc = spawn(BINARY, ['serve', '--runtime', dir], { stdio: 'ignore' });
  const socket = path.join(dir, 'api.sock');
  const tokenPath = path.join(dir, 'admin-token');
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (fs.existsSync(socket) && fs.existsSync(tokenPath)) {
      return { dir, proc, socket, tokenPath, admin: fs.readFileSync(tokenPath, 'utf8').trim() };
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  proc.kill();
  throw new Error('agent-mailbox service did not start');
}

function stopService(service) {
  service.proc.kill('SIGTERM');
}

async function loadPlugin() {
  const candidates = [
    new URL('../../../../../skill/scripts/dsh-plugin.mjs', import.meta.url),
    new URL('../../cc-agent-ipc/scripts/dsh-plugin.mjs', import.meta.url),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return import(candidate.href);
  }
  throw new Error('dsh-plugin.mjs not found');
}

function makeCtx(agents) {
  const listeners = new Map();
  const ctx = {
    agents: { get: (id) => agents.get(id) },
    tools: { register: () => () => {} },
    on: (event, fn) => {
      const list = listeners.get(event) || [];
      list.push(fn);
      listeners.set(event, list);
      return () => {};
    },
  };
  ctx.emit = (event, payload) => Promise.all((listeners.get(event) || []).map((fn) => fn(payload)));
  return ctx;
}

function makeAgent(id, cwd = '/tmp') {
  const steered = [];
  const tools = [];
  const agent = {
    id,
    status: 'running',
    session: { header: { id, cwd } },
    ctx: { tools: { register: (definition) => { tools.push(definition); return () => {}; } } },
    steer: (message) => { steered.push(message); },
  };
  return { agent, steered, tools };
}

async function waitFor(predicate, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() >= deadline) throw new Error('waitFor timeout');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function setupChild(service, requestUnix) {
  const parent = (await requestUnix(service.socket, 'POST', '/v1/register', service.admin, {
    registration_key: 'svc-parent', token: 'parent-token', label: 'atlas', kind: 'atlas',
    coordinator: true, scope: SCOPE, team: '', parent_id: '', metadata: {},
  })).mailbox;
  const child = (await requestUnix(service.socket, 'POST', '/v1/register', service.admin, {
    registration_key: 'svc-child-' + Math.random().toString(16).slice(2), token: 'child-token',
    label: 'hermes', kind: 'hermes', coordinator: false, scope: SCOPE, team: 'team-svc',
    parent_id: parent.mailbox_id, metadata: {},
  })).mailbox;
  const message = (await requestUnix(service.socket, 'POST', '/v1/send', 'parent-token', {
    client_message_id: 'c1', to: child.mailbox_id, body: 'recover me', task_id: 'task-1', reply_to: '',
  })).message;
  return { parent, child, message };
}

function childFiles(dir, service, generation, mailboxId) {
  const socketPath = service.socket;
  const bindingFile = path.join(dir, 'binding.json');
  const rootFile = path.join(dir, 'root.json');
  const stateDir = path.join(dir, 'state');
  fs.mkdirSync(stateDir, { recursive: true });
  fs.writeFileSync(bindingFile, JSON.stringify({
    schema: 1, socket: socketPath, token: 'child-token', mailbox_id: mailboxId, generation,
  }));
  fs.writeFileSync(rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
  return { bindingFile, rootFile, stateDir, recoveryFile: path.join(stateDir, 'mailbox-recovery.json') };
}

async function applyPlugin(ctx, paths, dir) {
  const plugin = await loadPlugin();
  await plugin.apply(ctx, {
    bindingFile: paths.bindingFile,
    rootIdentityFile: paths.rootFile,
    stateDir: paths.stateDir,
    toolsModule: modules.tools,
    llmModule: modules.llm,
  });
  return plugin;
}

test('a lost claim response is replayed with its original generation', { skip }, async () => {
  const service = await startService();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-svc-'));
  try {
    const plugin = await loadPlugin();
    const { child, message } = await setupChild(service, plugin.requestUnix);
    await plugin.requestUnix(service.socket, 'POST', '/v1/activate', 'child-token',
      { generation: 'gen-old', native_session_id: SESSION, capabilities: ['steer'] });
    const claimed = await plugin.requestUnix(service.socket, 'POST', '/v1/claim', 'child-token',
      { generation: 'gen-old', request_id: 'req-old' });
    // The launcher owns cross-generation cleanup and ends the task's own
    // previous generation before the resumed plugin starts.
    await plugin.requestUnix(service.socket, 'POST', '/v1/deactivate', 'child-token',
      { generation: 'gen-old' });
    assert.equal(claimed.message.message_id, message.message_id);
    // The process died after the server committed the claim; only the request id survives.
    const paths = childFiles(dir, service, 'gen-new', child.mailbox_id);
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-old', request_id: 'req-old', resolved: false,
        message_id: null, attempt_id: null, message: null, delivery: null,
      },
      receipt: null,
    }));
    const root = makeAgent(SESSION, dir);
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await applyPlugin(ctx, paths, dir);
    await waitFor(() => root.steered.length === 1);
    assert.equal(root.steered[0].source.messageId, message.message_id);
    await waitFor(async () => {
      const view = await plugin.requestUnix(service.socket, 'GET', `/v1/messages/${message.message_id}`, 'child-token');
      return view.message.state === 'accepted';
    });
    const view = await plugin.requestUnix(service.socket, 'GET', `/v1/messages/${message.message_id}`, 'child-token');
    assert.match(view.message.receipts[0].evidence, /^dsh_inbox:/);
    assert.equal(JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim, null);
    assert.equal(child.mailbox_id.length > 0, true);
  } finally {
    stopService(service);
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('an attempted delivery is recorded unknown and never re-injected', { skip }, async () => {
  const service = await startService();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-svc-'));
  try {
    const plugin = await loadPlugin();
    const { child, message } = await setupChild(service, plugin.requestUnix);
    await plugin.requestUnix(service.socket, 'POST', '/v1/activate', 'child-token',
      { generation: 'gen-old', native_session_id: SESSION, capabilities: ['steer'] });
    const claimed = await plugin.requestUnix(service.socket, 'POST', '/v1/claim', 'child-token',
      { generation: 'gen-old', request_id: 'req-old' });
    // The launcher owns cross-generation cleanup and ends the task's own
    // previous generation before the resumed plugin starts.
    await plugin.requestUnix(service.socket, 'POST', '/v1/deactivate', 'child-token',
      { generation: 'gen-old' });
    const paths = childFiles(dir, service, 'gen-new', child.mailbox_id);
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-old', request_id: 'req-old', resolved: true,
        message_id: message.message_id, attempt_id: claimed.attempt_id, message: claimed.message,
        delivery: {
          native_message_id: 'native-1', native_message: { id: 'native-1' },
          receipt_state: 'unknown', receipt_evidence: 'dsh_steer_failed',
        },
      },
      receipt: null,
    }));
    const root = makeAgent(SESSION, dir);
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await applyPlugin(ctx, paths, dir);
    await waitFor(async () => {
      const view = await plugin.requestUnix(service.socket, 'GET', `/v1/messages/${message.message_id}`, 'child-token');
      return view.message.state === 'unknown';
    });
    assert.equal(root.steered.length, 0);
    assert.equal(JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim, null);
  } finally {
    stopService(service);
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('an accepted receipt is retried after a rebuild', { skip }, async () => {
  const service = await startService();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-svc-'));
  try {
    const plugin = await loadPlugin();
    const { child, message } = await setupChild(service, plugin.requestUnix);
    await plugin.requestUnix(service.socket, 'POST', '/v1/activate', 'child-token',
      { generation: 'gen-old', native_session_id: SESSION, capabilities: ['steer'] });
    const claimed = await plugin.requestUnix(service.socket, 'POST', '/v1/claim', 'child-token',
      { generation: 'gen-old', request_id: 'req-old' });
    // The launcher owns cross-generation cleanup and ends the task's own
    // previous generation before the resumed plugin starts.
    await plugin.requestUnix(service.socket, 'POST', '/v1/deactivate', 'child-token',
      { generation: 'gen-old' });
    const paths = childFiles(dir, service, 'gen-new', child.mailbox_id);
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-old', request_id: 'req-old', resolved: true,
        message_id: message.message_id, attempt_id: claimed.attempt_id, message: claimed.message,
        delivery: {
          native_message_id: 'native-1', native_message: { id: 'native-1' },
          receipt_state: 'accepted', receipt_evidence: 'dsh_inbox:native-1',
        },
      },
      receipt: null,
    }));
    const root = makeAgent(SESSION, dir);
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await applyPlugin(ctx, paths, dir);
    await waitFor(async () => {
      const view = await plugin.requestUnix(service.socket, 'GET', `/v1/messages/${message.message_id}`, 'child-token');
      return view.message.state === 'accepted';
    });
    assert.equal(root.steered.length, 0);
    const view = await plugin.requestUnix(service.socket, 'GET', `/v1/messages/${message.message_id}`, 'child-token');
    assert.equal(view.message.receipts[0].evidence, 'dsh_inbox:native-1');
  } finally {
    stopService(service);
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
