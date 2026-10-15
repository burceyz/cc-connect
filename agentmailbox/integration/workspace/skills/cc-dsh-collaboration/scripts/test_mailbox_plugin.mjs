/**
 * Offline tests for skill/scripts/dsh-plugin.mjs.
 *
 * Uses the real installed `@deepseek-ai/dsh-tools` and `@deepseek-ai/dsh-llm`
 * packages (resolved from the installed `dsh`) for defineTool/createUserMessage,
 * and a real Unix-socket HTTP server implementing the frozen protocol subset.
 * Run: node --test test_mailbox_plugin.mjs
 */

import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';

const SESSION = 'session-' + '1'.repeat(8);

function resolveDshModules() {
  const which = execFileSync('bash', ['-lc', 'command -v dsh'], { encoding: 'utf8' }).trim();
  if (!which) return null;
  const real = fs.realpathSync(which);
  if (path.basename(real) !== 'bin.js' || path.basename(path.dirname(real)) !== 'lib') return null;
  const pkgdir = path.dirname(path.dirname(real));
  if (!fs.existsSync(path.join(pkgdir, 'package.json'))) return null;
  const require = createRequire(path.join(pkgdir, 'package.json'));
  return {
    tools: require.resolve('@deepseek-ai/dsh-tools'),
    llm: require.resolve('@deepseek-ai/dsh-llm'),
  };
}

const modules = (() => {
  try {
    return resolveDshModules();
  } catch {
    return null;
  }
})();

function json(res, status, value) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': body.length });
  res.end(body);
}

class FakeMailbox {
  constructor(socketPath) {
    this.socketPath = socketPath;
    this.mailboxId = 'mbx_' + 'a'.repeat(32);
    this.activation = null;
    this.activateCalls = 0;
    this.activateDelayMs = 0;
    this.deactivationCalls = [];
    this.deactivationDelayMs = 0;
    this.activeGeneration = null;
    this.claimed = new Map();
    this.claimCalls = [];
    this.receipts = [];
    this.acks = [];
    this.messages = [];
    this.loseNextClaimResponse = false;
    this.failReceipts = false;
    this.claimCounter = 0;
    this.server = http.createServer((req, res) => this.handle(req, res));
  }

  listen() {
    return new Promise((resolve) => this.server.listen(this.socketPath, resolve));
  }

  close() {
    return new Promise((resolve) => {
      this.server.closeAllConnections?.();
      this.server.close(resolve);
    });
  }

  addMessage({ id = 'msg_' + Math.random().toString(16).slice(2), body = 'hello', taskId = 't1' } = {}) {
    const message = {
      message_id: id,
      client_message_id: 'c1',
      sender_id: 'mbx_' + 'b'.repeat(32),
      to_id: this.mailboxId,
      body,
      task_id: taskId,
      reply_to: '',
      state: 'pending',
      attempt_id: '',
      claimed_by_generation: '',
      claimed_at: 0,
      received_at: 0,
      receipts: [],
    };
    this.messages.push(message);
    return message;
  }

  seedClaim(requestId, generation, message, attemptId) {
    this.claimed.set(requestId, { message, attempt_id: attemptId, generation });
    message.state = 'dispatching';
    message.attempt_id = attemptId;
    message.claimed_by_generation = generation;
    return message;
  }

  handle(req, res) {
    let raw = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      raw += chunk;
    });
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) : {};
      this.route(req, res, body);
    });
  }

  route(req, res, body) {
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname === '/v1/activate') {
      this.activation = body;
      this.activateCalls += 1;
      this.activeGeneration = body.generation;
      if (this.activateDelayMs > 0) {
        setTimeout(() => json(res, 200, { mailbox: this.mailbox() }), this.activateDelayMs);
        return;
      }
      json(res, 200, { mailbox: this.mailbox() });
      return;
    }
    if (url.pathname === '/v1/deactivate') {
      this.deactivationCalls.push(body);
      this.activeGeneration = null;
      if (this.deactivationDelayMs > 0) {
        setTimeout(() => json(res, 200, { mailbox: this.mailbox() }), this.deactivationDelayMs);
        return;
      }
      json(res, 200, { mailbox: this.mailbox() });
      return;
    }
    if (url.pathname === '/v1/inbox') {
      const after = Number(url.searchParams.get('after') || '0');
      json(res, 200, {
        messages: this.messages.map((m, i) => ({ ...m, seq: i + 1 })).filter((m) => m.seq > after),
        cursor: String(this.messages.length),
      });
      return;
    }
    if (url.pathname === '/v1/claim') {
      this.claimCalls.push({ generation: body.generation, request_id: body.request_id });
      let entry = this.claimed.get(body.request_id);
      if (entry && entry.generation !== body.generation) {
        json(res, 409, { error: 'request_id_conflict' });
        return;
      }
      if (!entry && body.generation !== this.activeGeneration) {
        json(res, 409, { error: 'generation_mismatch' });
        return;
      }
      if (!entry) {
        this.claimCounter += 1;
        const message = this.messages.find((m) => m.state === 'pending' && !m.received_at);
        if (message) {
          message.state = 'dispatching';
          message.attempt_id = 'att_' + this.claimCounter;
          message.claimed_by_generation = body.generation;
          entry = { message, attempt_id: message.attempt_id, generation: body.generation };
        } else {
          entry = { message: null, attempt_id: null, generation: body.generation };
        }
        this.claimed.set(body.request_id, entry);
      }
      if (this.loseNextClaimResponse) {
        this.loseNextClaimResponse = false;
        req.socket.destroy();
        return;
      }
      json(res, 200, { message: entry.message ? { ...entry.message } : null, attempt_id: entry.attempt_id });
      return;
    }
    if (url.pathname === '/v1/receipt') {
      if (this.failReceipts) {
        json(res, 500, { error: 'internal' });
        return;
      }
      this.receipts.push(body);
      const message = this.messages.find((m) => m.message_id === body.message_id);
      if (message) {
        message.receipts.push({ state: body.state, evidence: body.evidence, created_at: 0 });
        if (body.state === 'accepted') message.state = 'accepted';
      }
      json(res, 200, { message });
      return;
    }
    if (url.pathname.startsWith('/v1/messages/')) {
      const message = this.messages.find((m) => m.message_id === url.pathname.split('/').pop());
      json(res, message ? 200 : 404, message ? { message } : { error: 'not_found' });
      return;
    }
    if (url.pathname === '/v1/ack') {
      this.acks.push(body);
      const message = this.messages.find((m) => m.message_id === body.message_id);
      if (message) message.received_at = message.received_at || 1;
      json(res, 200, { message });
      return;
    }
    if (url.pathname === '/v1/peers') {
      json(res, 200, { peers: [] });
      return;
    }
    if (url.pathname === '/v1/send') {
      json(res, 201, {
        message: {
          message_id: 'msg_sent', client_message_id: body.client_message_id, sender_id: this.mailboxId,
          to_id: body.to, body: body.body, task_id: body.task_id, state: 'pending', received_at: 0, receipts: [],
        },
      });
      return;
    }
    json(res, 404, { error: 'not_found' });
  }

  mailbox() {
    return { mailbox_id: this.mailboxId, kind: 'hermes', team: 't', scope: 'cc:x', active: true };
  }
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
    ctx: {
      tools: {
        register: (definition) => {
          tools.push(definition);
          return () => {
            const index = tools.indexOf(definition);
            if (index >= 0) tools.splice(index, 1);
          };
        },
      },
    },
    steer: (message) => {
      steered.push(message);
    },
  };
  return { agent, steered, tools };
}

async function waitFor(predicate, timeoutMs = 4000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return;
    if (Date.now() >= deadline) throw new Error('waitFor timeout');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

function setup(dir) {
  const socketPath = path.join(dir, 'api.sock');
  const bindingFile = path.join(dir, 'binding.json');
  const rootFile = path.join(dir, 'root.json');
  const stateDir = path.join(dir, 'state');
  fs.mkdirSync(stateDir, { recursive: true });
  fs.writeFileSync(bindingFile, JSON.stringify({
    schema: 1, socket: socketPath, token: 'tok', mailbox_id: 'mbx_' + 'a'.repeat(32), generation: 'gen-test',
  }));
  return { socketPath, bindingFile, rootFile, stateDir, recoveryFile: path.join(stateDir, 'mailbox-recovery.json') };
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

function configFor(paths) {
  return {
    bindingFile: paths.bindingFile,
    rootIdentityFile: paths.rootFile,
    stateDir: paths.stateDir,
    toolsModule: modules.tools,
    llmModule: modules.llm,
  };
}

const skip = modules ? false : 'installed @deepseek-ai/dsh packages not found';

test('apply rejects a missing module location', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const plugin = await loadPlugin();
  const ctx = makeCtx(new Map());
  await assert.rejects(() => plugin.apply(ctx, { ...configFor(paths), toolsModule: '/missing/tools.mjs' }),
    /dsh_mailbox_tools_module_unavailable/);
});

test('waits for the root identity without blocking agent creation', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await new Promise((resolve) => setTimeout(resolve, 400));
    assert.equal(mailbox.activation, null);
    assert.equal(root.tools.length, 0);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    await waitFor(() => mailbox.activation !== null);
    await waitFor(() => root.tools.length === 6);
  } finally {
    await mailbox.close();
  }
});

test('binds the exact root session, not the first root-like agent', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const child = makeAgent('session-subagent', dir);
    const root = makeAgent(SESSION, dir);
    const agents = new Map([[child.agent.id, child.agent], [root.agent.id, root.agent]]);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(agents);
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    assert.equal(mailbox.activation.native_session_id, SESSION);
    await waitFor(() => root.tools.length === 6);
    assert.equal(child.tools.length, 0);
    assert.deepEqual(root.tools.map((tool) => tool.name).sort(),
      ['ack_message', 'list_peers', 'message_status', 'read_message', 'send_message', 'wait_for_message']);
    for (const tool of root.tools) {
      assert.equal(tool.parameters.type, 'object', `${tool.name} parameters`);
      assert.equal(typeof tool.execute, 'function', `${tool.name} execute`);
      assert.equal(typeof tool.output.render, 'function', `${tool.name} render`);
      assert.ok(tool.output.schema, `${tool.name} output schema`);
    }
  } finally {
    await mailbox.close();
  }
});

test('refuses a session id whose live header does not match', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const impostor = makeAgent('session-other', dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, impostor.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await new Promise((resolve) => setTimeout(resolve, 600));
    assert.equal(mailbox.activation, null);
  } finally {
    await mailbox.close();
  }
});

test('turn-stopping injects one framed message and records accepted', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    const pending = mailbox.addMessage({ body: '需要确认接口字段', taskId: 'task-9' });
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 1);
    const message = root.steered[0];
    assert.equal(message.role, 'user');
    assert.ok(Object.isFrozen(message));
    assert.equal(message.source.kind, 'agent-mailbox');
    assert.equal(message.source.form, 'relay');
    assert.equal(message.source.messageId, pending.message_id);
    const text = message.content[0].text;
    assert.match(text, /外部协作资料/);
    assert.match(text, new RegExp(pending.sender_id));
    assert.match(text, /task-9/);
    assert.match(text, /需要确认接口字段/);
    assert.equal(mailbox.receipts.length, 1);
    assert.equal(mailbox.receipts[0].state, 'accepted');
    assert.match(mailbox.receipts[0].evidence, /^dsh_inbox:/);
    assert.equal(mailbox.messages[0].state, 'accepted');
  } finally {
    await mailbox.close();
  }
});

test('background polling does not wake an idle agent', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    root.agent.status = 'idle';
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    mailbox.addMessage();
    await new Promise((resolve) => setTimeout(resolve, 1300));
    assert.equal(root.steered.length, 0);
    assert.equal(mailbox.claimCalls.length, 0);
    root.agent.status = 'running';
    await waitFor(() => root.steered.length === 1, 3000);
  } finally {
    await mailbox.close();
  }
});

test('empty inbox performs no claim and advances the cursor', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    const delivered = mailbox.addMessage();
    delivered.state = 'accepted';
    delivered.received_at = 1;
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(mailbox.claimCalls.length, 0);
    assert.equal(root.steered.length, 0);
  } finally {
    await mailbox.close();
  }
});

test('an unknown claim result reuses the same request id', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    mailbox.addMessage({ body: 'lost once' });
    mailbox.loseNextClaimResponse = true;
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 0);
    assert.equal(JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim.resolved, false);
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 1);
    assert.equal(mailbox.claimCalls.length, 2);
    assert.equal(mailbox.claimCalls[0].request_id, mailbox.claimCalls[1].request_id);
    assert.equal(mailbox.claimCalls[0].generation, 'gen-test');
    assert.equal(JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim, null);
  } finally {
    await mailbox.close();
  }
});

test('an unconfirmed receipt blocks claiming the next message until it is flushed', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    mailbox.failReceipts = true;
    mailbox.addMessage({ id: 'msg_A', body: 'first' });
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 1);
    assert.equal(JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).receipt.state, 'accepted');
    mailbox.addMessage({ id: 'msg_B', body: 'second' });
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 1);
    assert.equal(mailbox.messages.find((m) => m.message_id === 'msg_B').state, 'pending');
    mailbox.failReceipts = false;
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 2);
    assert.equal(root.steered[1].source.messageId, 'msg_B');
  } finally {
    await mailbox.close();
  }
});

test('a slow activation is attempted once and registers six tools once', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  mailbox.activateDelayMs = 800;
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    await waitFor(() => root.tools.length === 6, 3000);
    await new Promise((resolve) => setTimeout(resolve, 600));
    assert.equal(mailbox.activateCalls, 1);
    assert.equal(root.tools.length, 6);
  } finally {
    await mailbox.close();
  }
});

test('disposal during a slow activation prevents the binding', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  mailbox.activateDelayMs = 700;
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    await ctx.emit('agent/disposed', { agent: root.agent });
    await new Promise((resolve) => setTimeout(resolve, 900));
    assert.equal(root.tools.length, 0);
    assert.equal(mailbox.activateCalls, 1);
    await waitFor(() => mailbox.deactivationCalls.length >= 1);
    assert.equal(mailbox.deactivationCalls[0].generation, 'gen-test');
  } finally {
    await mailbox.close();
  }
});

test('a cross-generation rebuild replays the original claim and delivers once', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    const pending = mailbox.addMessage({ id: 'msg_old', body: 'recover me' });
    mailbox.seedClaim('req-old', 'gen-old', pending, 'att-old');
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: { generation: 'gen-old', request_id: 'req-old', resolved: false, message_id: null, attempt_id: null, message: null, delivery: null },
      receipt: null,
    }));
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    await waitFor(() => root.steered.length === 1);
    assert.equal(root.steered[0].source.messageId, 'msg_old');
    await waitFor(() => JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim === null);
    assert.equal(mailbox.receipts[0].generation, 'gen-old');
    assert.equal(mailbox.receipts[0].message_id, 'msg_old');
    assert.equal(mailbox.messages[0].state, 'accepted');
  } finally {
    await mailbox.close();
  }
});

test('an attempted delivery with no acceptance is never re-injected', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    const pending = mailbox.addMessage({ id: 'msg_try', body: 'attempted' });
    mailbox.seedClaim('req-try', 'gen-test', pending, 'att-try');
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-test', request_id: 'req-try', resolved: true, message_id: 'msg_try',
        attempt_id: 'att-try', message: pending,
        delivery: { native_message_id: 'native-try', native_message: { id: 'native-try' }, receipt_state: 'unknown', receipt_evidence: 'dsh_steer_failed' },
      },
      receipt: null,
    }));
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.receipts.length === 1);
    assert.equal(root.steered.length, 0);
    assert.equal(mailbox.receipts[0].state, 'unknown');
    assert.equal(mailbox.messages[0].state, 'dispatching');
    await waitFor(() => JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim === null);
  } finally {
    await mailbox.close();
  }
});

test('an accepted-but-unsent receipt is retried after a rebuild', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    const pending = mailbox.addMessage({ id: 'msg_acc', body: 'accepted once' });
    mailbox.seedClaim('req-acc', 'gen-old', pending, 'att-acc');
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-old', request_id: 'req-acc', resolved: true, message_id: 'msg_acc',
        attempt_id: 'att-acc', message: pending,
        delivery: { native_message_id: 'native-acc', native_message: { id: 'native-acc' }, receipt_state: 'accepted', receipt_evidence: 'dsh_inbox:native-acc' },
      },
      receipt: null,
    }));
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.receipts.length === 1);
    assert.equal(root.steered.length, 0);
    assert.equal(mailbox.receipts[0].state, 'accepted');
    assert.equal(mailbox.receipts[0].generation, 'gen-old');
    assert.equal(mailbox.messages[0].state, 'accepted');
  } finally {
    await mailbox.close();
  }
});

test('a ghost claim with no service record is dropped instead of wedging', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-ghost', request_id: 'req-ghost', resolved: false,
        message_id: null, attempt_id: null, message: null, delivery: null,
      },
      receipt: null,
    }));
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    mailbox.addMessage({ id: 'msg_after', body: 'deliver after ghost' });
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    await waitFor(() => root.steered.length === 1);
    assert.equal(root.steered[0].source.messageId, 'msg_after');
    await waitFor(() => JSON.parse(fs.readFileSync(paths.recoveryFile, 'utf8')).claim === null);
  } finally {
    await mailbox.close();
  }
});

test('the plugin never ends a generation it did not activate', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.recoveryFile, JSON.stringify({
      schema: 1,
      claim: {
        generation: 'gen-old', request_id: 'req-old', resolved: false,
        message_id: null, attempt_id: null, message: null, delivery: null,
      },
      receipt: null,
    }));
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    // Ending a previous generation belongs to the launcher; this plugin must only
    // activate its own and never deactivate the recorded stale one.
    assert.equal(mailbox.activation.generation, 'gen-test');
    assert.equal(mailbox.deactivationCalls.length, 0);
  } finally {
    await mailbox.close();
  }
});

test('the tool caller guard rejects a non-owning agent', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    const other = makeAgent('session-other', dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => root.tools.length === 6);
    const send = root.tools.find((tool) => tool.name === 'send_message');
    await assert.rejects(() => send.execute(
      { to: 'mbx_x', id: 'c1', task_id: 't1', body: 'hi' }, { agent: other.agent }),
    /mailbox_not_owner/);
    const result = await send.execute(
      { to: 'mbx_' + 'b'.repeat(32), id: 'c1', task_id: 't1', body: 'hi' }, { agent: root.agent });
    assert.equal(result.message.message_id, 'msg_sent');
  } finally {
    await mailbox.close();
  }
});

test('disposal stops injection and deactivates the generation', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mailbox-plugin-'));
  const paths = setup(dir);
  const mailbox = new FakeMailbox(paths.socketPath);
  await mailbox.listen();
  try {
    const root = makeAgent(SESSION, dir);
    fs.writeFileSync(paths.rootFile, JSON.stringify({ schema: 1, sessionId: SESSION, cwd: dir }));
    const plugin = await loadPlugin();
    const ctx = makeCtx(new Map([[SESSION, root.agent]]));
    await plugin.apply(ctx, configFor(paths));
    await waitFor(() => mailbox.activation !== null);
    await ctx.emit('agent/disposed', { agent: root.agent });
    await waitFor(() => mailbox.deactivationCalls.length === 1);
    mailbox.addMessage();
    await ctx.emit('agent/turn-stopping', { agent: root.agent, turn: 1 });
    assert.equal(root.steered.length, 0);
  } finally {
    await mailbox.close();
  }
});
