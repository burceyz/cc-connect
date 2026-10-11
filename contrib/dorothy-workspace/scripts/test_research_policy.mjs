/** Offline fixture against the installed DSH registry, factory and prompt path. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as policy from './research-policy.mjs';

const root = process.argv[2];
assert.ok(root, 'pass the installed @deepseek-ai/dsh package root');
const native = name => import(pathToFileURL(join(root, 'node_modules/@deepseek-ai', name, 'lib/index.js')));
const { Context } = await native('cordis');
const { scopeTarget } = await native('dsh-scope');
const modules = await Promise.all(['dsh-system-prompt', 'dsh-tools', 'dsh-agent',
  'dsh-session', 'dsh-session-projection', 'dsh-llm', 'dsh-agent-loop'].map(native));
const config = {
  pluginSha256: createHash('sha256').update(readFileSync(new URL('./research-policy.mjs', import.meta.url))).digest('hex'),
  overlaySha256: 'b'.repeat(64), configSha256: 'c'.repeat(64),
};
const selection = { provider: 'opencode-go', model: 'deepseek-v4.1-flash', reasoningEffort: 'high' };
let toolBodies = 0;
const definition = name => ({ name, description: 'offline test marker',
  parameters: { type: 'object', properties: {} },
  output: { schema: { type: 'object' }, render: () => [{ type: 'text', text: 'executed' }] },
  execute: async () => { toolBodies += 1; return {}; },
});

async function fixture(names, run) {
  const ctx = new Context();
  const fibers = [];
  let handle;
  try {
    for (const [i, module] of modules.entries()) fibers.push(await ctx.plugin(module.default, i === 1 ? { mode: 'both' } : {}));
    for (const name of [...names, 'shell', 'read_file', 'mcp__duckduckgo__fetch_content']) ctx.tools.register(definition(name));
    fibers.push(await ctx.plugin(policy, config));
    assert.equal(ctx.get('consultationPolicy').policy, 'research');
    handle = await ctx.agents.create({ sessionId: 'session-research-fixture',
      meta: { cwd: process.cwd() }, agentOptions: selection });
    await run(ctx, handle.agent);
  } finally {
    if (handle) await handle.dispose();
    for (const fiber of fibers.reverse()) await fiber.dispose();
  }
}

await fixture(policy.ALLOWED_TOOLS, async (ctx, agent) => {
  assert.equal(ctx.tools.modeFor(agent), 'native');
  assert.deepEqual(ctx.tools.schemas(agent).map(tool => tool.name).sort(), policy.ALLOWED_TOOLS);
  const assemble = () => ctx.systemPrompt.assemble({ agent, scope: agent });
  const request = options => ctx.waterfall(scopeTarget(ctx, agent), 'agent/request',
    { agent, turn: 1, step: 1, signal: new AbortController().signal }, async () => options);
  let serial = 0;
  const execute = (name, args = {}) => ctx.tools.execute({ callId: `call-${++serial}`, name, arguments: args,
    agent, signal: new AbortController().signal });
  await assemble();
  assert.deepEqual(await request(selection), selection);
  await assert.rejects(request({ ...selection, reasoningEffort: 'max' }), /consultation_model_mismatch/);
  await assert.rejects(request({ ...selection, provider: 'happy2' }), /consultation_model_mismatch/);
  for (const [name, args] of [
    ['mcp__duckduckgo__search', { query: 'IANA example domains', max_results: 3, region: 'wt-wt' }],
    ['web_fetch', { url: 'https://example.com/' }],
    ['mcp__research_status__status_read', { service: 'duckduckgo', capability: 'search' }],
  ]) assert.notEqual((await execute(name, args)).isError, true);
  const allowedBodies = toolBodies;
  for (const name of ['shell', 'read_file', 'write_file', 'subagent', 'run_code', 'send_message',
    'mcp__duckduckgo__fetch_content', 'mcp__anything__search']) {
    assert.equal((await execute(name)).isError, true);
  }
  for (const url of ['file:///tmp/private', 'https://user:secret@example.com/', 'http://localhost/',
    'http://a.local/', 'http://a.internal/', 'http://127.0.0.1/', 'http://2130706433/',
    'http://0x7f000001/', 'http://10.0.0.1/', 'http://169.254.169.254/', 'http://[::1]/',
    'http://[::ffff:127.0.0.1]/', 'https://example.com:8080/', ' https://example.com/',
    'https://example.com/\nsecret']) {
    assert.equal(policy.publicWebURL(url), false, url);
    assert.equal((await execute('web_fetch', { url })).isError, true, url);
  }
  for (const args of [{ query: '' }, { query: 'topic', max_results: 21 }, { query: 'topic', cwd: '/private' },
    { query: 'topic', region: '../private' }]) assert.equal((await execute('mcp__duckduckgo__search', args)).isError, true);
  assert.equal((await execute('web_fetch', { url: 'https://example.com', path: '/tmp/output' })).isError, true);
  assert.equal((await execute('mcp__research_status__status_read', { path: '/private' })).isError, true);
  assert.equal((await execute('mcp__research_status__status_claim_probe', { reason: 'user_requested' })).isError, true);

  const removeAllow = ctx.on('tools/pre-execute', async () => ({ kind: 'allow' }), { global: true, prepend: true });
  assert.equal((await execute('shell')).isError, true);
  assert.equal((await execute('web_fetch', { url: 'file:///tmp/private' })).isError, true);
  removeAllow();

  // Same permitted name must not let a scope-local implementation replace the host tool.
  const removeShadow = agent.ctx.tools.register(definition('web_fetch'));
  await assert.rejects(assemble(), /research_tools_changed/);
  assert.equal((await execute('web_fetch', { url: 'https://example.com/' })).isError, true);
  removeShadow();
  const removeLocal = agent.ctx.tools.register(definition('local_tool'));
  await assert.rejects(request(selection), /research_tools_changed/);
  assert.equal((await execute('local_tool')).isError, true);
  removeLocal();

  const host = ctx.tools.get('web_fetch');
  const savedExecute = host.execute;
  host.execute = definition('replacement').execute;
  assert.equal((await execute('web_fetch', { url: 'https://example.com/' })).isError, true);
  host.execute = savedExecute;
  const removeProvider = agent.ctx.systemPrompt.tools(() => ({ schemas: [definition('prompt_tool')] }));
  await assert.rejects(assemble(), /research_tools_changed/);
  removeProvider();
  await assemble();
  assert.equal(toolBodies, allowedBodies);
});

// A failed status-board plugin does not prevent research; search and fetch are required.
await fixture(policy.REQUIRED_TOOLS, async (ctx, agent) => {
  assert.deepEqual(ctx.tools.schemas(agent).map(tool => tool.name).sort(), policy.REQUIRED_TOOLS);
  await ctx.systemPrompt.assemble({ agent, scope: agent });
});
await assert.rejects(fixture(['web_fetch'], async () => assert.fail('missing search tool admitted')), /research_tools_unavailable/);
console.log(JSON.stringify({ fixture: 'native_research', allowed_tool_bodies: toolBodies,
  rejected_tool_bodies: 0, network_requests: 0 }));
