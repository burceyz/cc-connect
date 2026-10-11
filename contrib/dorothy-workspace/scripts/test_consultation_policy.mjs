/** Offline native DSH fixture: real registry, agent factory and prompt assembly. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as policy from './consultation-policy.mjs';

const root = process.argv[2];
assert.ok(root, 'pass the installed @deepseek-ai/dsh package root');
const native = name => import(pathToFileURL(join(root, 'node_modules/@deepseek-ai', name, 'lib/index.js')));
const { Context } = await native('cordis');
const { scopeTarget } = await native('dsh-scope');
const modules = await Promise.all(['dsh-system-prompt', 'dsh-tools', 'dsh-agent',
  'dsh-session', 'dsh-session-projection', 'dsh-llm', 'dsh-agent-loop'].map(native));
const ctx = new Context();
const fibers = [];
const config = {
  pluginSha256: createHash('sha256').update(readFileSync(new URL('./consultation-policy.mjs', import.meta.url))).digest('hex'),
  overlaySha256: 'b'.repeat(64), configSha256: 'c'.repeat(64),
};
let toolBodies = 0;
const definition = name => ({ name, description: 'offline test marker',
  parameters: { type: 'object', properties: {} },
  output: { schema: { type: 'object' }, render: () => [{ type: 'text', text: 'executed' }] },
  execute: async () => { toolBodies += 1; return {}; },
});
let handle;
try {
  for (const [i, module] of modules.entries()) {
    // A conflicting deployment default must still yield native per-agent form.
    fibers.push(await ctx.plugin(module.default, i === 1 ? { mode: 'both' } : {}));
  }
  const unregisterGlobal = ctx.tools.register(definition('shell'));
  fibers.push(await ctx.plugin(policy, config));
  assert.equal(ctx.get('consultationPolicy').policy, 'none');
  const selection = { provider: 'opencode-go', model: 'deepseek-v4.1-flash', reasoningEffort: 'high' };
  handle = await ctx.agents.create({ sessionId: 'session-consultation-fixture',
    meta: { cwd: process.cwd() }, agentOptions: selection });
  const agent = handle.agent;
  assert.equal(ctx.tools.modeFor(agent), 'native');
  assert.deepEqual(ctx.tools.schemas(agent), []);
  const assemble = () => ctx.systemPrompt.assemble({ agent, scope: agent });
  assert.deepEqual((await assemble()).tools, []);
  const request = config => ctx.waterfall(scopeTarget(ctx, agent), 'agent/request',
    { agent, turn: 1, step: 1, signal: new AbortController().signal }, async () => config);
  assert.deepEqual(await request(selection), selection);
  await assert.rejects(request({ ...selection, reasoningEffort: 'max' }), /consultation_model_mismatch/);

  const execute = name => ctx.tools.execute({ callId: `call-${name}`, name, arguments: {},
    agent, signal: new AbortController().signal });
  for (const name of ['shell', 'run_code', 'fabricated_tool']) {
    const result = await execute(name);
    assert.equal(result.isError, true);
    assert.match(result.error.message, /consultation_tools_disabled/);
  }

  // A scope-local tool is not hidden by restrict({allow: []}); assert instead.
  const unregisterLocal = agent.ctx.tools.register(definition('local_tool'));
  await assert.rejects(assemble(), /consultation_tools_exposed/);
  await assert.rejects(request(selection), /consultation_tools_exposed/);
  assert.equal((await execute('local_tool')).isError, true);
  unregisterLocal();

  // A malicious pre-hook that bypasses next() still cannot override the guard.
  const removeAllow = ctx.on('tools/pre-execute', async () => ({ kind: 'allow' }),
    { global: true, prepend: true });
  const unregisterAnother = agent.ctx.tools.register(definition('late_tool'));
  assert.equal((await execute('late_tool')).isError, true);
  unregisterAnother();
  removeAllow();

  // Prompt providers may bypass the tool registry; the assembly check catches it.
  const removeProvider = agent.ctx.systemPrompt.tools(() => ({ schemas: [definition('prompt_tool')] }));
  await assert.rejects(assemble(), /consultation_tools_exposed/);
  removeProvider();
  assert.deepEqual((await assemble()).tools, []);
  assert.equal(toolBodies, 0);
  unregisterGlobal();
  console.log(JSON.stringify({ fixture: 'native_consultation', schema_count: 0,
    presentation: 'native', tool_bodies: toolBodies, network_requests: 0 }));
} finally {
  if (handle) await handle.dispose();
  for (const fiber of fibers.reverse()) await fiber.dispose();
}
