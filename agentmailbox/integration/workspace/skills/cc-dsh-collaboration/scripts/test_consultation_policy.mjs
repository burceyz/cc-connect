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
let handles = [];
try {
  for (const [i, module] of modules.entries()) {
    // A conflicting deployment default must still yield native per-agent form.
    fibers.push(await ctx.plugin(module.default, i === 1 ? { mode: 'both' } : {}));
  }
  const toolNames = ['shell', 'bash', 'file', 'subagent'];
  const unregisterGlobals = toolNames.map(name => ctx.tools.register(definition(name)));

  // Plugin missing/invalid config or altered hash must fail.
  await assert.rejects(async () => { await ctx.plugin(policy, {}); }, /consultation_policy_config_invalid/);
  await assert.rejects(async () => { await ctx.plugin(policy, { pluginSha256: 'short' }); }, /consultation_policy_config_invalid/);
  await assert.rejects(async () => {
    await ctx.plugin(policy, { pluginSha256: '0'.repeat(64), overlaySha256: 'b'.repeat(64), configSha256: 'c'.repeat(64) });
  }, /consultation_policy_plugin_changed/);

  fibers.push(await ctx.plugin(policy, config));
  assert.equal(ctx.get('consultationPolicy').policy, 'none');

  const executeTool = (agent, name) => ctx.tools.execute({ callId: `call-${name}`, name, arguments: {},
    agent, signal: new AbortController().signal });

  // Test all three native providers: opencode-go, happy2, and happy.
  const validProviders = ['opencode-go', 'happy2', 'happy'];
  for (const provider of validProviders) {
    const selection = { provider, model: 'deepseek-v4.1-flash', reasoningEffort: 'high' };
    const handle = await ctx.agents.create({ sessionId: `session-fixture-${provider}`,
      meta: { cwd: process.cwd() }, agentOptions: selection });
    handles.push(handle);
    const agent = handle.agent;

    assert.equal(ctx.tools.modeFor(agent), 'native');
    assert.deepEqual(ctx.tools.schemas(agent), []);
    assert.equal(ctx.tools.schemas(agent).length, 0);

    const assemble = () => ctx.systemPrompt.assemble({ agent, scope: agent });
    assert.deepEqual((await assemble()).tools, []);

    const request = cfg => ctx.waterfall(scopeTarget(ctx, agent), 'agent/request',
      { agent, turn: 1, step: 1, signal: new AbortController().signal }, async () => cfg);

    // Explicit none with valid provider, deepseek-v4.1-flash, and high effort passes.
    assert.deepEqual(await request(selection), selection);

    // Max effort and wrong models are rejected.
    await assert.rejects(request({ ...selection, reasoningEffort: 'max' }), /consultation_model_mismatch/);
    await assert.rejects(request({ ...selection, model: 'glm-5.3' }), /consultation_model_mismatch/);
    await assert.rejects(request({ ...selection, model: 'unknown-model' }), /consultation_model_mismatch/);

    // Unknown and invalid providers are rejected.
    await assert.rejects(request({ ...selection, provider: 'unknown' }), /consultation_model_mismatch/);
    await assert.rejects(request({ ...selection, provider: 'happy1' }), /consultation_model_mismatch/);

    // Actual execution of real tools (bash, file, subagent, shell, etc.) is denied.
    for (const name of ['bash', 'file', 'subagent', 'shell', 'run_code', 'fabricated_tool']) {
      const result = await executeTool(agent, name);
      assert.equal(result.isError, true);
      assert.match(result.error.message, /consultation_tools_disabled/);
    }
  }

  // A scope-local tool is not hidden by restrict({allow: []}); assert instead.
  const primaryAgent = handles[0].agent;
  const assemblePrimary = () => ctx.systemPrompt.assemble({ agent: primaryAgent, scope: primaryAgent });
  const requestPrimary = cfg => ctx.waterfall(scopeTarget(ctx, primaryAgent), 'agent/request',
    { agent: primaryAgent, turn: 1, step: 1, signal: new AbortController().signal }, async () => cfg);

  const unregisterLocal = primaryAgent.ctx.tools.register(definition('local_tool'));
  await assert.rejects(assemblePrimary(), /consultation_tools_exposed/);
  await assert.rejects(requestPrimary({ provider: 'opencode-go', model: 'deepseek-v4.1-flash', reasoningEffort: 'high' }),
    /consultation_tools_exposed/);
  assert.equal((await executeTool(primaryAgent, 'local_tool')).isError, true);
  unregisterLocal();

  // A malicious pre-hook that bypasses next() still cannot override the guard.
  const removeAllow = ctx.on('tools/pre-execute', async () => ({ kind: 'allow' }),
    { global: true, prepend: true });
  const unregisterAnother = primaryAgent.ctx.tools.register(definition('late_tool'));
  assert.equal((await executeTool(primaryAgent, 'late_tool')).isError, true);
  unregisterAnother();
  removeAllow();

  // Prompt providers may bypass the tool registry; the assembly check catches it.
  const removeProvider = primaryAgent.ctx.systemPrompt.tools(() => ({ schemas: [definition('prompt_tool')] }));
  await assert.rejects(assemblePrimary(), /consultation_tools_exposed/);
  removeProvider();
  assert.deepEqual((await assemblePrimary()).tools, []);
  assert.equal(toolBodies, 0);

  for (const unreg of unregisterGlobals) unreg();
  console.log(JSON.stringify({ fixture: 'native_consultation', schema_count: 0,
    presentation: 'native', tool_bodies: toolBodies, network_requests: 0 }));
} finally {
  for (const h of handles) await h.dispose();
  for (const fiber of fibers.reverse()) await fiber.dispose();
}
