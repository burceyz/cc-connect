/** Host-owned DSH consultation policy. No model-selectable paths or capabilities. */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

export const name = 'consultation-policy';
export const inject = ['agents', 'tools', 'systemPrompt'];
export const POLICY_VERSION = 1;
const DENIAL = 'consultation_tools_disabled';

export function apply(ctx, config) {
  const fields = ['pluginSha256', 'overlaySha256', 'configSha256'];
  if (!config || fields.some(key => !/^[a-f0-9]{64}$/.test(config[key] ?? ''))) {
    throw new Error('consultation_policy_config_invalid');
  }
  const actualHash = createHash('sha256').update(readFileSync(new URL(import.meta.url))).digest('hex');
  if (actualHash !== config.pluginSha256) throw new Error('consultation_policy_plugin_changed');
  const prepared = new WeakSet();
  const assembled = new WeakSet();
  const emit = event => process.stdout.write(`${JSON.stringify(event)}\n`);
  const deny = () => {
    emit({ type: 'tool_policy_violation', policy: 'none', code: DENIAL });
    return DENIAL;
  };
  // The early hook avoids optional pre-execute side effects. The monotonic
  // guard also denies a later hook that returns allow without calling next().
  ctx.on('tools/pre-execute', async () => ({ kind: 'deny', reason: deny() }),
    { global: true, prepend: true });
  ctx.tools.guard(deny);

  const assertEmpty = agent => {
    if (!agent || !prepared.has(agent)) throw new Error('consultation_agent_unprepared');
    if (ctx.tools.modeFor(agent) !== 'native' || ctx.tools.schemas(agent).length !== 0) {
      throw new Error('consultation_tools_exposed');
    }
  };
  ctx.on('agent/created', ({ agent }) => {
    agent.ctx.tools.presentAs('native');
    agent.ctx.tools.restrict({ allow: [] });
    agent.ctx.systemPrompt.suppressRuntimeContext();
    prepared.add(agent);
    assertEmpty(agent);
  }, { global: true, prepend: true });

  // Prompt providers can contribute schemas without using the tool registry.
  // Inspect the final waterfall value as well as the live registry.
  ctx.on('system-prompt/assemble', async (_assembly, context, next) => {
    assertEmpty(context.scope);
    const result = await next();
    assertEmpty(context.scope);
    if (!Array.isArray(result.tools) || result.tools.length !== 0) {
      throw new Error('consultation_tools_exposed');
    }
    assembled.add(context.scope);
    return result;
  }, { global: true, prepend: true });

  ctx.on('agent/request', async ({ agent }, next) => {
    assertEmpty(agent);
    const selection = await next();
    assertEmpty(agent);
    if (!assembled.has(agent)) throw new Error('consultation_prompt_unverified');
    if (selection.provider !== 'opencode-go' || selection.model !== 'deepseek-v4.1-flash' ||
        selection.reasoningEffort !== 'high') {
      throw new Error('consultation_model_mismatch');
    }
    emit({ type: 'tool_policy', policy: 'none', version: POLICY_VERSION,
      sessionId: agent.id, schema_count: 0, presentation: 'native', guard: 'deny_all',
      plugin_sha256: config.pluginSha256, overlay_sha256: config.overlaySha256,
      config_sha256: config.configSha256 });
    return selection;
  }, { global: true, prepend: true });

  // headless-runner injects this service, so a missing/failed plugin cannot run.
  ctx.provide('consultationPolicy', Object.freeze({ policy: 'none', version: POLICY_VERSION }));
}
