/** Host-owned public research policy; no model-selected modules, files or commands. */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { isIP } from 'node:net';
import { isDeepStrictEqual } from 'node:util';

export const name = 'research-policy';
export const inject = ['agents', 'tools', 'systemPrompt'];
export const POLICY_VERSION = 1;
export const REQUIRED_TOOLS = Object.freeze(['mcp__duckduckgo__search', 'web_fetch']);
const STATUS_FIELDS = Object.freeze({
  mcp__research_status__status_read: ['service', 'capability', 'scope'],
  mcp__research_status__status_report: ['service', 'capability', 'scope', 'outcome', 'retry_after_seconds', 'reset_at'],
  mcp__research_status__status_claim_probe: ['service', 'capability', 'scope', 'reason'],
  mcp__research_status__status_complete_probe: ['lease_id', 'outcome', 'retry_after_seconds', 'reset_at'],
});
export const ALLOWED_TOOLS = Object.freeze([...REQUIRED_TOOLS, ...Object.keys(STATUS_FIELDS)].sort());

const fields = (args, allowed, required = []) => args !== null && typeof args === 'object' &&
  !Array.isArray(args) && Object.keys(args).every(key => allowed.includes(key)) &&
  required.every(key => Object.hasOwn(args, key));
const sortedSchemas = schemas => [...schemas].sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);

export function publicWebURL(value) {
  if (typeof value !== 'string' || value.length > 2048 || value !== value.trim() || /[\x00-\x20\x7f\\]/.test(value)) return false;
  try {
    const url = new URL(value);
    const host = url.hostname.replace(/\.$/, '');
    // Fetching IP literals or alternate service ports is unnecessary for this
    // public-document interface. Native HTTP also validates DNS and redirects.
    return ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.port &&
      host.includes('.') && !isIP(host.replace(/^\[|\]$/g, '')) &&
      !/(^|\.)(localhost|local|internal|lan|home|test|invalid|onion)$/.test(host);
  } catch {
    return false;
  }
}

export function argumentError(tool, args) {
  if (tool === 'web_fetch') {
    if (!fields(args, ['url'], ['url'])) return 'research_tool_arguments_invalid';
    return publicWebURL(args.url) ? undefined : 'research_url_not_public';
  }
  if (tool === 'mcp__duckduckgo__search') {
    if (!fields(args, ['query', 'max_results', 'region'], ['query']) ||
        typeof args.query !== 'string' || !args.query.trim() || args.query.length > 4000 ||
        /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(args.query) ||
        (args.max_results !== undefined && (!Number.isInteger(args.max_results) || args.max_results < 1 || args.max_results > 20)) ||
        (args.region !== undefined && (typeof args.region !== 'string' || !/^([a-z]{2}-[a-z]{2})?$/.test(args.region)))) {
      return 'research_tool_arguments_invalid';
    }
    return;
  }
  if (!Object.hasOwn(STATUS_FIELDS, tool) || !fields(args, STATUS_FIELDS[tool])) return 'research_tool_arguments_invalid';
  // The fixed status server validates its enums and owns all database paths.
  // This research request cannot invent an authorization to probe early.
  if (tool.endsWith('__status_claim_probe') && args.reason !== undefined && args.reason !== 'due') {
    return 'research_tool_arguments_invalid';
  }
}

export function apply(ctx, config) {
  const hashFields = ['pluginSha256', 'overlaySha256', 'configSha256'];
  if (!config || hashFields.some(key => !/^[a-f0-9]{64}$/.test(config[key] ?? ''))) {
    throw new Error('research_policy_config_invalid');
  }
  const actualHash = createHash('sha256').update(readFileSync(new URL(import.meta.url))).digest('hex');
  if (actualHash !== config.pluginSha256) throw new Error('research_policy_plugin_changed');
  const prepared = new WeakMap();
  const assembled = new WeakSet();
  const emit = event => process.stdout.write(`${JSON.stringify(event)}\n`);
  const assertTools = agent => {
    const state = agent && prepared.get(agent);
    if (!state) throw new Error('research_agent_unprepared');
    if (ctx.tools.modeFor(agent) !== 'native' ||
        !isDeepStrictEqual(sortedSchemas(ctx.tools.schemas(agent)), state.schemas) ||
        [...state.definitions].some(([tool, saved]) => ctx.tools.get(tool, agent) !== saved.definition ||
          saved.definition.execute !== saved.execute)) {
      throw new Error('research_tools_changed');
    }
    return state;
  };
  const restriction = exec => {
    let code;
    try {
      const state = assertTools(exec.agent);
      code = state.definitions.has(exec.name) ? argumentError(exec.name, exec.arguments) : 'research_tools_disabled';
    } catch {
      code = 'research_tools_changed';
    }
    if (code) emit({ type: 'tool_policy_violation', policy: 'research', code });
    return code;
  };
  // The monotonic guard remains effective if a later pre-hook force-allows.
  ctx.on('tools/pre-execute', async (exec, next) => {
    const reason = restriction(exec);
    return reason ? { kind: 'deny', reason } : next();
  }, { global: true, prepend: true });
  ctx.tools.guard(restriction);

  ctx.on('agent/created', ({ agent }) => {
    const definitions = new Map();
    for (const tool of ALLOWED_TOOLS) {
      const definition = ctx.tools.get(tool);
      if (definition) definitions.set(tool, { definition, execute: definition.execute });
    }
    if (REQUIRED_TOOLS.some(tool => !definitions.has(tool))) throw new Error('research_tools_unavailable');
    agent.ctx.tools.presentAs('native');
    agent.ctx.tools.restrict({ allow: [...definitions.keys()] });
    agent.ctx.systemPrompt.suppressRuntimeContext();
    const schemas = sortedSchemas(ctx.tools.schemas(agent));
    if (!isDeepStrictEqual(schemas.map(tool => tool.name), [...definitions.keys()].sort())) {
      throw new Error('research_tools_changed');
    }
    prepared.set(agent, { definitions, schemas });
    assertTools(agent);
  }, { global: true, prepend: true });

  ctx.on('system-prompt/assemble', async (_assembly, context, next) => {
    assertTools(context.scope);
    const result = await next();
    const state = assertTools(context.scope);
    if (!Array.isArray(result.tools) || !isDeepStrictEqual(sortedSchemas(result.tools), state.schemas)) {
      throw new Error('research_tools_changed');
    }
    assembled.add(context.scope);
    return result;
  }, { global: true, prepend: true });

  ctx.on('agent/request', async ({ agent }, next) => {
    assertTools(agent);
    const selection = await next();
    const state = assertTools(agent);
    if (!assembled.has(agent)) throw new Error('research_prompt_unverified');
    if (selection.provider !== 'opencode-go' || selection.model !== 'deepseek-v4.1-flash' ||
        selection.reasoningEffort !== 'high') throw new Error('consultation_model_mismatch');
    emit({ type: 'tool_policy', policy: 'research', version: POLICY_VERSION,
      sessionId: agent.id, schema_count: state.definitions.size, tools: [...state.definitions.keys()].sort(),
      presentation: 'native', guard: 'allow_research_only',
      plugin_sha256: config.pluginSha256, overlay_sha256: config.overlaySha256,
      config_sha256: config.configSha256 });
    return selection;
  }, { global: true, prepend: true });
  ctx.provide('consultationPolicy', Object.freeze({ policy: 'research', version: POLICY_VERSION }));
}
