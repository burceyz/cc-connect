/** Search discovery fixture for offline native headless startup; no network. */
export const name = 'research-fixture-tools';
export const inject = ['tools'];
export function apply(ctx) {
  ctx.tools.register({ name: 'mcp__duckduckgo__search', description: 'Offline discovery fixture',
    parameters: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] },
    output: { schema: { type: 'object' }, render: () => [] },
    execute: async () => { throw new Error('offline_search_must_not_execute'); },
  });
}
