/** Offline headless boot fixture; also loaded by Node --import before DSH. */
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import { syncBuiltinESMExports } from 'node:module';

let networkAttempts = 0;
const denyNetwork = () => {
  networkAttempts += 1;
  throw new Error('offline_fixture_network_forbidden');
};
globalThis.fetch = denyNetwork;
http.request = http.get = https.request = https.get = denyNetwork;
net.Socket.prototype.connect = denyNetwork;
syncBuiltinESMExports();

export const name = 'consultation-offline-stop';
export const inject = ['consultationPolicy', 'tools'];
export function apply(ctx) {
  ctx.on('agent/request', async ({ agent }, next) => {
    // The real policy must finish its own checks and emit its bound proof.
    const selection = await next();
    process.stdout.write(`${JSON.stringify({ type: 'offline_fixture_stop',
      provider: selection.provider, model: selection.model,
      reasoning_effort: selection.reasoningEffort,
      schema_count: ctx.tools.schemas(agent).length,
      network_attempts: networkAttempts })}\n`);
    throw new Error('offline_fixture_stopped_before_model');
  }, { global: true, prepend: true });
  ctx.provide('consultationFixtureStop', true);
}
