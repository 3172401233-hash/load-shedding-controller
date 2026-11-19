import { test } from 'node:test';
import assert from 'node:assert/strict';
import { LoadSheddingController, LEVELS } from '../src/controller.js';

function makeController(overrides = {}) {
  return new LoadSheddingController({
    queueCapacity: 100,
    parallelism: 1,
    capacityQps: 500,
    ...overrides,
  });
}

function shuffled(items, seed) {
  const out = [...items];
  let state = seed >>> 0;
  for (let i = out.length - 1; i > 0; i -= 1) {
    state = (state * 1664525 + 1013904223) >>> 0;
    const j = state % (i + 1);
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

test('overload: critical level sheds infeasible first, then low priority', () => {
  const c = makeController();
  c.observe({ queueDepth: 98, errorRate: 0.3, downstream: 'healthy' });
  assert.equal(c.levelName, 'critical');

  const infeasible = c.decide({
    tenant: 'a', priority: 'high', deadlineMs: 50, tenantQps: 100, queueDepth: 98, estimatedServiceMs: 10,
  });
  assert.equal(infeasible.action, 'reject');
  assert.equal(infeasible.reason, 'deadline-infeasible');

  const high = c.decide({
    tenant: 'a', priority: 'high', deadlineMs: 60_000, tenantQps: 100, queueDepth: 98, estimatedServiceMs: 10,
  });
  assert.equal(high.action, 'admit');
  assert.equal(high.reason, 'high-priority');

  const low = c.decide({
    tenant: 'a', priority: 'low', deadlineMs: 60_000, tenantQps: 100, queueDepth: 98, estimatedServiceMs: 10,
  });
  assert.equal(low.action, 'reject');
  assert.equal(low.reason, 'critical-overload');
  assert.ok(low.retryAfterMs >= 250);
  assert.equal(low.levelName, 'critical');
});

test('overload: near capacity rejects only deadline-infeasible at level 1', () => {
  const c = makeController();
  c.observe({ queueDepth: 80, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-infeasible');

  const doomed = c.decide({
    tenant: 'a', priority: 'high', deadlineMs: 100, queueDepth: 80, estimatedServiceMs: 10,
  });
  assert.equal(doomed.action, 'reject');
  assert.equal(doomed.reason, 'deadline-infeasible');

  const ok = c.decide({
    tenant: 'a', priority: 'low', deadlineMs: 60_000, queueDepth: 80, estimatedServiceMs: 10,
  });
  assert.equal(ok.action, 'admit');
});

test('hysteresis: level holds inside the dead band and steps down gradually', () => {
  const c = makeController();
  c.observe({ queueDepth: 96, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'critical');

  c.observe({ queueDepth: 88, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'critical');

  c.observe({ queueDepth: 84, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-low-priority');

  c.observe({ queueDepth: 72, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-low-priority');

  c.observe({ queueDepth: 68, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-infeasible');

  c.observe({ queueDepth: 60, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-infeasible');

  c.observe({ queueDepth: 40, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'normal');
});

test('tenant monopoly: burst tenant throttled while small tenant keeps fair share', () => {
  const c = makeController();
  c.observe({ queueDepth: 88, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-low-priority');

  const burstTenant = c.decide({
    tenant: 'big', priority: 'low', tenantQps: 400, queueDepth: 88, estimatedServiceMs: 10,
  });
  assert.equal(burstTenant.action, 'delay');
  assert.equal(burstTenant.reason, 'tenant-burst');
  assert.ok(burstTenant.retryAfterMs > 0);

  const bigNormal = c.decide({
    tenant: 'big', priority: 'normal', tenantQps: 60, queueDepth: 88, estimatedServiceMs: 10,
  });
  assert.equal(bigNormal.action, 'delay');
  assert.equal(bigNormal.reason, 'low-priority-throttled');

  const smallTenant = c.decide({
    tenant: 'small', priority: 'low', tenantQps: 5, queueDepth: 88, estimatedServiceMs: 10,
  });
  assert.equal(smallTenant.action, 'admit');
  assert.equal(smallTenant.reason, 'fair-share-protected');
});

test('downstream recovery: flapping does not cause level oscillation', () => {
  const c = makeController({ healthySamplesToRecover: 3 });
  c.observe({ queueDepth: 10, errorRate: 0, downstream: 'down' });
  assert.equal(c.levelName, 'critical');
  assert.equal(c.downstreamState, 'down');

  for (let i = 0; i < 4; i += 1) {
    c.observe({ queueDepth: 10, errorRate: 0, downstream: 'healthy' });
    c.observe({ queueDepth: 10, errorRate: 0, downstream: 'down' });
  }
  assert.equal(c.levelName, 'critical');
  assert.equal(c.downstreamState, 'down');

  c.observe({ queueDepth: 10, errorRate: 0, downstream: 'healthy' });
  c.observe({ queueDepth: 10, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.downstreamState, 'down');
  assert.equal(c.levelName, 'critical');

  c.observe({ queueDepth: 10, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.downstreamState, 'healthy');
  assert.equal(c.levelName, 'shed-low-priority');

  c.observe({ queueDepth: 10, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'shed-infeasible');

  c.observe({ queueDepth: 10, errorRate: 0, downstream: 'healthy' });
  assert.equal(c.levelName, 'normal');
});

test('downstream down rejects everything with Retry-After', () => {
  const c = makeController();
  c.observe({ queueDepth: 5, errorRate: 0, downstream: 'down' });
  const d = c.decide({ tenant: 'a', priority: 'high', queueDepth: 5, estimatedServiceMs: 10 });
  assert.equal(d.action, 'reject');
  assert.equal(d.reason, 'downstream-down');
  assert.ok(d.retryAfterMs > 0);
});

test('edge cases: no deadline, extremely short deadline, duplicate keys', () => {
  const c = makeController();

  const noDeadline = c.decide({ tenant: 'a', priority: 'normal', queueDepth: 1 });
  assert.equal(noDeadline.action, 'admit');

  const tooShort = c.decide({
    tenant: 'a', priority: 'high', deadlineMs: 5, queueDepth: 0, estimatedServiceMs: 10,
  });
  assert.equal(tooShort.action, 'reject');
  assert.equal(tooShort.reason, 'deadline-too-short');

  const expired = c.decide({ tenant: 'a', priority: 'high', deadlineMs: 0, queueDepth: 0 });
  assert.equal(expired.action, 'reject');
  assert.equal(expired.reason, 'deadline-expired');

  c.observe({ queueDepth: 88, errorRate: 0, downstream: 'healthy' });
  const delayed = c.decide({
    key: 'req-1', tenant: 'a', priority: 'normal', tenantQps: 100, queueDepth: 88, estimatedServiceMs: 10,
  });
  assert.equal(delayed.action, 'delay');
  assert.equal(delayed.reason, 'low-priority-throttled');

  const replay = c.decide({
    key: 'req-1', tenant: 'a', priority: 'normal', tenantQps: 100, queueDepth: 88, estimatedServiceMs: 10,
  });
  assert.equal(replay.action, delayed.action);
  assert.equal(replay.reason, delayed.reason);
  assert.equal(replay.duplicate, true);
});

test('determinism: shuffled input order yields identical decisions', () => {
  const metrics = { queueDepth: 88, errorRate: 0.02, downstream: 'healthy' };
  const requests = [];
  const priorities = ['high', 'normal', 'low', 9, 0];
  for (let i = 0; i < 30; i += 1) {
    requests.push({
      key: `k${i}`,
      tenant: `t${i % 4}`,
      priority: priorities[i % priorities.length],
      deadlineMs: i % 3 === 0 ? null : 50 + i * 200,
      tenantQps: (i * 37) % 300,
      queueDepth: 88,
      estimatedServiceMs: 5 + (i % 5) * 4,
    });
  }

  const run = (order) => {
    const c = makeController();
    c.observe(metrics);
    const out = new Map();
    for (const req of order) {
      const d = c.decide(req);
      out.set(req.key, `${d.action}:${d.reason}:${d.levelName}:${d.retryAfterMs}`);
    }
    return out;
  };

  const baseline = run(requests);
  for (const seed of [1, 42, 987654321]) {
    const other = run(shuffled(requests, seed));
    assert.deepEqual(other, baseline);
  }
});

test('levels are ordered and named', () => {
  assert.deepEqual([...LEVELS], ['normal', 'shed-infeasible', 'shed-low-priority', 'critical']);
});
