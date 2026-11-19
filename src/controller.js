export const ACTIONS = Object.freeze({
  ADMIT: 'admit',
  DELAY: 'delay',
  REJECT: 'reject',
});

export const LEVELS = Object.freeze([
  'normal',
  'shed-infeasible',
  'shed-low-priority',
  'critical',
]);

const DEFAULTS = {
  queueCapacity: 1000,
  parallelism: 8,
  capacityQps: 500,
  defaultServiceMs: 20,
  minTenantShare: 0.05,
  burstMultiplier: 4,
  enter: [0.75, 0.85, 0.95],
  exit: [0.5, 0.7, 0.85],
  errorRateDegraded: 0.05,
  errorRateCritical: 0.25,
  healthySamplesToRecover: 3,
  duplicateTtlMs: 30_000,
  minRetryAfterMs: 250,
  maxRetryAfterMs: 30_000,
  priorityBands: { high: 7, normal: 4 },
};

const PRIORITY_NAMES = new Set(['high', 'normal', 'low']);

function clamp(value, lo, hi) {
  return Math.min(hi, Math.max(lo, value));
}

export class LoadSheddingController {
  constructor(options = {}) {
    this.cfg = { ...DEFAULTS, ...options };
    this._level = 0;
    this._downstream = 'healthy';
    this._healthyStreak = 0;
    this._lastMetrics = { queueDepth: 0, errorRate: 0, downstream: 'healthy' };
    this._dedup = new Map();
  }

  get level() {
    return this._level;
  }

  get levelName() {
    return LEVELS[this._level];
  }

  get downstreamState() {
    return this._downstream;
  }

  observe(metrics = {}) {
    const queueDepth = Math.max(0, metrics.queueDepth ?? this._lastMetrics.queueDepth);
    const errorRate = clamp(metrics.errorRate ?? this._lastMetrics.errorRate, 0, 1);
    const sample = metrics.downstream ?? this._lastMetrics.downstream ?? 'healthy';

    if (sample === 'healthy') {
      this._healthyStreak += 1;
    } else {
      this._healthyStreak = 0;
    }
    const downstream = this._effectiveDownstream(sample);
    this._lastMetrics = { queueDepth, errorRate, downstream: sample };

    const utilization = this.cfg.queueCapacity > 0 ? queueDepth / this.cfg.queueCapacity : 1;
    const target = this._targetLevel(utilization, errorRate, downstream);

    if (target > this._level) {
      this._level = target;
    } else if (target < this._level && this._canStepDown(utilization, errorRate, downstream)) {
      this._level -= 1;
    }
    return this._level;
  }

  decide(request = {}) {
    const now = request.now ?? Date.now();
    if (request.key != null) {
      const hit = this._dedupGet(request.key, now);
      if (hit) {
        return { ...hit, duplicate: true };
      }
    }
    const decision = this._evaluate(request);
    if (request.key != null) {
      this._dedupSet(request.key, decision, now);
    }
    return decision;
  }

  decideAll(requests) {
    return requests.map((request) => this.decide(request));
  }

  _effectiveDownstream(sample) {
    if (sample !== 'healthy') {
      this._downstream = sample;
      return sample;
    }
    if (this._downstream !== 'healthy' && this._healthyStreak < this.cfg.healthySamplesToRecover) {
      return 'degraded';
    }
    this._downstream = 'healthy';
    return 'healthy';
  }

  _targetLevel(utilization, errorRate, downstream) {
    let target = 0;
    if (utilization >= this.cfg.enter[0] || errorRate >= this.cfg.errorRateDegraded || downstream === 'degraded') {
      target = 1;
    }
    if (utilization >= this.cfg.enter[1]) {
      target = 2;
    }
    if (utilization >= this.cfg.enter[2] || errorRate >= this.cfg.errorRateCritical || downstream === 'down') {
      target = 3;
    }
    return target;
  }

  _canStepDown(utilization, errorRate, downstream) {
    return (
      utilization <= this.cfg.exit[this._level - 1] &&
      errorRate < this.cfg.errorRateDegraded &&
      downstream === 'healthy'
    );
  }

  _evaluate(request) {
    const level = this._level;
    const base = { level, levelName: LEVELS[level], policy: LEVELS[level] };
    const priority = this._normalizePriority(request.priority);
    const queueDepth = Math.max(0, request.queueDepth ?? this._lastMetrics.queueDepth);
    const serviceMs = Math.max(0, request.estimatedServiceMs ?? this.cfg.defaultServiceMs);
    const deadline = request.deadlineMs ?? null;
    const tenantQps = Math.max(0, request.tenantQps ?? 0);
    const retryAfterMs = this._retryAfterMs(queueDepth, serviceMs);

    if (this._downstream === 'down') {
      return { ...base, action: ACTIONS.REJECT, reason: 'downstream-down', retryAfterMs };
    }
    if (deadline != null && deadline <= 0) {
      return { ...base, action: ACTIONS.REJECT, reason: 'deadline-expired', retryAfterMs: 0 };
    }
    if (deadline != null && serviceMs >= deadline) {
      return { ...base, action: ACTIONS.REJECT, reason: 'deadline-too-short', retryAfterMs: 0 };
    }

    const waitMs = (queueDepth / Math.max(1, this.cfg.parallelism)) * serviceMs;
    const feasible = deadline == null || waitMs + serviceMs <= deadline;
    const fairShareQps = this.cfg.minTenantShare * this.cfg.capacityQps;
    const belowFairShare = tenantQps <= fairShareQps;
    const burst = tenantQps > fairShareQps * this.cfg.burstMultiplier;

    if (level === 0) {
      return { ...base, action: ACTIONS.ADMIT, reason: 'within-capacity', retryAfterMs: 0 };
    }
    if (!feasible) {
      return { ...base, action: ACTIONS.REJECT, reason: 'deadline-infeasible', retryAfterMs };
    }
    if (level === 1) {
      return { ...base, action: ACTIONS.ADMIT, reason: 'within-capacity', retryAfterMs: 0 };
    }
    if (belowFairShare) {
      return { ...base, action: ACTIONS.ADMIT, reason: 'fair-share-protected', retryAfterMs: 0 };
    }
    if (level === 2) {
      if (burst) {
        return { ...base, action: ACTIONS.DELAY, reason: 'tenant-burst', retryAfterMs };
      }
      if (priority === 'low' || (priority === 'normal' && deadline == null)) {
        return { ...base, action: ACTIONS.DELAY, reason: 'low-priority-throttled', retryAfterMs };
      }
      return { ...base, action: ACTIONS.ADMIT, reason: 'within-capacity', retryAfterMs: 0 };
    }
    if (priority === 'high') {
      return { ...base, action: ACTIONS.ADMIT, reason: 'high-priority', retryAfterMs: 0 };
    }
    if (priority === 'normal') {
      return { ...base, action: ACTIONS.DELAY, reason: 'critical-load', retryAfterMs };
    }
    return { ...base, action: ACTIONS.REJECT, reason: 'critical-overload', retryAfterMs };
  }

  _normalizePriority(priority) {
    if (typeof priority === 'string' && PRIORITY_NAMES.has(priority)) {
      return priority;
    }
    const value = Number(priority);
    if (!Number.isFinite(value)) {
      return 'normal';
    }
    if (value >= this.cfg.priorityBands.high) {
      return 'high';
    }
    if (value >= this.cfg.priorityBands.normal) {
      return 'normal';
    }
    return 'low';
  }

  _retryAfterMs(queueDepth, serviceMs) {
    const targetDepth = this.cfg.exit[0] * this.cfg.queueCapacity;
    const excess = Math.max(0, queueDepth - targetDepth);
    const drainMs = (excess / Math.max(1, this.cfg.parallelism)) * Math.max(1, serviceMs);
    return clamp(Math.ceil(drainMs), this.cfg.minRetryAfterMs, this.cfg.maxRetryAfterMs);
  }

  _dedupGet(key, now) {
    const entry = this._dedup.get(key);
    if (!entry) {
      return null;
    }
    if (entry.expiresAt <= now) {
      this._dedup.delete(key);
      return null;
    }
    return entry.decision;
  }

  _dedupSet(key, decision, now) {
    if (this._dedup.size > 10_000) {
      for (const [k, entry] of this._dedup) {
        if (entry.expiresAt <= now) {
          this._dedup.delete(k);
        }
      }
    }
    this._dedup.set(key, { decision, expiresAt: now + this.cfg.duplicateTtlMs });
  }
}
