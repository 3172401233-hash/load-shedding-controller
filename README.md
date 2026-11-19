# load-shedding-controller

根据截止时间、队列压力和租户公平性执行分级限流与降载。运行环境：Node.js 24.16.0。

## 重要说明

本控制器是一个**纯决策库**：它只输出 `admit` / `delay` / `reject` 决策、建议的
`Retry-After`、决策原因和当前策略等级。**它不直接修改任何网络设备**（不操作
iptables、TC、交换机、负载均衡器或网卡配置），所有决策由调用方（API 网关、
服务中间件等）在自己的请求路径上执行。

## 使用方式

```js
import { LoadSheddingController } from './src/controller.js';

const controller = new LoadSheddingController({
  queueCapacity: 1000,   // 队列容量
  parallelism: 8,        // 并发处理 worker 数
  capacityQps: 500,      // 服务容量（QPS）
  minTenantShare: 0.05,  // 每个租户的最低公平份额（占容量比例）
});

// 周期性地喂入系统指标，驱动策略等级（含滞回）
controller.observe({ queueDepth: 880, errorRate: 0.02, downstream: 'healthy' });

// 每个请求调用 decide，decide 是纯函数，不修改控制器状态
const decision = controller.decide({
  key: 'req-123',            // 可选，幂等去重键
  tenant: 'tenant-a',
  priority: 'high',          // 'high' | 'normal' | 'low' 或 0-9 数字
  deadlineMs: 300,           // 剩余 deadline；null 表示无 deadline
  tenantQps: 120,            // 该租户当前 QPS
  queueDepth: 880,           // 当前队列深度
  estimatedServiceMs: 20,    // 预计处理时间
});
// => { action, reason, level, levelName, policy, retryAfterMs, duplicate? }
```

## 策略等级

| 等级 | 名称 | 行为 |
| --- | --- | --- |
| 0 | `normal` | 全部放行 |
| 1 | `shed-infeasible` | 队列接近容量，先裁掉剩余 deadline 内注定无法完成的请求 |
| 2 | `shed-low-priority` | 再限制低优先级请求与突发租户（delay + Retry-After） |
| 3 | `critical` | 只放行高优先级与低于公平份额的租户 |

裁减顺序严格为：先裁 deadline 不可行的请求，再裁低优先级请求。

## 关键行为

- **Deadline 感知**：根据 `queueDepth / parallelism * serviceMs` 估算排队等待，
  剩余 deadline 不足以完成排队 + 处理的请求判为 `deadline-infeasible`；
  连空队列都来不及的判为 `deadline-too-short`，已过的判为 `deadline-expired`。
- **无 deadline**：`deadlineMs` 传 `null`，永不判为不可行，但在等级 2 下
  普通优先级的无 deadline 请求会被延后（它们等得起）。
- **租户公平份额**：`tenantQps <= minTenantShare * capacityQps` 的租户在任何
  等级下都被放行（`fair-share-protected`），防止大租户挤死小租户；
  超过 `burstMultiplier` 倍公平份额的租户判为突发并被 delay。
- **滞回（hysteresis）**：升级立即生效，降级需利用率跌破更低的退出水位且
  每次只降一级；下游从 `down`/`degraded` 恢复需连续
  `healthySamplesToRecover` 次健康样本，避免抖动导致反复开关。
- **重复请求键**：相同 `key` 在 `duplicateTtlMs` 内返回首次决策并标记
  `duplicate: true`，保证重试幂等。
- **Retry-After**：按排空超出退出水位的队列所需时间估算，并夹在
  `minRetryAfterMs` 与 `maxRetryAfterMs` 之间。

## 测试

```sh
node --test
```

覆盖：过载分级裁减、临界水位滞回、租户独占与公平份额、下游恢复抖动、
相同输入乱序的决策确定性、无 deadline / 极端短 deadline / 重复请求键。
