import test from 'node:test';
import assert from 'node:assert/strict';
import { startTransport } from '../src/transport.mjs';
import { createRegistry, resolveView, RegistryBook } from '../src/topology.mjs';
import { TransportPool } from '../src/pool.mjs';

const DATA = {
  sites: [
    { id: 'supplier-a', limit: 1 },
    { id: 'supplier-b', limit: 2 },
    { id: 'supplier-c', limit: 1 }
  ],
  channels: [
    { id: 'c1', capacity: 2, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' },
    { id: 'c2', capacity: 1, aliases: ['supplier-c'], proof: 'local-registration-2' }
  ]
};

const tick = (ms = 60) => new Promise(resolve => setTimeout(resolve, ms));

async function fixture({ transport = {}, pool: poolOptions = {}, data = DATA, mutate } = {}) {
  const wire = await startTransport(2, transport);
  const registry = createRegistry(structuredClone(data));
  for (const channel of registry.registry.values()) channel.sessionUrl = wire.url;
  mutate?.(registry);
  const pool = new TransportPool({
    origin: wire.url,
    sites: [...registry.siteLimits].map(([id, limit]) => ({ id, limit })),
    resolve: site => resolveView(registry, site),
    ...poolOptions
  });
  let closed = false;
  return {
    wire,
    registry,
    pool,
    async close() {
      if (closed) return;
      closed = true;
      await pool.close();
      await wire.close();
    }
  };
}

function drain(lease) {
  lease.stream.resume();
  lease.stream.on('data', () => {});
  return lease;
}

const consumeLease = drain;

test('两域共享一通道但各守逻辑额度，物理在途以真实流计数', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 20 } });
  try {
    const a1 = await f.pool.acquire('supplier-a', { path: '/large' });
    await a1.response();
    a1.pause();
    assert.equal(f.pool.siteOccupancy('supplier-a').held, 1);

    const b1 = await f.pool.acquire('supplier-b', { path: '/data' });
    drain(b1);
    // a 的额度已满，b 仍可拿到共享通道剩下的窗口。
    assert.equal(f.pool.siteOccupancy('supplier-a').held, 1);
    assert.equal(f.pool.siteOccupancy('supplier-b').held, 1);
    const channel = f.pool.snapshot().channels.find(value => value.id === 'c1');
    assert.equal(channel.wire, 2);
    assert.equal(channel.aliases.length, 2);
    assert.ok(f.pool.snapshot().invariant.ok);
  } finally {
    await f.close();
  }
});

test('同地址未证明可共享时拒绝推断，作业明确失败而非排队', async () => {
  const data = {
    sites: [{ id: 'supplier-a', limit: 1 }, { id: 'supplier-b', limit: 2 }],
    channels: []
  };
  const f = await fixture({ data });
  try {
    // 两个域名连到同一地址，但登记里没有 shared 证明。
    const wire = f.wire;
    const pool = new TransportPool({
      origin: wire.url,
      sites: data.sites,
      resolve: site => ({ site, channels: [{ id: 'shared', url: wire.url, proof: null, sharedWith: ['other'] }] })
    });
    f.pool = pool;
    await assert.rejects(pool.acquire('supplier-a'), /共享但缺少证明/);
  } finally {
    await f.close();
  }
});

test('显式独占登记仍可使用，不必伪造共享证明', async () => {
  const f = await fixture({
    data: {
      sites: [{ id: 'supplier-a', limit: 1 }],
      channels: [{ id: 'c1', capacity: 1, aliases: ['supplier-a'], proof: 'own-registration' }]
    }
  });
  try {
    const lease = await f.pool.acquire('supplier-a');
    drain(lease);
    assert.equal(lease.channelId, 'c1');
  } finally {
    await f.close();
  }
});

test('通道容量收缩后不再超发，排队原因随资源变化更新', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 20 } });
  try {
    const a = await f.pool.acquire('supplier-a', { path: '/hold' });
    const b = await f.pool.acquire('supplier-b', { path: '/hold' });
    drain(a); drain(b);
    assert.equal(f.pool.snapshot().channels.find(channel => channel.id === 'c1').wire, 2);
    const queuedB = f.pool.acquire('supplier-b', { priority: -1 });
    await tick(30);
    assert.match(f.pool.snapshot().queue.find(job => job.site === 'supplier-b').reason, /通道容量已满/);

    f.wire.resize(1);
    await tick(60);
    const channel = f.pool.snapshot().channels.find(value => value.id === 'c1');
    assert.equal(channel.remoteCapacity, 1);
    assert.equal(channel.freeWindow, 0);
    assert.equal(f.pool.snapshot().pressure.overCapacityGrants, 0);

    // 收缩不会新开流：在途仍是 2，排队作业继续保持排队。
    assert.equal(f.pool.snapshot().totals.wire, 2);
    assert.equal(f.pool.snapshot().queue.length, 1);

    // 一条流结束后窗口仍为 0（另一条仍在），排队作业绝不越过容量。
    await a.cancel('让出窗口');
    await tick(30);
    assert.equal(f.pool.snapshot().channels.find(value => value.id === 'c1').freeWindow, 0);
    assert.equal(f.pool.snapshot().totals.queued, 1);

    // 两条都结束后，排队作业获得唯一窗口，且没有超发记录。
    await b.cancel('让出窗口');
    const lease = await queuedB;
    drain(lease);
    assert.equal(f.pool.snapshot().pressure.overCapacityGrants, 0);
    assert.ok(f.pool.snapshot().invariant.ok);
  } finally {
    await f.close();
  }
});

test('取消等待真实释放，确认前不记完成也不放出窗口', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 120 }, pool: { releaseGraceMs: 120 } });
  try {
    const lease = await f.pool.acquire('supplier-a', { path: '/hold' });
    const state = await lease.cancel('媒体处理取消');
    assert.equal(state.totals.completed, 0);
    assert.equal(state.totals.cancelled + state.totals.releasing, 1);
    assert.equal(state.totals.wire, 0);
    assert.equal(f.pool.siteOccupancy('supplier-a').held, 1, '供方确认前仍占用站点额度');
    assert.equal(state.totals.awaitingRelease, 1);
    assert.equal(state.totals.releasing, 1);
    assert.ok(state.invariant.ok);

    const blocked = f.pool.acquire('supplier-a', { priority: -1 });
    await tick(30);
    assert.equal(f.pool.snapshot().totals.queued, 1);
    assert.match(f.pool.snapshot().queue[0].reason, /额度 1 已满/);

    await lease.settled();
    const next = await blocked;
    drain(next);
    const settled = f.pool.snapshot();
    assert.equal(settled.totals.cancelled, 1);
    assert.equal(settled.totals.completed, 0);
    assert.equal(settled.totals.releasing, 0);
    assert.ok(settled.invariant.ok);
  } finally {
    await f.close();
  }
});

test('大响应暂停读取时其他站点仍有进展', async () => {
  const f = await fixture({
    transport: { releaseDelayMs: 20, largeBytes: 4 * 1024 * 1024 },
    pool: { pauseGraceMs: 0 }
  });
  try {
    const large = await f.pool.acquire('supplier-a', { path: '/large', headers: { 'x-size': String(3 * 1024 * 1024) } });
    await large.response();
    large.pause();
    await tick(80);

    const other = await f.pool.acquire('supplier-c', { path: '/data' });
    drain(other);
    const headers = await other.response();
    assert.equal(headers.status, 200);

    const snapshot = f.pool.snapshot();
    assert.equal(snapshot.totals.paused, 1);
    assert.equal(snapshot.channels.find(channel => channel.id === 'c2').wire, 1);
    assert.ok(snapshot.invariant.ok);

    await large.cancel('演示结束大响应');
    assert.equal(f.pool.snapshot().totals.completed, 0);
  } finally {
    await f.close();
  }
});

test('别名迁移后排队作业重新选择通道', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 20 } });
  try {
    const holdA = await f.pool.acquire('supplier-a', { path: '/hold' });
    const holdB = await f.pool.acquire('supplier-b', { path: '/hold' });
    drain(holdA); drain(holdB);
    const queued = f.pool.acquire('supplier-b', { priority: -5 });

    const book = new RegistryBook(structuredClone(DATA));
    for (const channel of book.registry.values()) channel.sessionUrl = f.wire.url;
    // 用显式登记迁移：supplier-b 从 c1 移到 c2，并带共享证明。
    const view = book.migrate('supplier-b', {
      from: 'c1', to: 'c2', proof: 'local-registration-2', reason: 'test-migration'
    });
    assert.deepEqual(view.channels.map(channel => channel.id), ['c2']);

    f.pool.setBinding('supplier-b', view.channels.map(channel => channel.id), {
      proof: view.proof, reason: 'test-migration'
    });
    // c2 对 supplier-c 额度仍为 1，但 supplier-b 可以使用它剩余的窗口。
    const lease = await queued;
    assert.equal(lease.channelId, 'c2');
    drain(lease);
    assert.ok(f.pool.snapshot().invariant.ok);
  } finally {
    await f.close();
  }
});

test('服务端停止接收新流后排队作业不沿用旧判断也不超发', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 20 } });
  try {
    const first = await f.pool.acquire('supplier-a', { path: '/hold' });
    const second = await f.pool.acquire('supplier-b', { path: '/hold' });
    drain(first); drain(second);
    // 通道窗口已满，这条作业先因物理容量排队。
    const waiting = f.pool.acquire('supplier-b', { path: '/data', priority: -1 });
    await tick(30);
    assert.match(f.pool.snapshot().queue[0].reason, /通道容量已满/);

    f.wire.drain();
    await tick(80);
    const snapshot = f.pool.snapshot();
    assert.ok(snapshot.channels.every(channel => channel.state === 'draining'), '所有会话都已进入 drain');
    assert.match(snapshot.queue[0].reason, /通道已停止接收新流/);
    assert.equal(snapshot.pressure.overCapacityGrants, 0);

    // 在途流结束后会话关闭：作业不能借旧判断获得流，必须明确失败而不是记成完成。
    await first.cancel('结束在途');
    await second.cancel('结束在途');
    await tick(80);
    const settled = f.pool.snapshot();
    assert.equal(settled.totals.wire, 0);
    assert.equal(settled.totals.completed, 0);
    assert.equal(settled.pressure.overCapacityGrants, 0);
    assert.ok(settled.invariant.ok);
    await assert.rejects(waiting, /不可用|关闭|停止接收/);
    assert.equal(f.pool.snapshot().totals.queued, 0);
  } finally {
    await f.close();
  }
});

test('拒绝明确未知站点与非法额度', async () => {
  const f = await fixture();
  try {
    await assert.rejects(f.pool.acquire('not-a-site'), /未知站点/);
    assert.throws(() => new TransportPool({ origin: f.wire.url, sites: [{ id: 'x', limit: 0 }], resolve: () => ({}) }), /正整数/);
    assert.throws(() => new TransportPool({ origin: '', sites: {}, resolve: () => ({}) }), /本地通道地址/);
  } finally {
    await f.close();
  }
});

test('排队时取消计入取消而非完成，且不会获得授权', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 20 } });
  try {
    const holdA = await f.pool.acquire('supplier-a', { path: '/hold' });
    const holdB = await f.pool.acquire('supplier-b', { path: '/hold' });
    drain(holdA); drain(holdB);
    const controller = new AbortController();
    const pending = f.pool.acquire('supplier-b', { signal: controller.signal, priority: -1 });
    await tick(20);
    assert.equal(f.pool.snapshot().totals.queued, 1);
    controller.abort();
    await assert.rejects(pending, /取消/);
    await tick(20);
    const snapshot = f.pool.snapshot();
    assert.equal(snapshot.totals.cancelled, 1);
    assert.equal(snapshot.totals.completed, 0);
    assert.equal(snapshot.totals.failed, 0);
    assert.equal(snapshot.queue.length, 0);
    assert.equal(f.wire.stats.accepted, 2, '排队取消不应在通道上产生流');
  } finally {
    await f.close();
  }
});

test('暂停读取超时后主动取消并等待释放，不记成完成', async () => {
  const f = await fixture({
    transport: { releaseDelayMs: 30, largeBytes: 4 * 1024 * 1024 },
    pool: { pausedTimeoutMs: 80, releaseGraceMs: 30 }
  });
  try {
    const large = await f.pool.acquire('supplier-a', { path: '/large', headers: { 'x-size': String(3 * 1024 * 1024) } });
    await large.response();
    large.pause();
    await tick(60);
    assert.equal(f.pool.snapshot().totals.paused, 1);
    await tick(160);
    const during = f.pool.snapshot();
    assert.equal(during.totals.paused, 0);
    assert.equal(during.totals.completed, 0);
    assert.ok(during.totals.cancelled + during.totals.releasing === 1);
    await large.settled();
    const after = f.pool.snapshot();
    assert.equal(after.totals.cancelled, 1);
    assert.equal(after.totals.completed, 0);
    assert.ok(after.invariant.ok);
    assert.ok(f.pool.events.some(event => event.event === 'paused-timeout'));
  } finally {
    await f.close();
  }
});

test('多站点并发下逻辑额度始终成立，通道不超发', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 10 } });
  try {
    const violations = [];
    f.pool.subscribe((event, pool) => {
      const snapshot = pool.snapshot({ jobs: false });
      for (const site of snapshot.sites) {
        if (site.held > site.limit) violations.push({ event: event.event, site: site.id, held: site.held, limit: site.limit });
      }
      if (snapshot.pressure.overCapacityGrants > 0) violations.push({ event: event.event, over: true });
    });
    const jobs = [];
    for (let index = 0; index < 4; index += 1) {
      // 边授权边消费，否则未读的流会一直占着站点额度，测试自身会死锁。
      jobs.push(f.pool.acquire('supplier-a').then(lease => { drain(lease); return lease; }));
      jobs.push(f.pool.acquire('supplier-b').then(lease => { drain(lease); return lease; }));
      jobs.push(f.pool.acquire('supplier-c').then(lease => { drain(lease); return lease; }));
    }
    const leases = await Promise.all(jobs);
    await tick(80);
    assert.deepEqual(violations, []);
    const snapshot = f.pool.snapshot();
    assert.ok(snapshot.invariant.ok, snapshot.invariant.problems.join('; '));
    assert.equal(snapshot.pressure.overCapacityGrants, 0);
    assert.equal(snapshot.totals.wire, [...snapshot.channels].reduce((sum, channel) => sum + channel.wire, 0));
  } finally {
    await f.close();
  }
});

test('迁移移除旧通道后，排队作业不会在旧通道获得授权', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 20 } });
  try {
    const holdA = await f.pool.acquire('supplier-a', { path: '/hold' });
    const holdB = await f.pool.acquire('supplier-b', { path: '/hold' });
    drain(holdA); drain(holdB);
    const waiting = f.pool.acquire('supplier-b', { priority: -1 });
    await tick(20);
    f.pool.setBinding('supplier-b', ['c2'], { proof: 'local-registration-2', reason: 'test' });
    // 旧通道 c1 仍有 supplier-a，让出窗口后 supplier-b 也不能回到 c1。
    await holdA.cancel('让出窗口');
    const lease = await waiting;
    assert.equal(lease.channelId, 'c2');
    drain(lease);
    assert.ok(!f.pool.snapshot().channels.find(channel => channel.id === 'c1').aliases.includes('supplier-b'));
    assert.equal(f.pool.snapshot().pressure.overCapacityGrants, 0);
  } finally {
    await f.close();
  }
});

test('证明不一致的绑定被拒绝而不是复用通道', async () => {
  const f = await fixture();
  try {
    // 先让两个站点各自建立登记与连接信息。
    drain(await f.pool.acquire('supplier-c'));
    drain(await f.pool.acquire('supplier-b'));
    assert.throws(() => f.pool.setBinding('supplier-b', ['c2'], { proof: 'different', reason: 'test' }), /证明不一致/);
    // 冲突时不留下半迁移状态：supplier-b 仍只在原通道上。
    assert.deepEqual(f.pool.snapshot().sites.find(site => site.id === 'supplier-b').channel, ['c1']);
    assert.deepEqual(f.pool.snapshot().channels.find(channel => channel.id === 'c2').aliases, ['supplier-c']);
    assert.ok(f.pool.snapshot().invariant.ok);
  } finally {
    await f.close();
  }
});

test('共享通道上为等待中的站点保留最后窗口，避免一批大文件长期占满', async () => {
  const f = await fixture({
    data: {
      sites: [{ id: 'supplier-a', limit: 2 }, { id: 'supplier-b', limit: 2 }],
      channels: [{ id: 'c1', capacity: 2, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' }]
    },
    transport: { releaseDelayMs: 10 },
    pool: { releaseGraceMs: 10 }
  });
  try {
    const a1 = await f.pool.acquire('supplier-a', { path: '/hold' });
    const a2 = await f.pool.acquire('supplier-a', { path: '/hold' });
    drain(a1); drain(a2);
    // 两个窗口都在 supplier-a 手里；b 与 a 的第三个急件都在排队。
    const b1 = f.pool.acquire('supplier-b', { path: '/data' });
    const a3 = f.pool.acquire('supplier-a', { path: '/data', priority: -10 });
    b1.then(drain);
    a3.then(drain);
    await tick(20);
    assert.equal(f.pool.snapshot().queue.length, 2);

    // a 释放一个窗口后，即使 a 的作业优先级更高，也要把窗口留给等待中的 b。
    await a1.cancel('释放窗口');
    const bLease = await b1;
    assert.equal(bLease.site, 'supplier-b');
    assert.equal(bLease.channelId, 'c1');
    const snapshot = f.pool.snapshot();
    assert.ok(!snapshot.jobs.find(job => job.site === 'supplier-a' && job.id === a3.id && job.state === 'running'),
      'a 的排队作业不能抢走为 b 保留的窗口');
    assert.ok(snapshot.invariant.ok);
    assert.equal(snapshot.pressure.overCapacityGrants, 0);

    await a2.cancel('释放窗口');
    const aLease = await a3;
    assert.equal(aLease.site, 'supplier-a');
    await tick(30);
    assert.ok(f.pool.snapshot().invariant.ok);
  } finally {
    await f.close();
  }
});

test('连接失败或不存在的通道会明确失败，不会无限排队', async () => {
  const f = await fixture();
  try {
    const broken = new TransportPool({
      origin: f.wire.url,
      sites: [{ id: 'supplier-a', limit: 1 }],
      resolve: () => ({ channels: [{ id: 'ghost', url: 'http://127.0.0.1:1', capacity: 1, proof: 'p' }] })
    });
    try {
      await assert.rejects(broken.acquire('supplier-a'), /不可用/);
      const snapshot = broken.snapshot();
      assert.equal(snapshot.totals.completed, 0);
      assert.ok(['failed', 'closed'].includes(snapshot.channels[0].state));
      assert.equal(snapshot.totals.queued, 0);
      assert.ok(snapshot.invariant.ok);
    } finally {
      await broken.close();
    }
  } finally {
    await f.close();
  }
});

test('关闭资源池会把在途与排队作业结算为取消，不冒充完成', async () => {
  const f = await fixture({ transport: { releaseDelayMs: 500 }, pool: { releaseGraceMs: 500 } });
  try {
    const running = consumeLease(await f.pool.acquire('supplier-a', { path: '/hold' }));
    const queued = f.pool.acquire('supplier-a', { priority: -1 });
    queued.catch(() => {});
    const started = Date.now();
    await f.pool.close();
    assert.ok(Date.now() - started < 400, '关闭不应等待完整的释放宽限');
    const snapshot = f.pool.snapshot();
    assert.equal(snapshot.totals.completed, 0);
    assert.equal(snapshot.totals.wire, 0);
    assert.ok(snapshot.totals.cancelled >= 2);
    await assert.rejects(queued, /关闭/);
    assert.ok(snapshot.invariant.ok);
  } finally {
    await f.close();
  }
});

test('共享通道轮转：刚服务过的站点让给其他等待站点', async () => {
  const f = await fixture({
    data: {
      sites: [{ id: 'supplier-a', limit: 2 }, { id: 'supplier-b', limit: 2 }],
      channels: [{ id: 'c1', capacity: 1, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' }]
    },
    transport: { releaseDelayMs: 10 },
    pool: { releaseGraceMs: 10 }
  });
  try {
    const a1 = drain(await f.pool.acquire('supplier-a', { path: '/hold' }));
    const b1 = f.pool.acquire('supplier-b');
    const a2 = f.pool.acquire('supplier-a', { priority: -10 });
    b1.then(drain); a2.then(drain);
    await tick(20);
    // a1 释放唯一窗口后，即使 a2 优先级更高，也要先轮到 b1。
    await a1.cancel('让出窗口');
    const first = await b1;
    assert.equal(first.site, 'supplier-b');
    const during = f.pool.snapshot();
    assert.ok(!during.jobs.some(job => job.id === a2.id && job.state === 'running'));
    await first.cancel('让出窗口');
    const second = await a2;
    assert.equal(second.site, 'supplier-a');
    await tick(20);
    assert.ok(f.pool.snapshot().invariant.ok);
  } finally {
    await f.close();
  }
});
