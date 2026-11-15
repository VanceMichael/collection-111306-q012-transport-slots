import { resolve } from 'node:path';
import { startSource } from './source.mjs';
import { startTransport } from './transport.mjs';
import { CaptureStore } from './store.mjs';
import { preview } from './domain.mjs';
import { resolveView, RegistryBook, registryFromSnapshot } from './topology.mjs';
import { TransportPool } from './pool.mjs';

const tick = ms => new Promise(resolve => setTimeout(resolve, ms));

async function withTransports(specs, run) {
  const transports = specs.map(spec => startTransport(spec.capacity, spec));
  const started = [];
  try {
    for (const transport of transports) started.push(await transport);
    return await run(...started);
  } finally {
    for (const transport of started.reverse()) await transport.close().catch(() => {});
  }
}

async function withPool(pool, run) {
  try {
    return await run();
  } finally {
    await pool.close().catch(() => {});
  }
}

function consume(lease) {
  lease.stream.resume();
  lease.stream.on('data', () => {});
  return lease;
}

async function runScenarios(sourceUrl) {
  const scenarios = [];
  const record = scenario => scenarios.push(scenario);

  // 1. 两域共享一条通道，各有逻辑额度；大响应暂停时其他站点仍有进展。
  record(await scenarioShared());
  // 2. 同地址但缺少共享证明：拒绝，而不是凭地址推断。
  record(await scenarioUnproven());
  // 3. 容量收缩：已授权流不假装从未发生，排队作业保持不超发。
  record(await scenarioShrink());
  // 4. 别名迁移：排队作业只按新登记重新选择通道。
  record(await scenarioMigration());
  // 5. 服务端停止接收新流：在途可以收尾，排队不得被偷偷授出。
  record(await scenarioDrain());
  // 6. 一批大文件不长期饿死其他站点：最后一个窗口留给等待中的站点。
  record(await scenarioFairness());

  return scenarios;

  async function scenarioShared() {
    return withTransports(
      [{ capacity: 2, releaseDelayMs: 60, largeBytes: 4 * 1024 * 1024 }, { capacity: 1, releaseDelayMs: 60 }],
      async (sharedWire, separateWire) => {
        // 登记内容来自本地源实际返回的快照；连接地址来自本地传输的会话信息。
        const snapshot = await (await fetch(sourceUrl + '/api/snapshot')).json();
        const data = { sites: snapshot.sites, channels: snapshot.channels };
        // 两个通道各有一条独立本地会话，证明“同地址”并不是共享依据。
        const registry = registryFromSnapshot(data, { urls: { c1: sharedWire.url, c2: separateWire.url } });
        const pool = new TransportPool({
          origin: sharedWire.url,
          sites: data.sites,
          resolve: site => resolveView(registry, site),
          releaseGraceMs: 60,
          pausedTimeoutMs: 250
        });
        return withPool(pool, async () => {
          const transitions = [];
          pool.subscribe(event => {
            if (['granted', 'stream-closed', 'releasing', 'completed', 'cancelled', 'requeued', 'channel-settings', 'channel-draining'].includes(event.event)) {
              transitions.push({ seq: event.seq, event: event.event, job: event.job ?? null, site: event.site ?? null, channel: event.channel ?? null, reason: event.reason ?? null });
            }
          });
          const large = await pool.acquire('supplier-a', { path: '/large', headers: { 'x-size': String(3 * 1024 * 1024) } });
          await large.response();
          // 暂停读取：大响应占用真实流，但不再消费。
          large.pause();
          await tick(80);
          const urgent = consume(await pool.acquire('supplier-c', { path: '/data' }));
          await urgent.response();
          const heldSnapshot = pool.snapshot();
          await large.cancel('演示：不再需要大响应');
          // 取消返回时本地流已关闭，但供方释放确认尚未到达：这是 releasing，不是完成。
          const immediatelyAfterCancel = pool.snapshot();
          await large.settled();
          const final = pool.snapshot();
          return {
            name: '两域共享通道的额度与暂停读取',
            ok: heldSnapshot.invariant.ok && final.invariant.ok
              && heldSnapshot.sites.find(site => site.id === 'supplier-a').held <= 1
              && heldSnapshot.sites.find(site => site.id === 'supplier-b').held <= 2
              && heldSnapshot.totals.paused === 1
              && heldSnapshot.totals.wire === 2
              && immediatelyAfterCancel.totals.releasing === 1
              && immediatelyAfterCancel.totals.completed === 0
              && final.totals.cancelled === 1
              && final.totals.completed === 0,
            observed: {
              pausedWhileOthersProgress: {
                supplierA: heldSnapshot.sites.find(site => site.id === 'supplier-a'),
                supplierC: heldSnapshot.sites.find(site => site.id === 'supplier-c'),
                queue: heldSnapshot.queue,
                wire: heldSnapshot.channels
              },
              immediatelyAfterCancel: { totals: immediatelyAfterCancel.totals, sites: immediatelyAfterCancel.sites },
              afterCancelConfirmed: { totals: final.totals, sites: final.sites, invariant: final.invariant },
              wireServer: { shared: sharedWire.stats, separate: separateWire.stats },
              overCapacityGrants: final.pressure.overCapacityGrants
            },
            transitions
          };
        });
      }
    );
  }

  async function scenarioUnproven() {
    return withTransports([{ capacity: 1, releaseDelayMs: 20 }], async wire => {
      const pool = new TransportPool({
        origin: wire.url,
        sites: [{ id: 'supplier-x', limit: 1 }, { id: 'supplier-y', limit: 1 }],
        // 两个站点都看到同一条物理会话，但没有 proof；这必须被拒绝。
        resolve: site => ({
          site,
          channels: [{ id: 'same-session', url: wire.url, proof: null, sharedWith: [site === 'supplier-x' ? 'supplier-y' : 'supplier-x'] }]
        })
      });
      return withPool(pool, async () => {
        let outcome;
        try {
          await pool.acquire('supplier-x');
          outcome = { accepted: true };
        } catch (error) {
          outcome = { accepted: false, reason: error.message };
        }
        const snapshot = pool.snapshot();
        return {
          name: '同地址未证明可共享',
          // 拒绝必须表现为失败，而不是悄悄排队或凭空获得通道。
          ok: outcome.accepted === false && /证明/.test(outcome.reason)
            && snapshot.totals.failed === 1 && snapshot.totals.queued === 0,
          observed: { outcome, totals: snapshot.totals }
        };
      });
    });
  }

  async function scenarioShrink() {
    return withTransports([{ capacity: 2, releaseDelayMs: 20 }], async wire => {
      const book = new RegistryBook({
        sites: [{ id: 'supplier-a', limit: 1 }, { id: 'supplier-b', limit: 2 }],
        channels: [{ id: 'c1', capacity: 2, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' }]
      });
      for (const channel of book.registry.values()) channel.sessionUrl = wire.url;
      const pool = new TransportPool({ origin: wire.url, sites: book.export().sites, resolve: site => book.resolve(site), releaseGraceMs: 20 });
      return withPool(pool, async () => {
        const a = consume(await pool.acquire('supplier-a', { path: '/hold' }));
        const b = consume(await pool.acquire('supplier-b', { path: '/hold' }));
        const waiting = pool.acquire('supplier-b', { priority: -1, path: '/data' });
        await tick(40);
        wire.resize(1);
        await tick(60);
        const afterShrink = pool.snapshot();
        await a.cancel('演示：让窗口释放');
        await tick(30);
        const afterOneRelease = pool.snapshot();
        await b.cancel('演示：让窗口释放');
        const lease = consume(await waiting);
        const afterGrant = pool.snapshot();
        return {
          name: '容量收缩与恢复',
          ok: afterShrink.channels[0].remoteCapacity === 1
            && afterShrink.totals.wire === 2
            && afterShrink.pressure.overCapacityGrants === 0
            && afterOneRelease.totals.wire === 1
            && afterOneRelease.channels[0].freeWindow === 0
            && afterGrant.pressure.overCapacityGrants === 0
            && afterGrant.invariant.ok,
          observed: {
            afterShrink: { channel: afterShrink.channels[0], queue: afterShrink.queue, wire: afterShrink.totals.wire },
            afterOneRelease: { channel: afterOneRelease.channels[0], queue: afterOneRelease.queue.length },
            afterGrant: { wire: afterGrant.totals.wire, job: afterGrant.jobs.find(job => job.id === lease.id), channel: afterGrant.channels[0], invariant: afterGrant.invariant }
          }
        };
      });
    });
  }

  async function scenarioMigration() {
    return withTransports([{ capacity: 2, releaseDelayMs: 20 }, { capacity: 1, releaseDelayMs: 20 }], async (c1Wire, c2Wire) => {
      const data = {
        sites: [{ id: 'supplier-a', limit: 1 }, { id: 'supplier-b', limit: 2 }, { id: 'supplier-c', limit: 1 }],
        channels: [
          { id: 'c1', capacity: 2, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' },
          { id: 'c2', capacity: 1, aliases: ['supplier-c'], proof: 'local-registration-2' }
        ]
      };
      const book = new RegistryBook(structuredClone(data));
      book.registry.get('c1').sessionUrl = c1Wire.url;
      book.registry.get('c2').sessionUrl = c2Wire.url;
      const pool = new TransportPool({ origin: c1Wire.url, sites: data.sites, resolve: site => book.resolve(site), releaseGraceMs: 20 });
      return withPool(pool, async () => {
        consume(await pool.acquire('supplier-a', { path: '/hold' }));
        consume(await pool.acquire('supplier-b', { path: '/hold' }));
        const waiting = pool.acquire('supplier-b', { priority: -1 });
        await tick(30);
        const before = pool.snapshot().queue[0];
        const view = book.migrate('supplier-b', { from: 'c1', to: 'c2', proof: 'local-registration-2', reason: 'demo' });
        // 迁移后 supplier-b 只能走 c2；c2 是新会话，有独立窗口。
        pool.setBinding('supplier-b', view.channels.map(channel => channel.id), { proof: view.proof, reason: 'demo' });
        const lease = await Promise.race([waiting, tick(2000).then(() => null)]);
        if (!lease) throw new Error('迁移后排队作业未在 2 秒内获得授权');
        consume(lease);
        const after = pool.snapshot();
        return {
          name: '别名迁移后重新判断通道',
          ok: after.jobs.find(job => job.id === lease.id)?.channel === 'c2'
            && !after.channels.find(channel => channel.id === 'c1').aliases.includes('supplier-b')
            && after.invariant.ok,
          observed: {
            before: { queue: before },
            registry: view,
            after: { job: after.jobs.find(job => job.id === lease.id), channels: after.channels, invariant: after.invariant }
          }
        };
      });
    });
  }

  async function scenarioFairness() {
    return withTransports([{ capacity: 1, releaseDelayMs: 10 }], async wire => {
      const book = new RegistryBook({
        sites: [{ id: 'supplier-a', limit: 2 }, { id: 'supplier-b', limit: 1 }],
        channels: [{ id: 'c1', capacity: 1, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' }]
      });
      for (const channel of book.registry.values()) channel.sessionUrl = wire.url;
      const pool = new TransportPool({ origin: wire.url, sites: book.export().sites, resolve: site => book.resolve(site), releaseGraceMs: 10 });
      return withPool(pool, async () => {
        // supplier-a 占住唯一窗口，并且还有一批文件在排队；supplier-b 也在等。
        const a1 = consume(await pool.acquire('supplier-a', { path: '/hold' }));
        const b1 = pool.acquire('supplier-b', { priority: 0 });
        const a2 = pool.acquire('supplier-a', { priority: -10 });
        b1.then(consume); a2.then(consume);
        await tick(30);
        const queued = pool.snapshot().queue;
        // a 的第一个流结束：窗口必须给 b，不能给优先级更高的 a2。
        await a1.cancel('演示：让出唯一窗口');
        const bLease = await b1;
        const arbitration = pool.snapshot();
        const aWaiting = arbitration.queue.find(job => job.site === 'supplier-a');
        const aRunning = arbitration.jobs.find(job => job.site === 'supplier-a' && job.state === 'running');
        await bLease.cancel('演示：让出唯一窗口');
        const aLease = consume(await a2);
        const final = pool.snapshot();
        return {
          name: '一批大文件不饿死其他站点',
          ok: queued.length === 2
            && bLease.site === 'supplier-b'
            && !aRunning
            && Boolean(aWaiting)
            && final.jobs.find(job => job.id === aLease.id)?.channel === 'c1'
            && final.invariant.ok
            && final.pressure.overCapacityGrants === 0,
          observed: {
            queued: queued.map(job => ({ id: job.id, site: job.site, priority: job.priority, reason: job.reason })),
            arbitration: { grantedTo: bLease.site, waiting: aWaiting, runningA: aRunning ?? null },
            afterB: { job: final.jobs.find(job => job.id === aLease.id), invariant: final.invariant }
          }
        };
      });
    });
  }

  async function scenarioDrain() {
    return withTransports([{ capacity: 1, releaseDelayMs: 20 }], async wire => {
      const book = new RegistryBook({
        sites: [{ id: 'supplier-a', limit: 1 }, { id: 'supplier-b', limit: 1 }],
        channels: [{ id: 'c1', capacity: 1, aliases: ['supplier-a', 'supplier-b'], proof: 'local-registration-1' }]
      });
      for (const channel of book.registry.values()) channel.sessionUrl = wire.url;
      const pool = new TransportPool({ origin: wire.url, sites: book.export().sites, resolve: site => book.resolve(site), releaseGraceMs: 20 });
      return withPool(pool, async () => {
        const first = consume(await pool.acquire('supplier-a', { path: '/hold' }));
        // supplier-b 有自己的额度，但唯一的通道窗口在 supplier-a 手里。
        const waiting = pool.acquire('supplier-b', { priority: -1 });
        waiting.catch(() => {});
        await tick(30);
        wire.drain();
        await tick(60);
        // GOAWAY 已到达：会话可让在途流收尾，但排队作业不得再被授出。
        const draining = pool.snapshot();
        await first.cancel('演示：在途收尾');
        await tick(60);
        const afterRelease = pool.snapshot();
        let rejection = null;
        try { await waiting; } catch (error) { rejection = error.message; }
        const settled = pool.snapshot();
        return {
          name: '通道停止接收新流',
          ok: draining.channels[0].state === 'draining'
            && draining.totals.queued === 1
            && draining.totals.wire === 1
            && /停止接收新流/.test(draining.queue[0].reason)
            && afterRelease.totals.wire === 0
            && afterRelease.totals.completed === 0
            && afterRelease.pressure.overCapacityGrants === 0
            && settled.totals.failed === 1
            && settled.totals.completed === 0
            && settled.totals.queued === 0
            && rejection !== null,
          observed: {
            draining: { channel: draining.channels[0], queue: draining.queue },
            afterRelease: { totals: afterRelease.totals, queue: afterRelease.queue, invariant: afterRelease.invariant },
            settled: { totals: settled.totals, rejection }
          }
        };
      });
    });
  }
}

try {
  const source = await startSource();
  try {
    const observed = preview(await (await fetch(source.url + '/api/snapshot')).json());
    const path = resolve(process.env.CAPTURE_PATH ?? 'var/captures.json');
    const store = new CaptureStore(path);
    const scenarios = await runScenarios(source.url);
    // 源端控制只用于印证“源可以收缩/复位”，接收侧的判断在场景里独立产生。
    const control = async action => {
      const response = await fetch(source.url + '/api/action', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ action })
      });
      if (!response.ok) throw new Error(`源端控制 ${action} 失败`);
      return response.json();
    };
    await control('shrink');
    const shrunk = preview(await (await fetch(source.url + '/api/snapshot')).json());
    await control('reset');
    const restored = preview(await (await fetch(source.url + '/api/snapshot')).json());
    const allOk = scenarios.every(scenario => scenario.ok);
    const receipt = await store.append({
      kind: 'transport-slots',
      observed,
      demo: { ok: allOk, scenarios, sourceControls: { shrunk, restored } },
      at: new Date().toISOString()
    });
    console.log(JSON.stringify({
      资料源: source.url,
      接收文件: path,
      已存记录: receipt.revision,
      本次预览: observed,
      演示: {
        总体通过: allOk,
        未覆盖: ['真实供应商会话复用与账号额度', '跨进程重启后的排队恢复', '多进程共享同一通道'],
        源端控制: { shrink: shrunk, reset: restored },
        场景: scenarios.map(scenario => ({ 场景: scenario.name, 通过: scenario.ok, 观察: scenario.observed }))
      }
    }, null, 2));
    if (!allOk) process.exitCode = 1;
  } finally {
    await source.close();
  }
} catch (error) {
  console.error(JSON.stringify({ 错误: error.message, 栈: error.stack }, null, 2));
  process.exitCode = 1;
}
