import { connect, constants } from 'node:http2';

const DELAY = ms => new Promise(resolve => setTimeout(resolve, ms));
const TERMINAL = new Set(['completed', 'cancelled', 'failed', 'released']);
// 真实流：从授权到本地流关闭，计入通道在途。
const WIRE = new Set(['dispatching', 'running', 'paused', 'cancelling']);
// 站点额度：即使本地流已关闭，只要供方还没确认释放，就仍然占用站点额度。
// releasing 是“已结束、等待确认”的中间态，不是完成，也不能借出新额度。
const QUOTA = new Set(['dispatching', 'running', 'paused', 'cancelling', 'releasing']);

function abortError(message = '请求已取消') {
  const error = new Error(message);
  error.name = 'AbortError';
  return error;
}

function normaliseSites(sites) {
  const entries = Array.isArray(sites)
    ? sites.map(site => [site.id, site.limit])
    : Object.entries(sites ?? {});
  const result = new Map();
  for (const [id, limit] of entries) {
    if (typeof id !== 'string' || id.length === 0) throw new Error('站点标识必须是非空字符串');
    if (!Number.isInteger(limit) || limit < 1) throw new Error(`站点 ${id} 的逻辑额度必须是正整数`);
    result.set(id, limit);
  }
  return result;
}

/**
 * 两层资源池。
 *
 * 逻辑层：每个站点（域名/供应商）有独立额度，计数只在持有真实流的作业上。
 * 物理层：站点通过明确的连接信息（通道 id 及可共享证明）映射到 h2c 会话，
 * 每次授权都对应一条真实流；容量以服务端 SETTINGS 为准，绝不以域名个数推断。
 *
 * 排队作业在容量变化、别名迁移、通道 drain/关闭时重新评估，而不是沿用旧决定。
 */
export class TransportPool {
  constructor({ origin, sites, resolve, releaseGraceMs = 0, pausedTimeoutMs = 0, maxAttempts = 2, maxEvents = 10000 } = {}) {
    if (typeof origin !== 'string' || origin.length === 0) throw new Error('必须提供本地通道地址');
    if (typeof resolve !== 'function') throw new Error('必须提供连接信息解析函数');
    this.origin = origin;
    this.resolve = resolve;
    this.siteLimits = normaliseSites(sites);
    this.releaseGraceMs = Math.max(0, Number(releaseGraceMs) || 0);
    this.pausedTimeoutMs = Math.max(0, Number(pausedTimeoutMs) || 0);
    this.maxAttempts = Math.max(1, Number(maxAttempts) || 1);
    this.maxEvents = maxEvents;
    this.channels = new Map();
    this.bindings = new Map();
    this.pending = [];
    this.jobs = [];
    this.events = [];
    this.subscribers = new Set();
    this.sequence = 0;
    this.eventSequence = 0;
    this.closed = false;
    this.pumping = false;
    this.pumpAgain = false;
    this.peakWire = 0;
    this.peakWireByChannel = new Map();
    this.overCapacityGrants = 0;
    this.grants = 0;
  }

  // ---------- 观察 ----------

  emit(event) {
    const record = { seq: ++this.eventSequence, at: Date.now(), ...event };
    this.events.push(record);
    if (this.events.length > this.maxEvents) this.events.splice(0, this.events.length - this.maxEvents);
    for (const listener of this.subscribers) {
      try { listener(record, this); } catch { /* 观察者不能影响调度 */ }
    }
    return record;
  }

  subscribe(listener) {
    this.subscribers.add(listener);
    return () => this.subscribers.delete(listener);
  }

  siteOccupancy(id) {
    return {
      id,
      limit: this.siteLimits.get(id),
      held: this.activeForSite(id),
      releasing: this.jobs.filter(job => job.site === id && job.state === 'releasing').length,
      queued: this.pending.filter(job => job.site === id).length
    };
  }

  snapshot({ jobs = true } = {}) {
    const channels = [...this.channels.values()].map(channel => ({
      id: channel.id,
      state: channel.state,
      remoteCapacity: channel.remote,
      capacity: channel.capacity,
      wire: channel.wireInFlight,
      awaitingRelease: channel.awaitingRelease,
      freeWindow: channel.freeWindow,
      aliases: [...channel.aliases].sort(),
      sessionDestroyed: channel.session?.destroyed ?? false
    }));
    const sites = [...this.siteLimits.keys()].map(id => {
      const records = this.jobs.filter(job => job.site === id);
      const count = state => records.filter(job => job.state === state).length;
      return {
        id,
        limit: this.siteLimits.get(id),
        channel: [...(this.bindings.get(id)?.channels ?? [])],
        binding: this.bindings.get(id)?.status ?? 'unresolved',
        proof: this.bindings.get(id)?.proof ?? null,
        held: records.filter(job => QUOTA.has(job.state)).length,
        queued: this.pending.filter(job => job.site === id).length,
        running: count('running'),
        paused: count('paused'),
        cancelling: count('cancelling'),
        releasing: count('releasing'),
        completed: count('completed'),
        cancelled: count('cancelled'),
        failed: count('failed')
      };
    });
    const totals = { queued: this.pending.length, wire: 0, held: 0, completed: 0, cancelled: 0, failed: 0, releasing: 0, paused: 0, awaitingRelease: 0 };
    for (const channel of this.channels.values()) {
      totals.wire += channel.wireInFlight;
      totals.awaitingRelease += channel.awaitingRelease;
    }
    for (const job of this.jobs) {
      if (QUOTA.has(job.state)) totals.held += 1;
      if (job.state === 'paused') totals.paused += 1;
      if (job.state === 'releasing') totals.releasing += 1;
      if (job.state === 'completed') totals.completed += 1;
      if (job.state === 'cancelled') totals.cancelled += 1;
      if (job.state === 'failed') totals.failed += 1;
    }
    return {
      at: Date.now(),
      sites,
      channels,
      queue: this.pending.map(job => ({ id: job.id, site: job.site, priority: job.priority, reason: job.reason, attempts: job.attempt })),
      totals,
      pressure: {
        siteLimits: Object.fromEntries(this.siteLimits),
        grants: this.grants,
        overCapacityGrants: this.overCapacityGrants,
        peakWire: this.peakWire,
        peakWireByChannel: Object.fromEntries(this.peakWireByChannel),
        capacityOverlap: Object.fromEntries([...this.channels.values()]
          .filter(channel => Number.isFinite(channel.capacity) && channel.wireInFlight > channel.capacity)
          .map(channel => [channel.id, { wire: channel.wireInFlight, capacity: channel.capacity }])),
        releaseGraceMs: this.releaseGraceMs,
        pausedTimeoutMs: this.pausedTimeoutMs
      },
      invariant: this.checkInvariants(),
      ...(jobs ? { jobs: this.jobs.map(job => ({
        id: job.id,
        site: job.site,
        state: job.state,
        channel: job.channelId,
        priority: job.priority,
        reason: job.reason,
        attempts: job.attempt
      })) } : {})
    };
  }

  /** 快照自检，演示与测试都读取同一份判断。 */
  checkInvariants() {
    const problems = [];
    for (const [id, limit] of this.siteLimits) {
      const held = this.jobs.filter(job => job.site === id && QUOTA.has(job.state)).length;
      if (held > limit) problems.push(`站点 ${id} 占用 ${held} 超过额度 ${limit}`);
    }
    for (const channel of this.channels.values()) {
      if (channel.wireInFlight < 0) problems.push(`通道 ${channel.id} 流计数为负`);
    }
    if (this.overCapacityGrants > 0) problems.push(`发生 ${this.overCapacityGrants} 次超出通道容量的新授权`);
    const wire = [...this.channels.values()].reduce((sum, channel) => sum + channel.wireInFlight, 0);
    const jobsWithWire = this.jobs.filter(job => WIRE.has(job.state)).length;
    if (wire !== jobsWithWire) problems.push(`通道流合计 ${wire} 与作业流合计 ${jobsWithWire} 不一致`);
    const awaiting = [...this.channels.values()].reduce((sum, channel) => sum + channel.awaitingRelease, 0);
    const jobsReleasing = this.jobs.filter(job => job.state === 'releasing').length;
    if (awaiting !== jobsReleasing) problems.push(`待确认释放 ${awaiting} 与待结算作业 ${jobsReleasing} 不一致`);
    if (this.jobs.some(job => job.state === 'completed' && job.cancelRequested)) problems.push('取消的作业被记成完成');
    for (const job of this.jobs) {
      if (TERMINAL.has(job.state) && job.stream && !job.stream.destroyed && !job.stream.closed) {
        problems.push(`作业 ${job.id} 已结束但流仍打开`);
      }
      if (job.state === 'queued' && job.stream) problems.push(`作业 ${job.id} 在排队却持有流`);
    }
    return { ok: problems.length === 0, problems };
  }

  // ---------- 连接信息 ----------

  binding(site) {
    if (!this.siteLimits.has(site)) throw new Error('未知站点: ' + site);
    let binding = this.bindings.get(site);
    if (!binding) {
      binding = { status: 'pending', channels: new Set(), urls: new Map(), proofs: new Map(), proof: null, error: null };
      this.bindings.set(site, binding);
      const fail = error => {
        binding.status = 'failed';
        binding.error = error.message;
        binding.channels.clear();
        this.emit({ event: 'binding-failed', site, reason: error.message });
        this.failPending(site, '连接信息不可用: ' + error.message, error);
        this.pump();
      };
      Promise.resolve()
        .then(() => this.resolve(site))
        .then(info => {
          if (this.closed) return;
          const channels = Array.isArray(info?.channels) ? info.channels : [];
          if (channels.length === 0) {
            binding.status = 'unavailable';
            binding.proof = info?.proof ?? null;
            const reason = '没有明确的连接信息，不能把域名当成通道';
            this.emit({ event: 'binding-unavailable', site, reason });
            this.failPending(site, reason);
            this.pump();
            return;
          }
          const missing = this.requiredProof(site, info);
          if (missing) return fail(new Error(missing));
          binding.status = 'ready';
          const proofs = channels.map(channel => typeof channel === 'string' ? null : channel.proof).filter(Boolean);
          binding.proof = proofs.length > 0 ? proofs.join(',') : info?.proof ?? null;
          try {
            for (const value of channels) this.attach(
              site,
              typeof value === 'string' ? value : value?.id,
              typeof value === 'string' ? null : value?.url,
              typeof value === 'string' ? null : value?.capacity,
              typeof value === 'string' ? null : value?.proof ?? null
            );
          } catch (error) {
            return fail(error);
          }
          this.emit({ event: 'binding', site, channels: [...binding.channels], proof: binding.proof });
          this.pump();
        })
        .catch(fail);
    }
    return binding;
  }

  /** 显式迁移：只有明确的连接信息才能改变站点可用的通道。 */
  setBinding(site, channelIds, { proof = null, reason = 'registry', urls = null } = {}) {
    if (!this.siteLimits.has(site)) throw new Error('未知站点: ' + site);
    const binding = this.bindings.get(site) ?? { status: 'ready', channels: new Set(), urls: new Map(), proofs: new Map(), proof: null, error: null };
    this.bindings.set(site, binding);
    const next = new Set();
    for (const id of channelIds) {
      if (typeof id !== 'string' || id.length === 0) throw new Error('通道标识必须是非空字符串');
      next.add(id);
    }
    const previous = new Set(binding.channels);
    const previousProof = binding.proof;
    binding.status = 'ready';
    binding.error = null;
    if (proof !== null) binding.proof = proof;
    // 先建立新归属；任何证据冲突都保留旧归属，不留下半迁移状态。
    for (const id of next) {
      try {
        this.attach(site, id, urls?.get(id) ?? binding.urls.get(id) ?? null, null, proof ?? binding.proofs.get(id) ?? null);
      } catch (error) {
        binding.channels = previous;
        binding.proof = previousProof;
        const failure = new Error(error.message);
        this.emit({ event: 'binding-failed', site, reason: failure.message });
        this.failPending(site, failure.message, failure);
        throw failure;
      }
    }
    for (const id of previous) {
      if (!next.has(id)) {
        this.channels.get(id)?.removeAlias(site);
        binding.urls.delete(id);
        binding.proofs.delete(id);
      }
    }
    this.emit({ event: 'aliases', site, channels: [...next], proof: binding.proof, reason });
    this.pump();
  }

  /** 登记变化后重新读取连接信息；排队作业据此改换通道。 */
  async rebind(site, reason = 'registry-reload') {
    const info = await this.resolve(site);
    const missing = this.requiredProof(site, info);
    const urls = new Map();
    for (const value of Array.isArray(info?.channels) ? info.channels : []) {
      if (typeof value !== 'string' && value?.id) urls.set(value.id, value.url ?? null);
    }
    const ids = [...urls.keys()];
    if (missing) {
      this.setBinding(site, [], { proof: info?.proof ?? null, reason: reason + ': 缺少共享证明' });
      return;
    }
    this.setBinding(site, ids, { proof: info?.proof ?? null, reason, urls });
  }

  /**
   * 多站点通道必须带 proof；只凭“地址相同”或别名数量推断共享是不可接受的。
   * 返回 null 表示可接受，否则返回拒绝原因。
   */
  requiredProof(site, info) {
    const list = Array.isArray(info?.channels) ? info.channels : [];
    for (const value of list) {
      if (typeof value === 'string') return `通道 ${value} 缺少明确的连接记录`;
      const sharedWith = Array.isArray(value.sharedWith) ? value.sharedWith : [];
      // 独占通道不需要“共享证明”；但只要出现第二个站点，就必须有 proof。
      if (sharedWith.length > 0 && !value.proof) {
        return `通道 ${value.id} 与 ${sharedWith.join(',')} 共享但缺少证明`;
      }
    }
    return null;
  }

  failPending(site, reason, error = new Error(reason)) {
    for (const job of [...this.pending]) {
      if (job.site !== site) continue;
      this.dropPending(job);
      job.state = 'failed';
      job.reason = reason;
      job.reject?.(error);
    }
  }

  attach(site, channelId, url = null, declaredCapacity = null, proof = null) {
    if (typeof channelId !== 'string' || channelId.length === 0) throw new Error('连接信息缺少通道标识');
    const binding = this.bindings.get(site);
    const channel = this.ensureChannel(channelId, url ?? binding.urls.get(channelId));
    if (Number.isInteger(declaredCapacity)) channel.declared = declaredCapacity;
    const expected = proof ?? binding.proofs.get(channelId) ?? null;
    const others = [...channel.aliases].filter(alias => alias !== site);
    // 两个站点同时指向同一通道 id，必须有可核对的一致证明；地址相同不算证据。
    if (others.length > 0 && (!expected || !channel.proof)) {
      throw new Error(`通道 ${channelId} 已被 ${others.join(',')} 使用，缺少共享证明`);
    }
    if (channel.proof && expected && channel.proof !== expected) {
      throw new Error(`通道 ${channelId} 的共享证明不一致`);
    }
    channel.proof = channel.proof ?? expected ?? null;
    binding.channels.add(channelId);
    if (expected) binding.proofs.set(channelId, expected);
    if (url) binding.urls.set(channelId, url);
    channel.addAlias(site);
  }

  ensureChannel(id, url = null) {
    let channel = this.channels.get(id);
    if (channel) {
      // 同一通道 id 只能对应一个会话；若连接信息给出不同地址，明确报错而不是默默复用。
      if (url && channel.url !== url) throw new Error(`通道 ${id} 的连接信息不一致: ${channel.url} 与 ${url}`);
      return channel;
    }
    if (this.closed) throw new Error('资源池已关闭');
    channel = new Channel(this, id, url ?? this.origin);
    this.channels.set(id, channel);
    channel.connect();
    return channel;
  }

  // ---------- 调度 ----------

  acquire(site, { priority = 0, signal, path = '/data', headers = {}, channelId = null } = {}) {
    if (this.closed) return Promise.reject(new Error('资源池已关闭'));
    if (!this.siteLimits.has(site)) return Promise.reject(new Error('未知站点: ' + site));
    if (!Number.isFinite(priority)) return Promise.reject(new Error('优先级必须是数字'));
    if (signal?.aborted) return Promise.reject(abortError());
    const job = {
      id: 'j' + String(++this.sequence).padStart(4, '0'),
      site,
      priority,
      path,
      headers,
      channelId,
      state: 'queued',
      reason: '等待绑定与额度',
      at: Date.now(),
      attempt: 0,
      readPaused: false,
      cancelRequested: false,
      cancelReason: null,
      responseHeaders: null,
      responseEnded: false,
      settled: null,
      attemptClosed: null,
      localClosed: null,
      localClose: null,
      pausedTimer: null,
      responseWaiters: []
    };
    this.jobs.push(job);
    const promise = new Promise((resolve, reject) => {
      job.resolve = resolve;
      job.reject = reject;
    });
    // 调用方可能放弃等待；内部标记已处理，避免进程因未处理的拒绝退出。
    promise.catch(() => {});
    job.abort = () => this.abortPending(job);
    if (signal) {
      job.signal = signal;
      signal.addEventListener('abort', job.abort, { once: true });
    }
    this.pending.push(job);
    this.emit({ event: 'queued', job: job.id, site, priority });
    this.binding(site);
    this.pump();
    return promise;
  }

  abortPending(job) {
    if (job.outcome) return;
    if (job.stream || QUOTA.has(job.state)) {
      // 已授权：走真实取消，等待流关闭。
      void this.cancelJob(job, 'aborted');
      return;
    }
    this.dropPending(job);
    if (!TERMINAL.has(job.state)) {
      job.state = 'cancelled';
      job.reason = '排队时取消';
      this.emit({ event: 'queue-cancelled', job: job.id, site: job.site, reason: job.reason });
      job.reject?.(abortError());
    }
    this.pump();
  }

  dropPending(job) {
    const index = this.pending.indexOf(job);
    if (index >= 0) this.pending.splice(index, 1);
    job.signal?.removeEventListener('abort', job.abort);
  }

  activeForSite(site) {
    let count = 0;
    for (const job of this.jobs) if (job.site === site && QUOTA.has(job.state)) count += 1;
    return count;
  }

  eligible(job) {
    if (job.state !== 'queued' || job.cancelRequested) return false;
    const binding = this.bindings.get(job.site);
    if (binding?.status !== 'ready' || binding.channels.size === 0) return false;
    return this.activeForSite(job.site) < this.siteLimits.get(job.site);
  }

  compare(left, right) {
    return left.priority - right.priority
      || this.activeForSite(left.site) - this.activeForSite(right.site)
      || left.at - right.at
      || left.id.localeCompare(right.id);
  }

  chooseChannel(job) {
    const binding = this.bindings.get(job.site);
    if (!binding) return null;
    const candidates = [];
    for (const id of binding.channels) {
      if (job.channelId && id !== job.channelId) continue;
      const channel = this.channels.get(id);
      if (!channel || !channel.aliases.has(job.site)) continue;
      if (channel.state !== 'open' || channel.freeWindow <= 0) continue;
      if (!this.fairnessAllows(job, channel)) continue;
      candidates.push(channel);
    }
    candidates.sort((left, right) => left.wireInFlight - right.wireInFlight || left.id.localeCompare(right.id));
    return candidates[0] ?? null;
  }

  siteWireOnChannel(site, channelId) {
    let count = 0;
    for (const job of this.jobs) {
      if (job.site === site && job.channelId === channelId && WIRE.has(job.state)) count += 1;
    }
    return count;
  }

  /**
   * 共享通道上的公平性，避免某站点的一批文件长期占满通道：
   * - 刚用过这个窗口的站点，如果还有其他站点在等，就不立刻再拿一次（轮转）；
   * - 已在用窗口的站点不能把最后一个窗口也拿走，给尚未拿到窗口的站点留出空间。
   */
  fairnessAllows(job, channel) {
    const waiting = this.waitingSitesOnChannel(channel);
    if (channel.lastServed === job.site && [...waiting].some(site => site !== job.site)) {
      return false;
    }
    if (this.siteWireOnChannel(job.site, channel.id) === 0) return true;
    const idleOther = [...waiting].some(site => site !== job.site && this.siteWireOnChannel(site, channel.id) === 0);
    if (!idleOther) return true;
    // 本次授权后必须仍至少留一个窗口给等待中的站点。
    return channel.freeWindow >= 2;
  }

  waitingSitesOnChannel(channel) {
    const sites = new Set();
    for (const job of this.pending) {
      if (job.state !== 'queued' || !this.eligible(job)) continue;
      const binding = this.bindings.get(job.site);
      if (!binding?.channels.has(channel.id)) continue;
      if (job.channelId && job.channelId !== channel.id) continue;
      sites.add(job.site);
    }
    return sites;
  }

  pump() {
    if (this.closed) return;
    if (this.pumping) { this.pumpAgain = true; return; }
    this.pumping = true;
    try {
      let progressed = true;
      while (progressed && !this.closed) {
        progressed = false;
        const candidates = this.pending.filter(job => this.eligible(job)).sort((a, b) => this.compare(a, b));
        for (const job of candidates) {
          const channel = this.chooseChannel(job);
          if (!channel) { this.explain(job); continue; }
          this.grant(job, channel);
          progressed = true;
          break;
        }
        this.explainAll();
        this.reapUnavailable();
      }
    } finally {
      this.pumping = false;
    }
    if (this.pumpAgain) {
      this.pumpAgain = false;
      this.pump();
    }
  }

  /**
   * 绑定通道全部不可再用（连接失败/会话已关闭）时，排队作业不能无限等待。
   * drain 中的通道仍可能收尾，不算不可用，但会话一旦关闭就会在这里结算。
   */
  reapUnavailable() {
    if (this.closed) return;
    for (const job of [...this.pending]) {
      if (job.state !== 'queued') continue;
      const binding = this.bindings.get(job.site);
      if (binding?.status !== 'ready' || binding.channels.size === 0) continue;
      const channels = [...binding.channels].map(id => this.channels.get(id)).filter(Boolean);
      if (channels.length === 0) continue;
      const usable = channels.some(channel => channel.state === 'connecting' || channel.state === 'open' || channel.state === 'draining');
      if (usable) continue;
      this.dropPending(job);
      job.state = 'failed';
      job.reason = '通道不可用且没有替代连接信息';
      this.emit({ event: 'failed', job: job.id, site: job.site, channel: job.channelId, reason: job.reason });
      job.reject?.(new Error(job.reason));
    }
  }

  /** 排队原因必须来自当前资源状态，而不是入队时的旧判断。 */
  explain(job) {
    const binding = this.bindings.get(job.site);
    let reason;
    if (binding?.status === 'pending' || binding?.status === 'unresolved') reason = '等待明确的连接信息';
    else if (binding?.status === 'unavailable') reason = '没有明确的连接信息';
    else if (binding?.status === 'failed') reason = '连接信息不可用';
    else if (this.activeForSite(job.site) >= this.siteLimits.get(job.site)) reason = `站点额度 ${this.siteLimits.get(job.site)} 已满`;
    else {
      const ids = [...(binding?.channels ?? [])];
      const channels = ids.map(id => this.channels.get(id)).filter(Boolean);
      if (channels.some(channel => channel.state === 'connecting')) reason = '等待通道建立会话';
      else if (channels.some(channel => channel.state === 'open' && channel.freeWindow > 0 && !this.fairnessAllows(job, channel))) {
        reason = '为等待中的其他站点保留窗口';
      }
      else if (channels.some(channel => channel.state === 'open' && channel.freeWindow > 0)) reason = '等待其他站点让出流的窗口';
      else if (channels.some(channel => channel.state === 'open')) reason = `通道容量已满（在途 ${channels.reduce((sum, channel) => sum + channel.wireInFlight, 0)}）`;
      else reason = '通道已停止接收新流或已关闭';
    }
    if (job.reason !== reason) {
      job.reason = reason;
      this.emit({ event: 'queue-reason', job: job.id, site: job.site, reason });
    }
  }

  explainAll() {
    for (const job of this.pending) if (job.state === 'queued') this.explain(job);
  }

  grant(job, channel) {
    this.dropPending(job);
    job.state = 'dispatching';
    job.reason = '已获得额度';
    job.channelId = channel.id;
    job.attempt += 1;
    this.grants += 1;
    channel.wireInFlight += 1;
    channel.lastServed = job.site;
    const wire = channel.wireInFlight;
    if (Number.isFinite(channel.capacity) && wire > channel.capacity) {
      // 记录而不隐藏：容量收缩与已授权流之间存在无法避免的窗口，
      // 这里必须能观察到“曾超发”而不是让数字看起来永远合规。
      this.overCapacityGrants += 1;
      this.emit({ event: 'over-capacity-grant', job: job.id, channel: channel.id, wire, capacity: channel.capacity });
    }
    this.peakWire = Math.max(this.peakWire, [...this.channels.values()].reduce((sum, item) => sum + item.wireInFlight, 0));
    this.peakWireByChannel.set(channel.id, Math.max(this.peakWireByChannel.get(channel.id) ?? 0, channel.wireInFlight));
    this.emit({ event: 'granted', job: job.id, site: job.site, channel: channel.id, wire: channel.wireInFlight, attempt: job.attempt });
    let stream;
    try {
      stream = channel.session.request({
        ':method': 'GET',
        ':path': job.path,
        'x-request-id': job.id,
        'x-site': job.site,
        ...normaliseHeaders(job.headers)
      });
    } catch (error) {
      channel.releaseWire();
      job.stream = null;
      if (error.code === 'ERR_HTTP2_INVALID_SESSION') {
        // 会话已不可用：直接失败，避免在上限内反复重排到同一会话。
        return void this.finish(job, job.cancelRequested ? 'cancelled' : 'failed', '通道会话不可用: ' + error.message);
      }
      if (this.closed || job.attempt >= this.maxAttempts) {
        return void this.finish(job, job.cancelRequested ? 'cancelled' : 'failed', '通道不接受新流: ' + error.message);
      }
      this.requeue(job, '通道不接受新流: ' + error.message);
      return;
    }
    job.stream = stream;
    job.state = 'running';
    job.responseHeaders = null;
    job.responseEnded = false;
    job.streamError = null;
    job.attemptClosed = new Promise(resolve => { job.attemptClose = resolve; });
    job.localClosed = new Promise(resolve => { job.localClose = resolve; });
    stream.on('error', error => { job.streamError = error; });
    stream.once('response', headers => {
      if (job.stream !== stream) return;
      job.responseHeaders = headers;
      job.state = job.readPaused ? 'paused' : 'running';
      const waiters = job.responseWaiters.splice(0);
      for (const waiter of waiters) waiter.resolve({ status: headers[':status'], headers });
      this.emit({ event: 'response', job: job.id, site: job.site, channel: channel.id, status: headers[':status'] });
    });
    stream.once('end', () => {
      if (job.stream !== stream) return;
      job.responseEnded = true;
      this.emit({ event: 'response-end', job: job.id, channel: channel.id });
    });
    stream.once('close', () => {
      // 先结算本次尝试的关闭，避免重排后的旧流让取消一直等待。
      job.attemptClose?.();
      job.attemptClose = null;
      if (job.stream !== stream) return;
      this.streamClosed(job, channel, stream);
    });
    job.resolve?.(new Lease(this, job));
    job.resolve = null;
    job.reject = null;
    this.emit({ event: 'dispatched', job: job.id, site: job.site, channel: channel.id, wire: channel.wireInFlight });
  }

  requeue(job, reason, { channelId = null } = {}) {
    if (this.closed || job.cancelRequested || job.settled) return;
    job.state = 'queued';
    job.reason = reason;
    job.channelId = channelId;
    job.stream = null;
    job.responseHeaders = null;
    job.responseEnded = false;
    job.readPaused = false;
    clearTimeout(job.pausedTimer);
    this.pending.push(job);
    this.emit({ event: 'requeued', job: job.id, site: job.site, reason, attempts: job.attempt });
    this.pump();
  }

  streamClosed(job, channel, stream) {
    channel.releaseWire();
    const confirmed = channel.confirmRelease();
    job.localClose?.();
    job.localClose = null;
    clearTimeout(job.pausedTimer);
    job.pausedTimer = null;
    const wasPaused = job.readPaused;
    job.readPaused = false;
    this.emit({ event: 'stream-closed', job: job.id, site: job.site, channel: channel.id, wire: channel.wireInFlight, paused: wasPaused });
    if (job.settled) return;
    if (job.cancelRequested) return void this.finish(job, 'cancelled', job.cancelReason ?? 'cancelled', confirmed);
    if (job.responseEnded) return void this.finish(job, 'completed', '响应读取完成', confirmed);
    if (wasPaused) return void this.finish(job, 'released', '暂停读取期间流被关闭', confirmed);
    if (!job.responseHeaders && job.attempt < this.maxAttempts && !this.closed) {
      return this.requeue(job, '连接中断，重新判断可用通道', { channelId: null });
    }
    return void this.finish(job, 'failed', job.streamError?.message ?? '流在响应完成前关闭', confirmed);
  }

  finish(job, outcome, reason, confirmed = null) {
    if (job.settled) return job.settled;
    job.state = 'releasing';
    job.outcome = outcome;
    job.reason = reason;
    this.emit({ event: 'releasing', job: job.id, site: job.site, outcome, reason });
    job.settled = (async () => {
      // 供方未确认释放前，这条流对应的窗口不能借给下一次授权。
      if (confirmed) await confirmed;
      else if (this.releaseGraceMs > 0) await DELAY(this.releaseGraceMs);
      job.state = outcome;
      job.reason = reason;
      job.finishedAt = Date.now();
      job.signal?.removeEventListener('abort', job.abort);
      for (const waiter of job.responseWaiters.splice(0)) waiter.reject(new Error(reason));
      this.emit({ event: outcome, job: job.id, site: job.site, channel: job.channelId, reason });
      this.pump();
    })();
    return job.settled;
  }

  /**
   * 请求取消。返回时本地流已关闭（真实释放已在传输层发生）；
   * 供方确认释放是之后可观察的 releasing 阶段，cancel 不把它记成完成。
   */
  async cancelJob(job, reason) {
    // 已定终态（含等待释放确认）的作业不能因为迟到的取消被改写。
    if (job.settled || job.outcome) return job.settled;
    if (job.stream) {
      if (!job.cancelRequested) {
        job.cancelRequested = true;
        job.cancelReason = reason;
        job.state = 'cancelling';
        job.reason = reason;
        this.emit({ event: 'cancel-requested', job: job.id, site: job.site, channel: job.channelId, reason });
      }
      // close() 只发送 RST 并不一定立刻触发 close；若读侧暂停或没人消费，
      // 需要 destroy() 让本地流真正结束，才能等待到真实释放。
      try { job.stream.close(constants.NGHTTP2_CANCEL); } catch { /* 已关闭 */ }
      try { job.stream.destroy(); } catch { /* 已销毁 */ }
      await job.localClosed;
      return null;
    }
    // 尚未分配流（理论上 Lease 只在授权后产生）。
    this.dropPending(job);
    job.cancelRequested = true;
    job.cancelReason = reason;
    this.emit({ event: 'cancel-requested', job: job.id, site: job.site, channel: null, reason });
    return this.finish(job, 'cancelled', reason);
  }

  pause(job) {
    if (job.settled || !job.stream || job.responseEnded) return false;
    if (job.readPaused) return true;
    job.readPaused = true;
    if (job.state === 'running') job.state = 'paused';
    job.stream.pause();
    this.emit({ event: 'reading-paused', job: job.id, site: job.site, channel: job.channelId });
    if (this.pausedTimeoutMs > 0) {
      job.pausedTimer = setTimeout(() => {
        if (job.settled || !job.readPaused || job.cancelRequested) return;
        this.emit({ event: 'paused-timeout', job: job.id, site: job.site });
        void this.cancelJob(job, '暂停读取超时，释放占用');
      }, this.pausedTimeoutMs);
    }
    return true;
  }

  resume(job) {
    if (job.settled || !job.stream || !job.readPaused) return false;
    job.readPaused = false;
    if (job.state === 'paused') job.state = 'running';
    clearTimeout(job.pausedTimer);
    job.pausedTimer = null;
    job.stream.resume();
    this.emit({ event: 'reading-resumed', job: job.id, site: job.site, channel: job.channelId });
    return true;
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    for (const job of [...this.pending]) {
      this.dropPending(job);
      job.state = 'cancelled';
      job.reason = '资源池关闭';
      this.emit({ event: 'queue-cancelled', job: job.id, site: job.site, reason: job.reason });
      job.reject?.(new Error('资源池关闭'));
    }
    const running = this.jobs.filter(job => job.stream && !job.settled && !job.cancelRequested);
    await Promise.all(running.map(job => this.cancelJob(job, '资源池关闭')));
    // 关闭通道会结束所有释放确认计时器，避免收尾等待完整的释放宽限。
    for (const channel of this.channels.values()) await channel.close();
    await Promise.all(this.jobs.map(job => job.settled).filter(Boolean));
    this.emit({ event: 'closed' });
    this.subscribers.clear();
  }
}

class Channel {
  constructor(pool, id, url) {
    this.pool = pool;
    this.id = id;
    this.url = url;
    this.declared = null;
    this.aliases = new Set();
    this.state = 'connecting';
    this.session = null;
    this.remote = undefined;
    this.wireInFlight = 0;
    this.awaitingRelease = 0;
    this.lastServed = null;
    this.releaseTimers = new Set();
    this.releaseWaiters = new Set();
    this.capacity = undefined;
  }

  get freeWindow() {
    if (this.state !== 'open') return 0;
    const capacity = Number.isFinite(this.capacity) ? this.capacity : Infinity;
    // 已结束但供方还没确认释放的流仍然占着窗口。
    return Math.max(0, capacity - this.wireInFlight - this.awaitingRelease);
  }

  addAlias(site) {
    if (!this.aliases.has(site)) {
      this.aliases.add(site);
      this.pool.emit({ event: 'alias-added', channel: this.id, site });
    }
  }

  removeAlias(site) {
    if (this.aliases.delete(site)) this.pool.emit({ event: 'alias-removed', channel: this.id, site });
  }

  connect() {
    const session = connect(this.url);
    this.session = session;
    session.on('error', error => {
      if (this.state === 'connecting') {
        this.state = 'failed';
        this.capacity = 0;
        this.pool.pump();
      }
      this.pool.emit({ event: 'channel-error', channel: this.id, code: error.code, reason: error.message });
    });
    session.on('remoteSettings', settings => {
      if (this.state === 'closed' || this.state === 'draining') return;
      this.applyRemote(settings, this.state === 'connecting');
    });
    session.on('goaway', (code, lastStreamID) => {
      if (this.state === 'closed') return;
      this.state = 'draining';
      this.capacity = 0;
      this.pool.emit({ event: 'channel-draining', channel: this.id, code, lastStreamID });
      this.pool.pump();
    });
    session.on('close', () => {
      const wasClosed = this.state === 'closed';
      this.state = 'closed';
      this.capacity = 0;
      if (!wasClosed) this.pool.emit({ event: 'channel-closed', channel: this.id, reason: '会话已关闭' });
      this.pool.pump();
    });
  }

  applyRemote(settings, initial) {
    const remote = settings.maxConcurrentStreams;
    this.remote = remote;
    this.capacity = Number.isFinite(remote) ? remote : Infinity;
    if (this.state === 'connecting') {
      this.state = 'open';
      this.pool.emit({ event: 'channel-open', channel: this.id, capacity: this.capacity, initial });
      if (Number.isInteger(this.declared) && this.declared !== this.capacity) {
        // 登记容量与真实设置不一致时必须可见，并以源端设置作为物理上限。
        this.pool.emit({
          event: 'capacity-discrepancy',
          channel: this.id,
          declared: this.declared,
          observed: this.capacity,
          reason: '登记容量与源端 SETTINGS 不一致，以源端为准'
        });
      }
    } else {
      this.pool.emit({ event: 'channel-settings', channel: this.id, capacity: this.capacity, wire: this.wireInFlight });
    }
    this.pool.pump();
  }

  releaseWire() {
    this.wireInFlight = Math.max(0, this.wireInFlight - 1);
  }

  /** 流关闭后进入“等待供方释放确认”；确认前容量不对新流开放。 */
  confirmRelease() {
    this.awaitingRelease += 1;
    return new Promise(resolve => {
      const done = () => {
        if (this.releaseTimers.has(timer)) this.releaseTimers.delete(timer);
        this.releaseWaiters.delete(done);
        this.awaitingRelease = Math.max(0, this.awaitingRelease - 1);
        resolve();
        this.pool.emit({ event: 'release-confirmed', channel: this.id, awaitingRelease: this.awaitingRelease });
        this.pool.pump();
      };
      const timer = setTimeout(done, this.pool.releaseGraceMs);
      timer.unref?.();
      this.releaseTimers.add(timer);
      this.releaseWaiters.add(done);
    });
  }

  async close() {
    if (!this.session) return;
    this.state = 'closed';
    if (!this.session.destroyed) this.session.destroy();
    // 关闭时不再等待供方确认，避免演示或测试收尾被计时器拖住。
    for (const timer of this.releaseTimers) clearTimeout(timer);
    this.releaseTimers.clear();
    const waiters = [...this.releaseWaiters];
    this.releaseWaiters.clear();
    for (const done of waiters) done();
    await DELAY(0);
  }
}

function normaliseHeaders(headers) {
  const result = {};
  for (const [key, value] of Object.entries(headers ?? {})) result[String(key).toLowerCase()] = String(value);
  return result;
}

/**
 * 一次逻辑授权。stream 是当前真实流；重排后由作业替换，lease 始终读取最新值。
 * 取消不会记成完成：等待流关闭，再按取消结算。
 */
export class Lease {
  constructor(pool, job) {
    this.pool = pool;
    this.job = job;
  }

  get id() { return this.job.id; }
  get site() { return this.job.site; }
  get channelId() { return this.job.channelId; }
  get state() { return this.job.state; }
  get stream() { return this.job.stream; }
  get headers() { return this.job.responseHeaders; }
  get responseEnded() { return this.job.responseEnded; }

  async response() {
    if (this.job.responseHeaders) return { status: this.job.responseHeaders[':status'], headers: this.job.responseHeaders };
    if (TERMINAL.has(this.job.state) || this.job.state === 'releasing') throw new Error(this.job.reason ?? '作业已结束');
    const pending = new Promise((resolve, reject) => this.job.responseWaiters.push({ resolve, reject }));
    pending.catch(() => {});
    return pending;
  }

  pause() { return this.pool.pause(this.job); }
  resume() { return this.pool.resume(this.job); }

  async cancel(reason = '用户取消') {
    await this.pool.cancelJob(this.job, reason);
    return this.pool.snapshot({ jobs: false });
  }

  /** 等供方释放确认完成；只有到这里才是终态。 */
  settled() { return this.job.settled ?? Promise.resolve(); }

  async release() {
    if (this.job.settled) return this.job.settled;
    if (this.job.stream && !this.job.responseEnded) {
      return this.cancel('调用方释放未读完的流');
    }
    await this.job.attemptClosed;
    return this.job.settled;
  }
}
