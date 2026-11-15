/**
 * 通道登记与连接信息。
 *
 * 站点到通道的归属只能来自登记里明确写出的 physical 关系：
 * - shared: 带 proof 的显式共享记录；
 * - exclusive: 该站点独占一条已登记通道。
 *
 * 这里刻意不使用主机名、地址或站点名相似度推断“可以共享”。
 * URL 只说明会话连到哪里，不代表两个域名可以共用同一条连接。
 */

export function deriveChannels(data) {
  const channels = new Map();
  const guarantee = id => {
    let channel = channels.get(id);
    if (!channel) {
      channel = { id, capacity: null, aliases: new Set(), proof: null, state: 'unknown', sessionUrl: null };
      channels.set(id, channel);
    }
    return channel;
  };
  for (const channel of data.channels ?? []) {
    if (typeof channel.id !== 'string' || channel.id.length === 0) throw new Error('通道登记缺少 id');
    if (!Number.isInteger(channel.capacity) || channel.capacity < 1) throw new Error(`通道 ${channel.id} 的登记容量非法`);
    const target = guarantee(channel.id);
    target.capacity = channel.capacity;
    target.proof = channel.proof ?? null;
    for (const alias of channel.aliases ?? []) target.aliases.add(alias);
  }
  for (const link of data.shared ?? []) {
    const target = guarantee(link.channel);
    if (target.proof !== null && target.proof !== link.proof) throw new Error(`通道 ${link.channel} 的共享证明不一致`);
    target.proof = link.proof ?? null;
    for (const site of link.sites ?? []) target.aliases.add(site);
  }
  for (const link of data.exclusive ?? []) {
    const target = guarantee(link.channel);
    if (target.proof !== null && target.proof !== link.proof) throw new Error(`通道 ${link.channel} 的共享证明不一致`);
    target.proof = link.proof ?? null;
    target.aliases.add(link.site);
  }
  return channels;
}

export function createRegistry(data) {
  const siteLimits = new Map((data.sites ?? []).map(site => [site.id, site.limit]));
  const registry = new Map();
  for (const channel of deriveChannels(data).values()) {
    if (channel.aliases.size > 1 && !channel.proof) {
      throw new Error(`通道 ${channel.id} 登记了多个站点但缺少共享证明`);
    }
    registry.set(channel.id, {
      id: channel.id,
      capacity: channel.capacity,
      proof: channel.proof,
      aliases: new Set(channel.aliases),
      state: channel.state,
      sessionUrl: channel.sessionUrl
    });
  }
  return { siteLimits, registry };
}

/**
 * 连接信息视图：只暴露一个站点可以使用的通道，并要求 proof。
 * 未证明共享时只返回该站点独占的通道；没有独占登记就不返回任何通道。
 */
export function resolveView({ registry, siteLimits }, site) {
  if (!siteLimits.has(site)) throw new Error('未知站点: ' + site);
  const links = [];
  for (const channel of registry.values()) {
    if (!channel.aliases.has(site)) continue;
    if (channel.aliases.size > 1 && !channel.proof) continue;
    links.push({
      id: channel.id,
      capacity: channel.capacity,
      proof: channel.proof,
      sharedWith: [...channel.aliases].filter(alias => alias !== site).sort(),
      ...(channel.sessionUrl ? { url: channel.sessionUrl } : {})
    });
  }
  links.sort((left, right) => left.id.localeCompare(right.id));
  return { site, channels: links, proof: links.map(link => link.proof).filter(Boolean).join(',') || null };
}

/**
 * 从本地源的实际响应构建登记。连接地址由调用方提供：
 * 源端只说“哪些站点共用同一个逻辑通道”，不说“地址相同就能共用”。
 */
export function registryFromSnapshot(snapshot, { sessionUrl, urls = {} } = {}) {
  const { siteLimits, registry } = createRegistry(snapshot);
  for (const channel of registry.values()) {
    channel.sessionUrl = urls[channel.id] ?? sessionUrl ?? null;
  }
  return { siteLimits, registry };
}

export function channelView(registry) {
  return [...registry.values()].sort((left, right) => left.id.localeCompare(right.id)).map(channel => ({
    id: channel.id,
    capacity: channel.capacity,
    proof: channel.proof,
    aliases: [...channel.aliases].sort(),
    state: channel.state,
    sessionUrl: channel.sessionUrl
  }));
}

/**
 * 可变的连接登记：别名迁移、容量调整都通过明确操作修改。
 * 每个操作带 reason，供审计与演示区分“来源”。
 */
export class RegistryBook {
  constructor(data) {
    const { siteLimits, registry } = createRegistry(data);
    this.siteLimits = siteLimits;
    this.registry = registry;
    this.audit = [];
    for (const channel of this.registry.values()) channel.aliases = new Set(channel.aliases);
  }

  export() {
    return {
      sites: [...this.siteLimits].map(([id, limit]) => ({ id, limit })),
      channels: channelView(this.registry)
    };
  }

  resolve(site) {
    return resolveView(this, site);
  }

  updateCapacity(id, capacity, reason = 'manual') {
    const channel = this.registry.get(id);
    if (!channel) throw new Error('未知通道: ' + id);
    if (!Number.isInteger(capacity) || capacity < 1) throw new Error('容量必须是正整数');
    // 登记容量只是来源信息；真实上限由会话 SETTINGS 决定，改变登记不会放开任何窗口。
    channel.capacity = capacity;
    this.audit.push({ at: new Date().toISOString(), action: 'capacity', channel: id, capacity, reason });
  }

  /**
   * 别名迁移：from 站点在通道间移动。只改变登记，不碰正在进行的流；
   * 排队作业由池重新解析后进入新通道的队列。
   */
  migrate(site, { from, to, proof, reason = 'manual' } = {}) {
    if (!this.siteLimits.has(site)) throw new Error('未知站点: ' + site);
    const source = this.registry.get(from);
    const target = this.registry.get(to);
    if (!source || !target) throw new Error('迁移涉及未知通道');
    if (!source.aliases.has(site)) throw new Error(`${site} 不在通道 ${from} 上`);
    if (to !== from) {
      const others = [...target.aliases].filter(alias => alias !== site);
      const nextProof = proof ?? target.proof;
      if (others.length > 0 && !nextProof) throw new Error('目标通道没有共享证明，拒绝迁移');
      if (target.proof && nextProof !== target.proof) throw new Error('目标通道的共享证明不一致');
      target.aliases.add(site);
      target.proof = nextProof ?? null;
      source.aliases.delete(site);
    }
    this.audit.push({ at: new Date().toISOString(), action: 'migrate', site, from, to, proof: proof ?? null, reason });
    return this.resolve(site);
  }
}
