import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRegistry, resolveView, RegistryBook, channelView } from '../src/topology.mjs';

const data = JSON.parse(await readFile(new URL('../fixtures/source.json', import.meta.url))).data;

test('连接信息只按显式登记解析，不从地址或站点名推断', () => {
  const registry = createRegistry(data);
  const a = resolveView(registry, 'supplier-a');
  const c = resolveView(registry, 'supplier-c');
  assert.deepEqual(a.channels.map(channel => channel.id), ['c1']);
  assert.deepEqual(a.channels[0].sharedWith, ['supplier-b']);
  assert.equal(a.channels[0].proof, 'local-registration-1');
  assert.deepEqual(c.channels.map(channel => channel.id), ['c2']);
  assert.deepEqual(c.channels[0].sharedWith, []);
});

test('未登记站点不获得任何通道', () => {
  const registry = createRegistry({
    sites: [{ id: 'known', limit: 1 }],
    channels: [{ id: 'c1', capacity: 1, aliases: ['known'], proof: 'p' }],
    shared: []
  });
  assert.deepEqual(resolveView(registry, 'known').channels.map(channel => channel.id), ['c1']);
  assert.throws(() => resolveView(registry, 'unknown'), /未知站点/);
});

test('共享登记必须带证明，且同一通道证明一致', () => {
  assert.throws(() => createRegistry({
    sites: [],
    channels: [],
    shared: [{ channel: 'c9', sites: ['x', 'y'], proof: null }]
  }), /证明不一致|缺少/);

  const registry = createRegistry({
    sites: [],
    shared: [{ channel: 'c9', sites: ['x', 'y'], proof: 'p9' }]
  });
  assert.equal(registry.registry.get('c9').proof, 'p9');
  assert.deepEqual([...registry.registry.get('c9').aliases].sort(), ['x', 'y']);
});

test('别名迁移需要目标通道证明，且可重复审计', () => {
  const book = new RegistryBook(structuredClone(data));
  const moved = book.migrate('supplier-b', { from: 'c1', to: 'c2', proof: 'local-registration-2', reason: 'rebalance' });
  assert.deepEqual(moved.channels.map(channel => channel.id), ['c2']);
  assert.deepEqual(channelView(book.registry).find(channel => channel.id === 'c1').aliases, ['supplier-a']);
  assert.throws(() => book.migrate('supplier-b', { from: 'c2', to: 'c1', proof: 'wrong' }), /证明不一致/);
  assert.equal(book.audit.filter(entry => entry.action === 'migrate').length, 1);
});

test('迁移不能把站点放到没有证明的共享通道上', () => {
  const book = new RegistryBook({
    sites: [{ id: 'x', limit: 1 }, { id: 'y', limit: 1 }],
    channels: [
      { id: 'c1', capacity: 1, aliases: ['x'], proof: 'p1' },
      { id: 'c2', capacity: 1, aliases: ['y'], proof: null }
    ]
  });
  assert.throws(() => book.migrate('x', { from: 'c1', to: 'c2', reason: 'test' }), /没有共享证明/);
  assert.ok(book.registry.get('c1').aliases.has('x'));
});
