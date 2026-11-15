import { createServer } from 'node:http2';

/**
 * 本地可控 h2c 通道。
 *
 * 服务端只负责“像供应商一样”给出真实流：
 * - SETTINGS.maxConcurrentStreams 可在线调整；
 * - GOAWAY/drain 停止接收新流，已在途的流可以自然结束；
 * - 流关闭后延迟发出 released 事件，表示供方额度并非立刻归还；
 * - /large 提供足够大的响应，供接收侧验证暂停读取时的占用；
 * - /hold 保持流打开，供取消与计数检查。
 *
 * 它不替接收器做配额或排队判断，接收器必须自己接入这些信号。
 */
export async function startTransport(capacity = 2, options = {}) {
  const releaseDelayMs = options.releaseDelayMs ?? 30;
  const chunkBytes = options.chunkBytes ?? 16384;
  const largeBytes = options.largeBytes ?? 2 * 1024 * 1024;
  const sessions = new Set();
  const events = [];
  const timers = new Set();
  let settingsCapacity = capacity;
  let activeStreams = 0;
  let peakActiveStreams = 0;
  let accepted = 0;
  let released = 0;
  let draining = false;
  const emit = event => {
    const record = { ...event, at: Date.now(), activeStreams, capacity: settingsCapacity };
    events.push(record);
    options.onEvent?.(record);
  };
  const initialCapacity = capacity;
  const server = createServer({ settings: { maxConcurrentStreams: capacity } });
  server.on('session', session => {
    sessions.add(session);
    session.on('error', () => {});
    session.on('close', () => sessions.delete(session));
    // 新会话也使用当前容量，避免“收缩只影响旧会话”的假象。
    // 未调整过时不重复发送 SETTINGS，保持一次连接一条初始设置。
    if (settingsCapacity !== initialCapacity) session.settings({ maxConcurrentStreams: settingsCapacity });
  });
  server.on('stream', (stream, headers) => {
    const id = headers['x-request-id'] ?? String(stream.id);
    const path = headers[':path'] ?? '/';
    accepted += 1;
    activeStreams += 1;
    peakActiveStreams = Math.max(peakActiveStreams, activeStreams);
    emit({ id, event: 'accepted', path });
    stream.on('error', () => {});
    stream.on('close', () => {
      activeStreams = Math.max(0, activeStreams - 1);
      const timer = setTimeout(() => {
        released += 1;
        emit({ id, event: 'released', path });
        timers.delete(timer);
      }, releaseDelayMs);
      timer.unref?.();
      timers.add(timer);
    });
    stream.respond({ ':status': 200, 'content-type': 'application/octet-stream' });
    if (path === '/hold') {
      stream.write(Buffer.alloc(chunkBytes, 65));
      return;
    }
    if (path === '/large') {
      const requested = Number(headers['x-size'] ?? largeBytes);
      const total = Number.isFinite(requested) && requested > 0 ? requested : largeBytes;
      let written = 0;
      const write = () => {
        if (stream.destroyed || stream.closed) return;
        try {
          while (written < total) {
            const size = Math.min(chunkBytes, total - written);
            written += size;
            if (!stream.write(Buffer.alloc(size, 66))) {
              stream.once('drain', write);
              return;
            }
          }
          stream.end('完成');
        } catch {
          // 客户端取消或连接关闭时写失败是正常路径。
        }
      };
      write();
      return;
    }
    stream.write(Buffer.alloc(chunkBytes, 65));
    stream.end('完成');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    events,
    get stats() {
      return {
        capacity: settingsCapacity,
        activeStreams,
        peakActiveStreams,
        accepted,
        released,
        sessions: sessions.size,
        draining
      };
    },
    get sessionCount() { return sessions.size; },
    resize(value) {
      if (!Number.isInteger(value) || value < 0) throw new Error('通道容量必须是大于等于 0 的整数');
      settingsCapacity = value;
      for (const session of sessions) {
        if (session.destroyed || session.closed) continue;
        session.settings({ maxConcurrentStreams: value });
      }
      emit({ event: 'settings', requestedCapacity: value });
    },
    drain() {
      draining = true;
      for (const session of sessions) {
        if (session.destroyed || session.closed) continue;
        session.close();
      }
      emit({ event: 'drain' });
    },
    close: async () => {
      for (const timer of timers) clearTimeout(timer);
      for (const session of sessions) {
        if (!session.destroyed) session.destroy();
      }
      await new Promise(resolve => server.close(resolve));
    }
  };
}
