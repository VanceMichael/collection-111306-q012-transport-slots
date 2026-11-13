import { createServer } from 'node:http2';

export async function startTransport(capacity = 2) {
  const sessions = new Set();
  const events = [];
  const timers = new Set();
  const server = createServer({ settings: { maxConcurrentStreams: capacity } });
  server.on('session', session => { sessions.add(session); session.on('error', () => {}); session.on('close', () => sessions.delete(session)); });
  server.on('stream', (stream, headers) => {
    const id = headers['x-request-id'] ?? String(stream.id);
    events.push({ id, event: 'accepted' });
    stream.on('error', () => {});
    stream.on('close', () => {
      const timer = setTimeout(() => { events.push({ id, event: 'released' }); timers.delete(timer); }, 30);
      timers.add(timer);
    });
    stream.respond({ ':status': 200, 'content-type': 'application/octet-stream' });
    stream.write(Buffer.alloc(16384, 65));
    if (headers[':path'] !== '/hold') stream.end('完成');
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`, events,
    resize: value => { for (const session of sessions) session.settings({ maxConcurrentStreams: value }); },
    close: async () => { for (const timer of timers) clearTimeout(timer); for (const session of sessions) session.destroy(); await new Promise(resolve => server.close(resolve)); }
  };
}
