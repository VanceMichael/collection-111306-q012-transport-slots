import {startSource} from './source.mjs';
import {startTransport} from './transport.mjs';
const port = Number(process.env.PORT ?? 0);
if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('PORT 必须是 0 至 65535 的整数');
const source=await startSource({ port });
// 本地复用通道与资料源分开监听：演示和测试都不连接真实供应商。
const transport=await startTransport(Number(process.env.CHANNEL_CAPACITY ?? 2));
console.log(JSON.stringify({本地资料源:source.url,本地通道:transport.url,通道容量:transport.stats.capacity},null,2));
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,async()=>{await transport.close();await source.close();process.exit(0);});
