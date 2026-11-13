import test from 'node:test';
import assert from 'node:assert/strict';
import {connect} from 'node:http2';
import {startTransport} from '../src/transport.mjs';
test('真实通道设置变化及流资源释放',async()=>{
 const source=await startTransport();const client=connect(source.url);
 try{await new Promise(resolve=>client.once('remoteSettings',resolve));assert.equal(client.remoteSettings.maxConcurrentStreams,2);
 const changed=new Promise(resolve=>client.once('remoteSettings',resolve));source.resize(1);await changed;assert.equal(client.remoteSettings.maxConcurrentStreams,1);
 const stream=client.request({':path':'/data','x-request-id':'r1'});let bytes=0;stream.on('data',chunk=>bytes+=chunk.length);await new Promise(resolve=>stream.once('end',resolve));assert.ok(bytes>16384);
 await new Promise(resolve=>setTimeout(resolve,60));assert.ok(source.events.some(value=>value.id==='r1'&&value.event==='released'));
 }finally{client.destroy();await source.close();}
});
