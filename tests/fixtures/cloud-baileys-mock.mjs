import path from'node:path';import{pathToFileURL}from'node:url';
const mock=await import(pathToFileURL(path.join(process.env.CENTRIX_RC_DIR,'tests/fixtures/baileys-mock.mjs')));
const makeSocket=mock.default;
export const {BufferJSON,DisconnectReason,proto,initAuthCreds,fetchLatestBaileysVersion,fetchLatestWaWebVersion}=mock;
export default function(config){const socket=makeSocket(config),handlers=new Map(),originalOn=socket.ev.on,originalEnd=socket.end;
 socket.ev.on=(event,callback)=>{const list=handlers.get(event)||[];list.push(callback);handlers.set(event,list);return originalOn(event,callback)};
 socket.sendMessage=async()=>{console.log(JSON.stringify({event:'centrix.test.send_attempt'}));throw Error('No real or mock WhatsApp send permitted')};
 socket.end=async()=>{for(const callback of handlers.get('creds.update')||[])callback({registered:false,syntheticStale:true});for(const callback of handlers.get('connection.update')||[])callback({connection:'close',lastDisconnect:{error:{output:{statusCode:401}}}});console.log(JSON.stringify({event:'centrix.test.stale_callbacks_after_retire'}));await originalEnd();const delay=Number(process.env.CENTRIX_TEST_SOCKET_STOP_MS||0);if(delay)await new Promise(resolve=>setTimeout(resolve,delay));};
 return socket;
}
