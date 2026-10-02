import {fork} from'node:child_process';
const child=fork(new URL('./runtime-worker.mjs',import.meta.url),[],{stdio:['ignore','inherit','inherit','ipc']});
process.on('SIGTERM',()=>{console.log(JSON.stringify({event:'worker.service_stopping',signal:'SIGTERM'}));child.kill('SIGTERM')});
child.on('exit',(code,signal)=>{console.log(JSON.stringify({event:'worker.child_exit',code,signal}));process.exit(code??1)});
child.on('message',value=>{if(value.type==='test.worker_ready')console.log(JSON.stringify({event:'test.nested_ready',pid:child.pid}));});
