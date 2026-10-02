import test from 'node:test';import assert from 'node:assert/strict';
import {watchdogPsqlUrl,readPostgresWatchdogSnapshot,evaluateWatchdog} from '../scripts/notifications-self-handoff.mjs';
test('read-only psql URI removes Prisma pool options, keeps TLS/libpq settings, and does not mutate Prisma config',()=>{
 const original='postgresql://fixture:fake@localhost:55484/test?sslmode=require&channel_binding=require&connection_limit=5&pool_timeout=10&schema=public&pgbouncer=true&statement_cache_size=0&socket_timeout=20&connect_timeout=10';
 const result=new URL(watchdogPsqlUrl(original));for(const k of ['connection_limit','pool_timeout','schema','pgbouncer','statement_cache_size','socket_timeout'])assert.equal(result.searchParams.has(k),false);
 assert.equal(result.searchParams.get('sslmode'),'require');assert.equal(result.searchParams.get('channel_binding'),'require');assert.equal(result.searchParams.get('connect_timeout'),'10');assert.equal(new URL(original).searchParams.get('connection_limit'),'5');
});
test('invalid/missing DB protocol fails closed before any probe',()=>{for(const v of ['', 'https://example.invalid/test'])assert.throws(()=>watchdogPsqlUrl(v))});
test('watchdog actual probe receives adapted URI and preserves healthy active-leader/no-op',async()=>{
 const release='a0d000a885bd9a4624e28956ed50e50dfff63436',now=new Date().toISOString(),env={DATABASE_URL:'postgresql://fixture:fake@localhost/test?sslmode=require&connection_limit=5'};let called=0;
 const snapshot={dbNow:now,active:true,ownerLabel:'github-actions',processing:0,capabilities:{release,whatsapp:{contractVersion:1,compatibility:'compatible',outboundEnabled:false,consecutiveConnectionPollErrors:0,consecutiveQueuePollErrors:0,connectionPollSuccessCount:5,queuePollSuccessCount:5,lastSuccessfulConnectionPoll:now,lastQueuePoll:now,errorCodeCounts:{P2032:0,P2021:0,P2022:0}}}};
 const s=await readPostgresWatchdogSnapshot({env,processImpl:async(bin,args)=>{called++;assert.equal(new URL(args[0].slice('--dbname='.length)).searchParams.has('connection_limit'),false);assert.match(args.at(-1),/SELECT json_build_object/);return{exitCode:0,stdout:JSON.stringify(snapshot)}}});
 assert.equal(called,1);assert.equal(new URL(env.DATABASE_URL).searchParams.get('connection_limit'),'5');assert.deepEqual(evaluateWatchdog({snapshot:s,expectedRelease:release,runs:[]}),{action:'none',reason:'active-leader'});
});
test('psql failure remains fail closed; no dispatch fallback',async()=>{await assert.rejects(readPostgresWatchdogSnapshot({env:{DATABASE_URL:'postgresql://fixture:fake@localhost/test?connection_limit=5'},processImpl:async()=>({exitCode:2,stdout:''})}),/watchdog query failed/)});
