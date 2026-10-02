let stopping = false;
process.on('SIGTERM', () => {
  if (stopping) return;
  stopping = true;
  console.log(JSON.stringify({event:'worker.stopping',signal:'SIGTERM'}));
  setTimeout(() => {
    console.log(JSON.stringify({event:'lease.heartbeat_stopped'}));
    process.exit(0);
  }, Number(process.env.TEST_WORKER_STOP_MS || 30));
});
setInterval(() => {}, 1000);
process.send?.({type:'test.worker_ready'});
