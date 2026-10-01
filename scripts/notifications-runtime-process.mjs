import { execFileSync } from 'node:child_process';

export function childProcessIds(pid) {
  if (!Number.isSafeInteger(pid)) return null;
  try {
    const rows = execFileSync('ps', ['-axo', 'pid=,ppid='], { encoding: 'utf8', timeout: 2000 })
      .trim().split('\n').map(line => line.trim().split(/\s+/).map(Number));
    const found = new Set([pid]);
    for (let pass = 0; pass < 5; pass++) for (const [child, parent] of rows) if (found.has(parent)) found.add(child);
    return [...found].filter(value => value !== pid);
  } catch { return null; }
}

function alive(pid) {
  try { process.kill(pid, 0); return true; } catch (error) { return error.code !== 'ESRCH'; }
}

export function lifecycleObserver(child, { tee = true } = {}) {
  const events = [];
  const consume = stream => {
    if (!stream) return;
    let pending = '';
    stream.on('data', chunk => {
      if (tee) (stream === child.stdout ? process.stdout : process.stderr).write(chunk);
      pending = (pending + chunk).slice(-100_000);
      let index;
      while ((index = pending.indexOf('\n')) >= 0) {
        const line = pending.slice(0, index); pending = pending.slice(index + 1);
        let value;
        try { value = JSON.parse(line); } catch { continue; }
        if (['worker.stopping', 'worker.service_stopping', 'lease.heartbeat_stopped', 'worker.child_exit'].includes(value.event)) {
          events.push({ event: value.event, observedAt: new Date().toISOString(),
            signal: ['SIGTERM', 'SIGINT', 'SIGKILL'].includes(value.signal) ? value.signal : null,
            code: Number.isInteger(value.code) ? value.code : null });
        }
      }
    });
  };
  consume(child.stdout); consume(child.stderr);
  return { events };
}

export async function stopRuntimeChild(child, { gracefulTimeoutMs, reason, lifecycle = { events: [] }, workerPids = childProcessIds(child.pid) }) {
  const requestedAt = new Date().toISOString();
  const alreadyExited = child.exitCode !== null || child.signalCode !== null;
  const stopped = alreadyExited ? { forced: false, exitCode: child.exitCode, signal: child.signalCode } : await new Promise(resolve => {
    let forced = false;
    const timeout = setTimeout(() => { forced = true; if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); }, gracefulTimeoutMs);
    child.once('exit', (exitCode, signal) => { clearTimeout(timeout); resolve({ forced, exitCode, signal }); });
    child.kill('SIGTERM');
  });
  // Reap exit/data callbacks before recording the acknowledgment.
  await new Promise(resolve => setTimeout(resolve, 30));
  const orphans = (workerPids || []).filter(alive);
  const exit = lifecycle.events.filter(event => event.event === 'worker.child_exit').at(-1);
  return { reason, requestedAt, acknowledgedAt: new Date().toISOString(), signalForwarded: alreadyExited ? null : 'SIGTERM',
    alreadyExited, ...stopped, wrapperPid: child.pid, workerPids,
    processInventoryVerified: Array.isArray(workerPids) && workerPids.length > 0,
    noOrphans: Array.isArray(workerPids) && workerPids.length > 0 && orphans.length === 0, orphanPids: orphans,
    workerStoppingObserved: lifecycle.events.some(event => event.event === 'worker.stopping'),
    wrapperStoppingObserved: lifecycle.events.some(event => event.event === 'worker.service_stopping'),
    heartbeatStoppedObserved: lifecycle.events.some(event => event.event === 'lease.heartbeat_stopped'),
    wrapperWorkerExitCode: exit?.code ?? null, lifecycleEvents: lifecycle.events };
}
