import crypto from 'node:crypto';

export const EVIDENCE_CHECK = 'Centrix direct runtime evidence';
export const RETIRE_CHECK = 'Centrix controlled retirement';
export const RUNTIME_PROTOCOL = 'centrix.cloud-runtime.v1';

function integer(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`Invalid runtime ${name}`);
  return value;
}

function timing(metric, name) {
  if (!metric || !Number.isFinite(metric.lastLatencyMs) || metric.lastLatencyMs < 0
    || !Number.isFinite(metric.maxLatencyMs) || metric.maxLatencyMs < 0) throw new Error(`Invalid runtime ${name} timing`);
  return { successCount: integer(metric.successCount, `${name} successes`), errorCount: integer(metric.errorCount, `${name} errors`),
    lastLatencyMs: metric.lastLatencyMs, maxLatencyMs: metric.maxLatencyMs,
    lastSuccessAt: Number.isFinite(Date.parse(metric.lastSuccessAt)) ? metric.lastSuccessAt : null };
}

export function canaryEvidence(value) {
  if (value == null) return null;
  if (!['NOT_ARMED','READY','CONSUMED','SUBMITTED','SERVER_ACK','DELIVERED','READ','FAILED','UNKNOWN','EXPIRED'].includes(value.status)
    || value.canaryId !== 'qa-first-live-canary-20261002') throw Error('Invalid QA canary evidence');
  const out = { canaryId: value.canaryId, status: value.status };
  for (const key of ['providerAttemptCount','providerCallCount']) if (value[key] != null) {
    out[key] = integer(value[key], key); if (out[key]>1) throw Error('QA_CANARY_ATTEMPT_LIMIT_VIOLATION');
  }
  for (const key of ['epoch','generation']) if (value[key]!=null) out[key]=integer(value[key],key);
  for (const key of ['centerId','sessionId']) if (value[key]!=null) {
    const exact = key==='centerId'?'cmswc4i4f0071ytaago69eyu6':'cmswcsk2j000zfwwl1bmpmd3r';
    if(value[key]!==exact) throw Error('Invalid QA canary identity'); out[key]=value[key];
  }
  if (value.maskedRecipient!=null) { if(value.maskedRecipient!=='+20******2924') throw Error('Invalid recipient mask'); out.maskedRecipient=value.maskedRecipient; }
  for(const key of ['consumedAt','capabilityDisabledAt','providerAttemptedAt','submittedAt','ackAt','deliveredAt','readAt'])
    if(value[key]!=null){ if(!Number.isFinite(Date.parse(value[key]))) throw Error('Invalid canary time');out[key]=value[key]; }
  for(const key of ['providerMessageIdHash','attemptMessageIdHash']) if(value[key]!=null) {
    if(!/^[a-f0-9]{64}$/.test(value[key])) throw Error('Invalid canary ID hash');out[key]=value[key];
  }
  if(value.failureCode!=null){if(!/^[A-Z][A-Z0-9_]{2,80}$/.test(value.failureCode)) throw Error('Invalid canary failure code');out.failureCode=value.failureCode;}
  return out;
}

// Export only operational fields from runner-local HTTP, never raw response/error payloads.
export function directEvidence({ probe, env, wrapperPid, workerPids = [], at = new Date().toISOString() }) {
  const d = probe?.diagnostics;
  const owner = d?.leaseOwner;
  const prefix = `${env.NOTIFICATIONS_WORKER_INSTANCE_ID}:`;
  const uuid = typeof owner === 'string' && owner.startsWith(prefix) ? owner.slice(prefix.length) : '';
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(uuid)
    || d.workerRevision !== env.RELEASE_SHA || !/^[0-9a-f]{40}$/.test(env.SELF_HANDOFF_ORCHESTRATOR_SHA)
    || !Number.isSafeInteger(wrapperPid) || !Array.isArray(workerPids) || workerPids.length === 0) {
    throw new Error('Direct HTTP owner/revision binding invalid');
  }
  return {
    protocol: RUNTIME_PROTOCOL, kind: 'direct-http', observedAt: at,
    runId: String(env.GITHUB_RUN_ID), orchestrationSha: env.SELF_HANDOFF_ORCHESTRATOR_SHA,
    owner, epoch: integer(d.leaseEpoch, 'epoch'), processUuid: uuid,
    supervisorPid: process.pid, wrapperPid, workerPids,
    revision: d.workerRevision, source: 'runner-loopback-http-wrapper',
    liveHttpStatus: integer(probe.live, 'live'), readyHttpStatus: integer(probe.ready, 'ready'),
    healthy: probe.healthy === true, compatibility: d.compatibility === 'compatible' ? 'compatible' : 'incompatible',
    connectionPollSuccessCount: integer(d.connectionPollSuccessCount, 'connection successes'),
    connectionPollErrorCount: integer(d.whatsAppPollErrorCount, 'connection errors'),
    consecutiveConnectionPollErrors: integer(d.consecutivePollErrors, 'connection consecutive errors'),
    queuePollSuccessCount: integer(d.queuePollSuccessCount, 'queue successes'),
    queuePollErrorCount: integer(d.queuePollErrorCount, 'queue errors'),
    consecutiveQueuePollErrors: integer(d.consecutiveQueuePollErrors, 'queue consecutive errors'),
    lastSuccessfulConnectionPoll: d.lastSuccessfulConnectionPoll,
    lastQueuePoll: d.lastQueuePoll,
    errorCodeCounts: Object.fromEntries(['P2032', 'P2021', 'P2022'].map(code => [code, integer(d.errorCodeCounts?.[code], code)])),
    databaseErrorCounts: Object.fromEntries(['P2024', 'P2028', 'P2032', 'P2021', 'P2022'].map(code => [code, integer(d.databaseErrorCounts?.[code], code)])),
    leaseRenewal: timing(d.leaseRenewal, 'lease renewal'), pushPoll: timing(d.pushPoll, 'PUSH poll'),
    authWrites: Object.fromEntries(['active','pending','pendingKeys','maxActive','maxPending','completed','completedKeys','failed','staleRejected','transactionCount','concurrencyLimit','keysPerTransaction'].map(key => [key, integer(d.authWrites?.[key], `auth ${key}`)])),
    qaCanary: canaryEvidence(d.qaCanary),
    outboundEnabled: d.outboundEnabled === false ? false : true
  };
}

export function readCheckOutput(check) {
  try { return JSON.parse(check?.output?.summary || ''); }
  catch { throw new Error('Runtime check output is invalid JSON'); }
}

export function retirementMatches(request, evidence, now = Date.now()) {
  return request?.protocol === RUNTIME_PROTOCOL && request.action === 'retire'
    && request.targetRunId === evidence.runId && request.targetOwner === evidence.owner
    && request.targetEpoch === evidence.epoch && request.targetProcessUuid === evidence.processUuid
    && request.revision === evidence.revision && request.orchestrationSha === evidence.orchestrationSha
    && /^[0-9a-f-]{36}$/i.test(request.requestId || '')
    && Number.isFinite(Date.parse(request.createdAt)) && Date.parse(request.createdAt) <= now
    && Date.parse(request.expiresAt) > now && Date.parse(request.expiresAt) - Date.parse(request.createdAt) <= 300_000;
}

export class CloudRuntimeChannel {
  constructor({ github, env, now = () => new Date().toISOString() }) {
    Object.assign(this, { github, env, now });
    this.prefix = `/repos/${github.repository}`;
    this.evidenceId = null;
    this.evidence = null;
  }
  output(value) { return { title: EVIDENCE_CHECK, summary: JSON.stringify(value) }; }
  async initialize() {
    const initial = { protocol: RUNTIME_PROTOCOL, kind: 'starting', runId: String(this.env.GITHUB_RUN_ID),
      orchestrationSha: this.env.SELF_HANDOFF_ORCHESTRATOR_SHA, revision: this.env.RELEASE_SHA, observedAt: this.now() };
    const result = await this.github.request(`${this.prefix}/check-runs`, { method: 'POST', body: {
      name: EVIDENCE_CHECK, head_sha: this.env.SELF_HANDOFF_ORCHESTRATOR_SHA,
      external_id: `centrix-runtime:${this.env.GITHUB_RUN_ID}`, status: 'in_progress',
      started_at: this.now(), details_url: `https://github.com/${this.github.repository}/actions/runs/${this.env.GITHUB_RUN_ID}`,
      output: this.output(initial)
    } });
    if (!Number.isSafeInteger(result?.id)) throw new Error('Runtime evidence check creation failed');
    this.evidenceId = result.id;
  }
  async publish(evidence) {
    if (!this.evidenceId) throw new Error('Runtime evidence channel not initialized');
    await this.github.request(`${this.prefix}/check-runs/${this.evidenceId}`, {
      method: 'PATCH', body: { status: 'in_progress', output: this.output(evidence) }
    });
    this.evidence = evidence;
  }
  async retirementRequest() {
    if (!this.evidence) return null;
    const result = await this.github.request(`${this.prefix}/commits/${this.env.SELF_HANDOFF_ORCHESTRATOR_SHA}/check-runs?check_name=${encodeURIComponent(RETIRE_CHECK)}&filter=all&per_page=100`);
    for (const check of result?.check_runs || []) {
      if (check.status !== 'in_progress' || check.app?.slug !== 'github-actions') continue;
      let request;
      try { request = readCheckOutput(check); } catch { continue; }
      if (request.targetEvidenceCheckId === this.evidenceId && retirementMatches(request, this.evidence, Date.parse(this.now()))) {
        return { checkId: check.id, request };
      }
    }
    return null;
  }
  async finish(ack, command = null) {
    const value = { ...this.evidence, kind: 'shutdown-acknowledgment', observedAt: this.now(), shutdown: ack };
    const success = ack.exitCode === 0 && !ack.forced && ack.noOrphans && ack.workerStoppingObserved
      && ack.heartbeatStoppedObserved && ack.wrapperWorkerExitCode === 0;
    const body = { status: 'completed', conclusion: success ? 'success' : 'failure', completed_at: this.now(), output: this.output(value) };
    await this.github.request(`${this.prefix}/check-runs/${this.evidenceId}`, { method: 'PATCH', body });
    if (command) await this.github.request(`${this.prefix}/check-runs/${command.checkId}`, {
      method: 'PATCH', body: { ...body, output: { title: RETIRE_CHECK, summary: JSON.stringify({ ...command.request, acknowledgment: value }) } }
    });
    return { success, evidence: value };
  }
}

export async function requestRetirement({ github, targetCheckId, expectedOwner, expectedEpoch, expectedRevision, expectedOrchestration,
  timeoutMs = 120_000, pollMs = 1000, sleepImpl = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  const prefix = `/repos/${github.repository}`;
  const check = await github.request(`${prefix}/check-runs/${targetCheckId}`);
  const evidence = readCheckOutput(check);
  const observedMs = Date.parse(evidence.observedAt);
  if (check.name !== EVIDENCE_CHECK || check.status !== 'in_progress' || check.app?.slug !== 'github-actions'
    || check.head_sha !== expectedOrchestration || !Number.isFinite(observedMs) || observedMs > Date.now()
    || evidence.kind !== 'direct-http' || evidence.healthy !== true || evidence.liveHttpStatus !== 200
    || evidence.readyHttpStatus !== 200 || evidence.owner !== expectedOwner || evidence.epoch !== expectedEpoch
    || evidence.revision !== expectedRevision || evidence.orchestrationSha !== expectedOrchestration
    || evidence.outboundEnabled !== false || Date.now() - observedMs > 60_000) {
    throw new Error('Retirement refused: fresh direct HTTP evidence/owner/epoch binding required');
  }
  const createdMs = Date.now();
  const request = { protocol: RUNTIME_PROTOCOL, action: 'retire', requestId: crypto.randomUUID(),
    targetEvidenceCheckId: Number(targetCheckId), targetRunId: evidence.runId, targetOwner: evidence.owner,
    targetEpoch: evidence.epoch, targetProcessUuid: evidence.processUuid, revision: evidence.revision,
    orchestrationSha: evidence.orchestrationSha, createdAt: new Date(createdMs).toISOString(), expiresAt: new Date(createdMs + 300_000).toISOString() };
  const created = await github.request(`${prefix}/check-runs`, { method: 'POST', body: {
    name: RETIRE_CHECK, head_sha: expectedOrchestration, external_id: `centrix-retire:${request.requestId}`,
    status: 'in_progress', started_at: request.createdAt, output: { title: RETIRE_CHECK, summary: JSON.stringify(request) }
  } });
  if (!Number.isSafeInteger(created?.id)) throw new Error('Retirement request creation failed');
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const current = await github.request(`${prefix}/check-runs/${created.id}`);
    if (current.status === 'completed') {
      const payload = readCheckOutput(current), ack = payload.acknowledgment;
      if (current.conclusion !== 'success' || payload.requestId !== request.requestId
        || ack?.owner !== expectedOwner || ack.epoch !== expectedEpoch || ack.processUuid !== evidence.processUuid
        || ack.shutdown?.exitCode !== 0 || ack.shutdown?.forced || !ack.shutdown?.noOrphans) {
        throw new Error('Retirement failed or acknowledgment binding invalid');
      }
      return { commandCheckId: created.id, acknowledgment: ack };
    }
    await sleepImpl(pollMs);
  }
  throw new Error('Retirement acknowledgment timed out; do not cancel/requeue/bypass lease gate');
}
