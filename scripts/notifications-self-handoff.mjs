#!/usr/bin/env node

import { fork, spawn } from "node:child_process";
import crypto from "node:crypto";
import { appendFile, writeFile, access } from "node:fs/promises";
import { constants } from "node:fs";
import { verifyRecoveryArtifact } from "./notifications-recovery-artifact.mjs";
import { CloudRuntimeChannel, directEvidence } from "./notifications-cloud-runtime.mjs";
import { lifecycleObserver, childProcessIds, stopRuntimeChild } from "./notifications-runtime-process.mjs";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";
import process from "node:process";
import { performance } from "node:perf_hooks";

const WORKER_STATUS_MESSAGE_TYPE = "centrix.notifications-worker.status.v1";
const LEADER_STATES = new Set(["leader", "leader-held"]);
const ACTIVE_RUN_STATES = new Set(["queued", "in_progress", "pending", "waiting", "requested"]);
const DEFAULT_GATE_STEP = "Wait for predecessor lease expiration";

function log(event, details = {}) {
  process.stdout.write(`${JSON.stringify({ event, ...details })}\n`);
}

function positiveNumber(value, fallback, name) {
  const parsed = Number(value ?? fallback);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive number.`);
  }
  return parsed;
}

function optionalHandoffCount(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized || normalized === "unlimited") return null;
  if (!/^\d+$/.test(normalized)) {
    throw new Error("SELF_HANDOFF_REMAINING must be a non-negative integer or unlimited.");
  }
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error("SELF_HANDOFF_REMAINING exceeds the supported integer range.");
  }
  return parsed;
}

function required(value, name) {
  const normalized = String(value || "").trim();
  if (!normalized) throw new Error(`${name} is required.`);
  return normalized;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function postgresTimestampMs(value) {
  const normalized = String(value || "").trim();
  if (!normalized) return Number.NaN;
  const withZone = /(?:Z|[+-]\d{2}:\d{2})$/i.test(normalized) ? normalized : `${normalized}Z`;
  return Date.parse(withZone);
}

function redactError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message
    .replace(/postgres(?:ql)?:\/\/[^\s"']+/gi, "[REDACTED_DATABASE_URL]")
    .replace(/Bearer\s+[^\s"']+/gi, "Bearer [REDACTED]");
}

export class GitHubActionsClient {
  constructor({
    token,
    repository,
    apiUrl = "https://api.github.com",
    fetchImpl = fetch,
    requestTimeoutMs = 15_000
  }) {
    this.token = required(token, "GH_TOKEN");
    this.repository = required(repository, "GITHUB_REPOSITORY");
    this.apiUrl = apiUrl.replace(/\/$/, "");
    this.fetchImpl = fetchImpl;
    this.requestTimeoutMs = requestTimeoutMs;
  }

  async request(endpoint, { method = "GET", body } = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    let response;
    try {
      response = await this.fetchImpl(`${this.apiUrl}${endpoint}`, {
        method,
        headers: {
          Accept: "application/vnd.github+json",
          Authorization: `Bearer ${this.token}`,
          "Content-Type": "application/json",
          "X-GitHub-Api-Version": "2026-03-10"
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal
      });
    } finally {
      clearTimeout(timeout);
    }
    const raw = await response.text();
    let payload = null;
    if (raw) {
      try {
        payload = JSON.parse(raw);
      } catch {
        payload = null;
      }
    }
    if (!response.ok) {
      throw new Error(`GitHub API ${method} ${endpoint} failed with HTTP ${response.status}.`);
    }
    return payload;
  }

  async dispatchSession({
    workflowFile,
    ref,
    sessionId,
    predecessorRunId = "",
    trigger,
    handoffsRemaining = null
  }) {
    const inputs = {
      session_id: sessionId,
      predecessor_run_id: String(predecessorRunId || ""),
      trigger
    };
    if (handoffsRemaining !== null) inputs.handoffs_remaining = String(handoffsRemaining);
    const payload = await this.request(
      `/repos/${this.repository}/actions/workflows/${encodeURIComponent(workflowFile)}/dispatches`,
      {
        method: "POST",
        body: {
          ref,
          inputs,
          return_run_details: true
        }
      }
    );
    const runId = Number(payload?.workflow_run_id);
    if (!Number.isSafeInteger(runId) || runId <= 0) {
      throw new Error("GitHub workflow dispatch did not return workflow_run_id; refusing an untraceable handoff.");
    }
    return { runId, runUrl: payload?.html_url || null };
  }

  async getRun(runId) {
    return this.request(`/repos/${this.repository}/actions/runs/${runId}`);
  }

  async getRunJobs(runId) {
    const payload = await this.request(
      `/repos/${this.repository}/actions/runs/${runId}/jobs?filter=latest&per_page=100`
    );
    return Array.isArray(payload?.jobs) ? payload.jobs : [];
  }

  async listSessionRuns(workflowFile) {
    const payload = await this.request(
      `/repos/${this.repository}/actions/workflows/${encodeURIComponent(workflowFile)}/runs?event=workflow_dispatch&per_page=100`
    );
    return Array.isArray(payload?.workflow_runs) ? payload.workflow_runs : [];
  }
}

export async function waitForOrchestrationReadiness({
  github,
  runId,
  expectedOrchestratorSha,
  gateStepName = DEFAULT_GATE_STEP,
  timeoutMs,
  pollMs,
  sleepImpl = sleep,
  now = () => Date.now(),
  isCancelled = () => false
}) {
  const deadline = now() + timeoutMs;
  while (now() < deadline) {
    if (isCancelled()) return { runId, orchestrationReady: false, cancelled: true };
    const run = await github.getRun(runId);
    if (run?.head_sha !== expectedOrchestratorSha) {
      throw new Error(`Successor orchestration SHA mismatch for run ${runId}.`);
    }
    if (run?.status === "completed") {
      throw new Error(`Successor run ${runId} completed before reaching the lease gate.`);
    }

    const jobs = await github.getRunJobs(runId);
    const gate = jobs.flatMap((job) => Array.isArray(job.steps) ? job.steps : [])
      .find((step) => step.name === gateStepName);
    if (gate?.status === "in_progress") {
      return { runId, gateStepName, orchestrationReady: true };
    }
    if (gate?.status === "completed") {
      throw new Error(`Successor run ${runId} passed the lease gate before predecessor shutdown.`);
    }
    await sleepImpl(pollMs);
  }
  if (isCancelled()) return { runId, orchestrationReady: false, cancelled: true };
  throw new Error(`Timed out waiting for successor run ${runId} to reach the lease gate.`);
}


function workerEnvironment(env) {
  if (!/^[0-9a-f]{40}$/.test(env.RELEASE_SHA || "")) throw new Error("An approved immutable RELEASE_SHA is required.");
  return {
    ...env,
    RELEASE_SHA: required(env.RELEASE_SHA, "RELEASE_SHA"),
    NOTIFICATIONS_WORKER_INSTANCE_ID: required(
      env.NOTIFICATIONS_WORKER_INSTANCE_ID
        || `gha-${required(env.GITHUB_RUN_ID, "GITHUB_RUN_ID")}-${env.GITHUB_RUN_ATTEMPT || "1"}`,
      "NOTIFICATIONS_WORKER_INSTANCE_ID"
    )
  };
}

export async function runWorkerSession({
  env = process.env,
  fetchImpl = fetch,
  forkImpl = fork,
  sleepImpl = sleep,
  monotonicNow = () => performance.now(),
  signalTarget = process
} = {}) {
  await verifyRecoveryArtifact({ env });
  const rcDir = path.resolve(required(env.CENTRIX_RC_DIR, "CENTRIX_RC_DIR"));
  const workerPath = path.join(rcDir, "scripts", "render-notifications-worker.mjs");
  const { probeCloudWorker } = await import(pathToFileURL(path.join(rcDir, "scripts", "whatsapp-cloud-health.mjs")));
  const handoffStartMs = positiveNumber(env.SELF_HANDOFF_START_MS, 285 * 60_000, "SELF_HANDOFF_START_MS");
  const targetStopMs = positiveNumber(env.SELF_HANDOFF_TARGET_MS, 300 * 60_000, "SELF_HANDOFF_TARGET_MS");
  const maximumStopMs = positiveNumber(env.SELF_HANDOFF_MAX_MS, 315 * 60_000, "SELF_HANDOFF_MAX_MS");
  const readinessTimeoutMs = positiveNumber(
    env.SELF_HANDOFF_READINESS_TIMEOUT_MS,
    25 * 60_000,
    "SELF_HANDOFF_READINESS_TIMEOUT_MS"
  );
  const pollMs = positiveNumber(env.SELF_HANDOFF_POLL_MS, 5_000, "SELF_HANDOFF_POLL_MS");
  const gracefulTimeoutMs = positiveNumber(
    env.SELF_HANDOFF_GRACEFUL_TIMEOUT_MS,
    5 * 60_000,
    "SELF_HANDOFF_GRACEFUL_TIMEOUT_MS"
  );
  if (!(handoffStartMs < targetStopMs && targetStopMs < maximumStopMs)) {
    throw new Error("Self-handoff timing must satisfy start < target < maximum.");
  }
  const handoffsRemaining = optionalHandoffCount(env.SELF_HANDOFF_REMAINING);

  const github = new GitHubActionsClient({
    token: env.GH_TOKEN,
    repository: env.GITHUB_REPOSITORY,
    apiUrl: env.GITHUB_API_URL,
    fetchImpl
  });
  const workflowFile = required(env.SELF_HANDOFF_WORKFLOW_FILE, "SELF_HANDOFF_WORKFLOW_FILE");
  const workflowRef = required(env.SELF_HANDOFF_WORKFLOW_REF, "SELF_HANDOFF_WORKFLOW_REF");
  const orchestratorSha = required(env.SELF_HANDOFF_ORCHESTRATOR_SHA, "SELF_HANDOFF_ORCHESTRATOR_SHA");
  const runId = required(env.GITHUB_RUN_ID, "GITHUB_RUN_ID");
  const runtimeExport = env.SELF_HANDOFF_RUNTIME_EXPORT_ENABLED === "true";
  const runtimeChannel = runtimeExport ? new CloudRuntimeChannel({ github, env }) : null;
  if (runtimeChannel) await runtimeChannel.initialize(); // Fail before child launch if checks permission is absent.
  const child = forkImpl(workerPath, [], {
    cwd: rcDir,
    env: { ...workerEnvironment(env), NOTIFICATIONS_WORKER_HTTP_HOST: "127.0.0.1" },
    stdio: ["ignore", "pipe", "pipe", "ipc"]
  });

  const lifecycle = lifecycleObserver(child);
  let workerPids = null;
  let retirementCommand = null;
  let shutdownAck = null;
  const stop = async (reason) => {
    if (shutdownAck) return shutdownAck;
    shutdownAck = await stopRuntimeChild(child, { gracefulTimeoutMs, reason, lifecycle, workerPids: workerPids || childProcessIds(child.pid) });
    log("shutdown.acknowledgment", shutdownAck);
    if (runtimeChannel?.evidence) {
      const completion = await runtimeChannel.finish(shutdownAck, retirementCommand);
      if (!completion.success) throw new Error("Runtime retirement acknowledgment failed; no successor bypass permitted.");
    }
    return shutdownAck;
  };

  let leaderStartedAt = null;
  let hadLeadership = false;
  let superseded = false;
  let initialStateResolve;
  let initialStateReject;
  const initialState = new Promise((resolve, reject) => {
    initialStateResolve = resolve;
    initialStateReject = reject;
  });
  let exitResolve;
  const workerExit = new Promise((resolve) => { exitResolve = resolve; });

  child.on("message", (message) => {
    if (message?.type !== WORKER_STATUS_MESSAGE_TYPE) return;
    if (LEADER_STATES.has(message.state)) {
      if (!hadLeadership) {
        hadLeadership = true;
        leaderStartedAt = monotonicNow();
        initialStateResolve({ state: message.state });
      }
      return;
    }
    if (message.state === "standby") {
      if (!hadLeadership) initialStateResolve({ state: "standby" });
      else superseded = true;
    }
  });
  child.once("error", (error) => initialStateReject(error));
  child.once("exit", (exitCode, signal) => {
    if (!hadLeadership) initialStateReject(new Error(`Worker exited before leadership (${exitCode ?? signal}).`));
    exitResolve({ exitCode, signal });
  });

  let externalSignal = null;
  let resolveExternalStop;
  const externalStop = new Promise((resolve) => { resolveExternalStop = resolve; });
  const requestExternalStop = (signal) => {
    if (externalSignal) return;
    externalSignal = signal;
    resolveExternalStop(signal);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  };
  const onSigterm = () => requestExternalStop("SIGTERM");
  const onSigint = () => requestExternalStop("SIGINT");
  signalTarget.once("SIGTERM", onSigterm);
  signalTarget.once("SIGINT", onSigint);

  try {
    const startupTimeoutMs = positiveNumber(env.SELF_HANDOFF_STARTUP_TIMEOUT_MS, 5 * 60_000, "SELF_HANDOFF_STARTUP_TIMEOUT_MS");
    let startupTimeout;
    const startupExpired = new Promise((_resolve, reject) => {
      startupTimeout = setTimeout(
        () => reject(new Error("Worker did not report leader or standby before startup timeout.")),
        startupTimeoutMs
      );
    });
    let firstState;
    try {
      firstState = await Promise.race([
        initialState,
        startupExpired,
        externalStop.then((signal) => ({ state: "external-stop", signal }))
      ]);
    } catch (error) {
      await stop(externalSignal || "session-stop");
      throw error;
    } finally {
      clearTimeout(startupTimeout);
    }
    if (firstState.state === "external-stop") {
      const stopped = await stop(externalSignal || "session-stop");
      return { outcome: "external-stop", signal: firstState.signal, forced: stopped.forced };
    }
    if (firstState.state === "standby") {
      const stopped = await stop(externalSignal || "session-stop");
      log("session.superseded", { runId, forced: stopped.forced });
      return { outcome: "superseded", hadLeadership: false, forced: stopped.forced };
    }

    log("session.leader", { runId, release: env.RELEASE_SHA });
    // Leadership heartbeat is insufficient. Probe the real wrapper on this runner.
    const healthTimeoutMs = positiveNumber(env.SELF_HANDOFF_HEALTH_TIMEOUT_MS, 60000, "SELF_HANDOFF_HEALTH_TIMEOUT_MS");
    const healthStartedAt = monotonicNow();
    let diagnostics;
    while (true) {
      try { diagnostics = await probeCloudWorker({ env, fetchImpl }); }
      catch { diagnostics = { healthy: false, live: 0, ready: 0 }; }
      if (diagnostics.healthy) break;
      if (externalSignal) return { outcome: "external-stop", signal: externalSignal };
      if (monotonicNow() - healthStartedAt >= healthTimeoutMs) throw new Error("Cloud worker readiness gate failed; outbound remains held.");
      await sleepImpl(Math.min(pollMs, 1000));
    }
    workerPids = childProcessIds(child.pid);
    if (runtimeChannel) await runtimeChannel.publish(directEvidence({ probe: diagnostics, env, wrapperPid: child.pid, workerPids }));
    log("session.readiness", diagnostics);
    // One machine-readable check annotation exposes the actual runner-local HTTP
    // result while a long-running session remains active; no public health port.
    if (env.GITHUB_ACTIONS === 'true') {
      process.stdout.write(`::notice title=Centrix verified readiness::${JSON.stringify(diagnostics)}\n`);
    }
    let lastHealthAt = monotonicNow();
    let lastRuntimeExportAt = monotonicNow();
    const runtimeExportMs = positiveNumber(env.SELF_HANDOFF_RUNTIME_EXPORT_MS, 15000, "SELF_HANDOFF_RUNTIME_EXPORT_MS");
    let handoffStarted = false;
    let orchestrationReady = false;
    let successorRunId = null;
    let handoffError = null;
    let handoffCancelled = false;

    const startHandoff = () => {
      handoffStarted = true;
      const sessionId = `${runId}-${crypto.randomUUID()}`;
      void (async () => {
        try {
          const dispatched = await github.dispatchSession({
            workflowFile,
            ref: workflowRef,
            sessionId,
            predecessorRunId: runId,
            trigger: "handoff",
            handoffsRemaining: handoffsRemaining === null ? null : handoffsRemaining - 1
          });
          successorRunId = dispatched.runId;
          log("handoff.dispatched", { runId, successorRunId });
          const readiness = await waitForOrchestrationReadiness({
            github,
            runId: successorRunId,
            expectedOrchestratorSha: orchestratorSha,
            timeoutMs: readinessTimeoutMs,
            pollMs,
            sleepImpl,
            isCancelled: () => handoffCancelled
          });
          if (readiness.cancelled || handoffCancelled) return;
          orchestrationReady = true;
          log("handoff.orchestration_ready", { runId, successorRunId });
        } catch (error) {
          if (handoffCancelled) return;
          handoffError = redactError(error);
          log("handoff.failed", { runId, error: handoffError });
        }
      })();
    };

    while (true) {
      if (externalSignal) {
        handoffCancelled = true;
        const stopped = await stop(externalSignal);
        return { outcome: externalSignal === "controlled-retire" ? "controlled-retire" : "external-stop", signal: externalSignal, forced: stopped.forced, shutdown: stopped };
      }
      const elapsedMs = monotonicNow() - leaderStartedAt;
      if (monotonicNow() - lastHealthAt >= pollMs) {
        diagnostics = await probeCloudWorker({ env, fetchImpl });
        log("session.readiness", diagnostics);
        lastHealthAt = monotonicNow();
        if (!diagnostics.healthy) throw new Error("Cloud worker readiness degraded; coordinated stop required.");
      }
      if (runtimeChannel && monotonicNow() - lastRuntimeExportAt >= runtimeExportMs) {
        try {
          await runtimeChannel.publish(directEvidence({ probe: diagnostics, env, wrapperPid: child.pid, workerPids }));
          retirementCommand = await runtimeChannel.retirementRequest();
          lastRuntimeExportAt = monotonicNow();
          if (retirementCommand) {
            handoffCancelled = true;
            log("retirement.accepted", { requestId: retirementCommand.request.requestId, checkId: retirementCommand.checkId });
            requestExternalStop("controlled-retire");
            continue;
          }
        } catch (error) {
          lastRuntimeExportAt = monotonicNow();
          log("runtime.export_failed", { error: redactError(error) });
        }
      }
      if (!handoffStarted && handoffsRemaining !== 0 && elapsedMs >= handoffStartMs) startHandoff();
      if (superseded) {
        handoffCancelled = true;
        const stopped = await stop(externalSignal || "session-stop");
        return { outcome: "superseded-after-leadership", hadLeadership: true, forced: stopped.forced };
      }
      if (elapsedMs >= targetStopMs && orchestrationReady) {
        handoffCancelled = true;
        const stopped = await stop(externalSignal || "session-stop");
        if (stopped.forced) throw new Error("Worker required SIGKILL during graceful handoff.");
        return {
          outcome: "handed-off",
          hadLeadership: true,
          successorRunId,
          orchestrationReady: true,
          forced: false
        };
      }
      if (elapsedMs >= targetStopMs && handoffsRemaining === 0) {
        const stopped = await stop(externalSignal || "session-stop");
        if (stopped.forced) throw new Error("Worker required SIGKILL while completing the terminal canary session.");
        return {
          outcome: "terminal-session-complete",
          hadLeadership: true,
          orchestrationReady: false,
          forced: false
        };
      }
      if (elapsedMs >= maximumStopMs) {
        handoffCancelled = true;
        const stopped = await stop(externalSignal || "session-stop");
        if (stopped.forced) throw new Error("Worker required SIGKILL at the maximum session boundary.");
        return {
          outcome: "maximum-boundary",
          hadLeadership: true,
          successorRunId,
          orchestrationReady,
          handoffError,
          forced: false
        };
      }

      const exit = await Promise.race([
        workerExit.then((value) => ({ type: "exit", value })),
        externalStop.then((signal) => ({ type: "external-stop", signal })),
        sleepImpl(Math.min(pollMs, 1_000)).then(() => ({ type: "tick" }))
      ]);
      if (exit.type === "external-stop" || (exit.type === "exit" && externalSignal)) {
        handoffCancelled = true;
        const stopped = await stop(externalSignal || "session-stop");
        return { outcome: "external-stop", signal: externalSignal, forced: stopped.forced };
      }
      if (exit.type === "exit") {
        handoffCancelled = true;
        throw new Error(`Worker exited unexpectedly while leading (${exit.value.exitCode ?? exit.value.signal}).`);
      }
    }
  } finally {
    // Covers failed HTTP/compatibility gates as well as ordinary handoff shutdown.
    if (!shutdownAck) await stop(externalSignal || "session-finalize");
    signalTarget.off("SIGTERM", onSigterm);
    signalTarget.off("SIGINT", onSigint);
  }
}

function runProcess(command, args, { cwd, env, timeoutMs }) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    const append = (current, chunk) => `${current}${chunk}`.slice(-32_768);
    child.stdout.on("data", (chunk) => { stdout = append(stdout, chunk); });
    child.stderr.on("data", (chunk) => { stderr = append(stderr, chunk); });
    const timeout = setTimeout(() => child.kill("SIGKILL"), timeoutMs);
    child.once("error", error => { clearTimeout(timeout); reject(error); });
    child.once("exit", (exitCode, signal) => {
      clearTimeout(timeout);
      resolve({ exitCode, signal, stdout, stderr });
    });
  });
}

export function validateLeaseProof(proof, leaseId) {
  if (proof?.protocol !== 'centrix.lease-gate.v1' || !['clear','blocked','transient','fatal'].includes(proof.state)) throw Error('LEASE_GATE_INVALID_OUTPUT: invalid verifier protocol/state');
  if (['transient','fatal'].includes(proof.state)) {
    if (typeof proof.code !== 'string' || typeof proof.message !== 'string') throw Error('LEASE_GATE_INVALID_OUTPUT: missing error code');
    if (proof.state === 'transient' && !['P1001','P1002','P1008','P1017','P2024','P2034','LEASE_GATE_PROBE_TIMEOUT'].includes(proof.code)) throw Error('LEASE_GATE_INVALID_OUTPUT: non-transient error classified for retry');
    return proof;
  }
  const s = proof.snapshot;
  if (!s || s.leaseId !== leaseId || !Number.isFinite(Date.parse(s.dbNow)) || typeof s.active !== 'boolean'
    || s.contractVersion !== 1 || s.outboundEnabled !== false
    || !['processing','ambiguousLocks','ambiguousCardAttempts'].every(k => Number.isSafeInteger(s[k]) && s[k] >= 0)
    || !['heartbeatAt','expiresAt'].every(k => s[k] === null || (typeof s[k] === 'string' && Number.isFinite(Date.parse(s[k]))))
    || !((s.ownerId === null && s.epoch === null && s.expiresAt === null && s.heartbeatAt === null)
      || (typeof s.ownerId === 'string' && s.ownerId.length > 0 && Number.isSafeInteger(s.epoch) && s.epoch >= 0 && s.expiresAt !== null && s.heartbeatAt !== null))) throw Error('LEASE_GATE_INVALID_OUTPUT: incomplete schema or outbound proof');
  if (s.active !== (s.expiresAt !== null && Date.parse(s.expiresAt) > Date.parse(s.dbNow))) throw Error('LEASE_GATE_INVALID_OUTPUT: contradictory server-time expiry');
  const clear = !s.active && s.processing === 0 && s.ambiguousLocks === 0 && s.ambiguousCardAttempts === 0;
  if ((proof.state === 'clear') !== clear) throw Error('LEASE_GATE_INVALID_OUTPUT: contradictory in-flight proof');
  return proof;
}

export async function runLeaseGate({ env = process.env, sleepImpl = sleep, probeImpl = runProcess, logImpl = log } = {}) {
  await verifyRecoveryArtifact({ env });
  const rcDir = path.resolve(required(env.CENTRIX_RC_DIR, "CENTRIX_RC_DIR"));
  const leaseStatusScript = path.resolve(env.LEASE_STATUS_SCRIPT || path.join(path.dirname(fileURLToPath(import.meta.url)), 'notifications-worker-lease-status.mjs'));
  try { await access(leaseStatusScript, constants.R_OK); }
  catch { throw Error('LEASE_GATE_ARTIFACT: verifier executable missing/unreadable'); }
  const leaseId = String(env.NOTIFICATIONS_WORKER_LEASE_ID || 'notifications-worker');
  const gateTimeoutMs = positiveNumber(env.SELF_HANDOFF_GATE_TIMEOUT_MS, 25 * 60_000, 'SELF_HANDOFF_GATE_TIMEOUT_MS');
  const retryMs = positiveNumber(env.SELF_HANDOFF_GATE_RETRY_MS, 2000, 'SELF_HANDOFF_GATE_RETRY_MS');
  const renewalMs = positiveNumber(env.NOTIFICATIONS_WORKER_LEASE_RENEW_MS, 10000, 'NOTIFICATIONS_WORKER_LEASE_RENEW_MS');
  const probeTimeoutMs = positiveNumber(env.SELF_HANDOFF_GATE_PROBE_TIMEOUT_MS, 45000, 'SELF_HANDOFF_GATE_PROBE_TIMEOUT_MS');
  const maxDbErrors = positiveNumber(env.SELF_HANDOFF_GATE_MAX_DB_ERRORS, 3, 'SELF_HANDOFF_GATE_MAX_DB_ERRORS');
  const deadline = performance.now() + gateTimeoutMs;
  let attempts = 0, dbErrors = 0, first = null;
  const safeStderr = value => {
    let safe = redactError(String(value || ''));
    for (const [key, secret] of Object.entries(env)) if (/DATABASE_URL|TOKEN|SECRET|PRIVATE_KEY|ENCRYPTION_KEY/.test(key) && secret) safe = safe.replaceAll(String(secret), '[REDACTED]');
    return safe.slice(-1000);
  };
  while (performance.now() < deadline) {
    attempts++;
    let result;
    try { result = await probeImpl(process.execPath, [leaseStatusScript], { cwd: rcDir, env, timeoutMs: probeTimeoutMs }); }
    catch (error) { throw Error(`LEASE_GATE_ARTIFACT: verifier cannot execute (${error.code || error.name})`); }
    if (/MODULE_NOT_FOUND|ERR_MODULE_NOT_FOUND|Cannot find module|ENOENT/.test(result.stderr || '')) throw Error('LEASE_GATE_ARTIFACT: missing verifier dependency; ' + safeStderr(result.stderr));
    let proof;
    if (result.signal === 'SIGKILL' && !result.stdout) proof = { protocol: 'centrix.lease-gate.v1', state: 'transient', code: 'LEASE_GATE_PROBE_TIMEOUT', message: 'Database probe timed out' };
    else {
      try { proof = validateLeaseProof(JSON.parse(result.stdout.trim()), leaseId); }
      catch (error) { throw Error(`LEASE_GATE_INVALID_OUTPUT: ${safeStderr(error.message)}; stderr=${safeStderr(result.stderr)}`); }
      const expectedExit = { clear: 0, blocked: 2, transient: 3, fatal: [4,5] }[proof.state];
      if (!(Array.isArray(expectedExit) ? expectedExit.includes(result.exitCode) : result.exitCode === expectedExit)) throw Error('LEASE_GATE_INVALID_OUTPUT: verifier exit/status mismatch');
    }
    if (proof.state === 'fatal') throw Error(`LEASE_GATE_FATAL: ${proof.code}; ${safeStderr(result.stderr)}`);
    if (proof.state === 'transient') {
      first = null; dbErrors++;
      logImpl('lease_gate.db_retry', { attempts, dbErrors, code: proof.code, stderr: safeStderr(result.stderr) });
      if (dbErrors >= maxDbErrors) throw Error(`LEASE_GATE_DB_UNAVAILABLE: ${dbErrors} failed probes; last=${proof.code}`);
      await sleepImpl(retryMs); continue;
    }
    const s = proof.snapshot;
    if (proof.state === 'blocked') {
      first = null;
      logImpl('lease_gate.blocked', { attempts, dbNow: s.dbNow, active: s.active, processing: s.processing, ambiguousLocks: s.ambiguousLocks, ambiguousCardAttempts: s.ambiguousCardAttempts });
      await sleepImpl(retryMs); continue;
    }
    if (first && ['ownerId','epoch','heartbeatAt','expiresAt'].every(k => first[k] === s[k]) && Date.parse(s.dbNow) - Date.parse(first.dbNow) >= renewalMs) {
      logImpl('lease_gate.open', { attempts, evidence: [first, s] });
      return { open: true, attempts, evidence: [first, s] };
    }
    first = s;
    await sleepImpl(renewalMs);
  }
  throw Error('LEASE_GATE_TIMEOUT: no stable expired lease and zero ambiguous in-flight proof');
}

const WATCHDOG_SQL = String.raw`
WITH db AS (
  SELECT transaction_timestamp() AS db_now
), lease AS (
  SELECT "ownerId", "ownerLabel", "heartbeatAt", "expiresAt", "capabilities"
  FROM "NotificationWorkerLease"
  WHERE "id" = :'lease_id'
), processing AS (
  SELECT COUNT(*)::integer AS count
  FROM "NotificationDelivery"
  WHERE "status" = 'PROCESSING'
)
SELECT json_build_object(
  'dbNow', db.db_now,
  'active', COALESCE(lease."expiresAt" > db.db_now, false),
  'ownerId', lease."ownerId",
  'ownerLabel', lease."ownerLabel",
  'heartbeatAt', lease."heartbeatAt",
  'expiresAt', lease."expiresAt",
  'capabilities', lease."capabilities",
  'processing', processing.count
)::text
FROM db
CROSS JOIN processing
LEFT JOIN lease ON true;
`;

// Prisma-only URI options are not accepted by libpq. Adapt only the read-only
// psql probe URI; leave the worker's Prisma URL and pool limit untouched.
export function watchdogPsqlUrl(value) {
  const url = new URL(required(value, "DATABASE_URL"));
  if (!["postgres:", "postgresql:"].includes(url.protocol)) throw new Error("Unsupported watchdog database protocol");
  for (const key of ["connection_limit", "pool_timeout", "schema", "pgbouncer", "statement_cache_size", "socket_timeout"]) url.searchParams.delete(key);
  return url.href;
}

export async function readPostgresWatchdogSnapshot({ env = process.env, processImpl = runProcess } = {}) {
  const databaseUrl = watchdogPsqlUrl(env.DATABASE_URL);
  const leaseId = String(env.NOTIFICATIONS_WORKER_LEASE_ID || "notifications-worker").trim();
  if (!/^[A-Za-z0-9_.:-]+$/.test(leaseId)) {
    throw new Error("NOTIFICATIONS_WORKER_LEASE_ID contains unsupported characters.");
  }
  const psql = String(env.PSQL_BIN || "psql").trim();
  const sql = WATCHDOG_SQL.replace(":'lease_id'", `'${leaseId}'`);
  const result = await processImpl(psql, [
    `--dbname=${databaseUrl}`,
    "--no-psqlrc",
    "--set=ON_ERROR_STOP=1",
    "--tuples-only",
    "--no-align",
    "--command",
    sql
  ], {
    cwd: process.cwd(),
    env: { ...env, PGCONNECT_TIMEOUT: env.PGCONNECT_TIMEOUT || "10" },
    timeoutMs: positiveNumber(env.WATCHDOG_DB_TIMEOUT_MS, 30_000, "WATCHDOG_DB_TIMEOUT_MS")
  });
  if (result.exitCode !== 0) {
    throw new Error(`PostgreSQL watchdog query failed (${result.exitCode ?? result.signal}).`);
  }
  const line = result.stdout.trim().split("\n").filter(Boolean).at(-1);
  if (!line) throw new Error("PostgreSQL watchdog query returned no snapshot.");
  return JSON.parse(line);
}

export function evaluateWatchdog({
  snapshot,
  runs,
  expectedRelease,
  expectedOwnerLabel = "github-actions",
  candidateFreshnessMs = 20 * 60_000
}) {
  if (!/^[0-9a-f]{40}$/.test(expectedRelease || "")) throw new Error("An approved immutable RELEASE_SHA is required.");
  const dbNowMs = Date.parse(snapshot?.dbNow);
  if (!Number.isFinite(dbNowMs)) throw new Error("Watchdog snapshot is missing PostgreSQL dbNow.");
  if (snapshot.active) {
    const release = snapshot.capabilities?.release || null;
    if (snapshot.ownerLabel !== expectedOwnerLabel || release !== expectedRelease) {
      throw new Error("An active lease has an unexpected owner or release; watchdog is fail-closed.");
    }
    const wa = snapshot.capabilities?.whatsapp;
    const connectionAge = dbNowMs - Date.parse(wa?.lastSuccessfulConnectionPoll);
    const queueAge = dbNowMs - Date.parse(wa?.lastQueuePoll);
    if (wa?.contractVersion !== 1 || wa?.compatibility !== "compatible"
      || wa?.consecutiveConnectionPollErrors !== 0 || wa?.consecutiveQueuePollErrors !== 0
      || wa?.outboundEnabled !== false || !wa?.connectionPollSuccessCount || !wa?.queuePollSuccessCount
      || ['P2032', 'P2021', 'P2022'].some(code => wa?.errorCodeCounts?.[code] !== 0)
      || !Number.isFinite(connectionAge) || connectionAge < 0 || connectionAge > 45000
      || !Number.isFinite(queueAge) || queueAge < 0 || queueAge > 45000) {
      // Fail visibly; do not dispatch a competing worker against an active lease.
      throw new Error("Active worker has degraded WhatsApp readiness; operator review required.");
    }
    return { action: "none", reason: "active-leader" };
  }
  if (Number(snapshot.processing) !== 0) {
    throw new Error("PROCESSING deliveries exist without an active leader; watchdog is fail-closed.");
  }

  const recentCandidate = (Array.isArray(runs) ? runs : []).find((run) => {
    if (!ACTIVE_RUN_STATES.has(run.status)) return false;
    const createdAtMs = Date.parse(run.created_at);
    return Number.isFinite(createdAtMs)
      && createdAtMs <= dbNowMs
      && dbNowMs - createdAtMs <= candidateFreshnessMs;
  });
  if (recentCandidate) {
    return { action: "none", reason: "recent-candidate", runId: recentCandidate.id };
  }
  return { action: "dispatch", reason: "no-leader-no-candidate" };
}

export async function runWatchdog({ env = process.env, fetchImpl = fetch } = {}) {
  const snapshot = await readPostgresWatchdogSnapshot({ env });
  const github = new GitHubActionsClient({
    token: env.GH_TOKEN,
    repository: env.GITHUB_REPOSITORY,
    apiUrl: env.GITHUB_API_URL,
    fetchImpl
  });
  const workflowFile = required(env.SELF_HANDOFF_WORKFLOW_FILE, "SELF_HANDOFF_WORKFLOW_FILE");
  const runs = await github.listSessionRuns(workflowFile);
  const decision = evaluateWatchdog({
    snapshot,
    runs,
    expectedRelease: required(env.RELEASE_SHA, "RELEASE_SHA"),
    expectedOwnerLabel: env.NOTIFICATIONS_WORKER_INSTANCE_LABEL || "github-actions",
    candidateFreshnessMs: positiveNumber(
      env.WATCHDOG_CANDIDATE_FRESHNESS_MS,
      20 * 60_000,
      "WATCHDOG_CANDIDATE_FRESHNESS_MS"
    )
  });
  if (decision.action === "none") {
    log("watchdog.noop", { reason: decision.reason, dbNow: snapshot.dbNow, runId: decision.runId || null });
    return decision;
  }

  const sessionId = `watchdog-${crypto.randomUUID()}`;
  const dispatched = await github.dispatchSession({
    workflowFile,
    ref: required(env.SELF_HANDOFF_WORKFLOW_REF, "SELF_HANDOFF_WORKFLOW_REF"),
    sessionId,
    trigger: "watchdog"
  });
  log("watchdog.dispatched", { runId: dispatched.runId, dbNow: snapshot.dbNow });
  return { action: "dispatch", reason: decision.reason, runId: dispatched.runId };
}

const CANARY_OBSERVER_SQL = String.raw`
WITH db AS (
  SELECT transaction_timestamp() AS db_now
), lease AS (
  SELECT "ownerId", "ownerLabel", "heartbeatAt", "expiresAt", "capabilities"
  FROM "NotificationWorkerLease"
  WHERE "id" = :'lease_id'
), counts AS (
  SELECT
    (SELECT COUNT(*)::integer FROM "NotificationDelivery" WHERE "status" = 'PROCESSING') AS processing,
    (SELECT COUNT(*)::integer FROM "NotificationDelivery" WHERE "status" = 'QUEUED') AS queued,
    (SELECT COUNT(*)::integer FROM "NotificationJob") AS jobs,
    (SELECT COUNT(*)::integer FROM "WhatsAppAuthState") AS auth_states
)
SELECT json_build_object(
  'dbNow', db.db_now,
  'activeLeaders', COUNT(lease.*) FILTER (WHERE lease."expiresAt" > db.db_now),
  'ownerId', MAX(lease."ownerId"),
  'ownerLabel', MAX(lease."ownerLabel"),
  'heartbeatAt', MAX(lease."heartbeatAt"),
  'expiresAt', MAX(lease."expiresAt"),
  'release', MAX(lease."capabilities"->>'release'),
  'trafficActive', COALESCE(BOOL_OR((lease."capabilities"->>'trafficActive')::boolean), false),
  'processing', MAX(counts.processing),
  'queued', MAX(counts.queued),
  'jobs', MAX(counts.jobs),
  'authStates', MAX(counts.auth_states)
)::text
FROM db
CROSS JOIN counts
LEFT JOIN lease ON true
GROUP BY db.db_now;
`;

export async function readCanaryObserverSnapshot({ env = process.env } = {}) {
  const databaseUrl = required(env.DATABASE_URL, "DATABASE_URL");
  const leaseId = String(env.SELF_HANDOFF_OBSERVER_LEASE_ID || "notifications-worker").trim();
  if (!/^[A-Za-z0-9_.:-]+$/.test(leaseId)) {
    throw new Error("SELF_HANDOFF_OBSERVER_LEASE_ID contains unsupported characters.");
  }
  const sql = CANARY_OBSERVER_SQL.replace(":'lease_id'", `'${leaseId}'`);
  const result = await runProcess(String(env.PSQL_BIN || "psql").trim(), [
    `--dbname=${databaseUrl}`,
    "--no-psqlrc",
    "--set=ON_ERROR_STOP=1",
    "--tuples-only",
    "--no-align",
    "--command",
    sql
  ], {
    cwd: process.cwd(),
    env: { ...env, PGCONNECT_TIMEOUT: env.PGCONNECT_TIMEOUT || "10" },
    timeoutMs: positiveNumber(env.SELF_HANDOFF_OBSERVER_DB_TIMEOUT_MS, 30_000, "SELF_HANDOFF_OBSERVER_DB_TIMEOUT_MS")
  });
  if (result.exitCode !== 0) {
    throw new Error(`PostgreSQL canary observer query failed (${result.exitCode ?? result.signal}).`);
  }
  const line = result.stdout.trim().split("\n").filter(Boolean).at(-1);
  if (!line) throw new Error("PostgreSQL canary observer returned no snapshot.");
  return JSON.parse(line);
}

export async function runCanaryObserver({ env = process.env, sleepImpl = sleep } = {}) {
  const outputPath = path.resolve(required(env.SELF_HANDOFF_OBSERVER_OUTPUT, "SELF_HANDOFF_OBSERVER_OUTPUT"));
  const expectedRelease = required(env.RELEASE_SHA, "RELEASE_SHA");
  const expectedOwners = positiveNumber(env.SELF_HANDOFF_OBSERVER_EXPECTED_OWNERS, 2, "SELF_HANDOFF_OBSERVER_EXPECTED_OWNERS");
  const expectedHeartbeats = positiveNumber(
    env.SELF_HANDOFF_OBSERVER_EXPECTED_HEARTBEATS,
    4,
    "SELF_HANDOFF_OBSERVER_EXPECTED_HEARTBEATS"
  );
  const pollMs = positiveNumber(env.SELF_HANDOFF_OBSERVER_POLL_MS, 2_000, "SELF_HANDOFF_OBSERVER_POLL_MS");
  const timeoutMs = positiveNumber(env.SELF_HANDOFF_OBSERVER_TIMEOUT_MS, 25 * 60_000, "SELF_HANDOFF_OBSERVER_TIMEOUT_MS");
  const deadline = performance.now() + timeoutMs;
  const owners = new Map();
  const ownerOrder = [];
  let maximumActiveLeaders = 0;
  let finalZeroSamples = 0;
  let firstOwnerLastExpiresAt = null;
  let secondOwnerFirstDbNow = null;
  await writeFile(outputPath, "", { mode: 0o600 });

  while (performance.now() < deadline) {
    const snapshot = await readCanaryObserverSnapshot({ env });
    await appendFile(outputPath, `${JSON.stringify({ type: "snapshot", ...snapshot })}\n`);
    maximumActiveLeaders = Math.max(maximumActiveLeaders, Number(snapshot.activeLeaders || 0));
    if (maximumActiveLeaders > 1) throw new Error("Canary observed more than one active PostgreSQL lease leader.");
    for (const [name, value] of [
      ["PROCESSING", snapshot.processing],
      ["QUEUED", snapshot.queued],
      ["NotificationJob", snapshot.jobs],
      ["WhatsAppAuthState", snapshot.authStates]
    ]) {
      if (Number(value) !== 0) throw new Error(`Canary isolation violated: ${name} count became ${value}.`);
    }
    if (snapshot.trafficActive) throw new Error("Canary worker reported trafficActive=true.");

    if (Number(snapshot.activeLeaders) === 1) {
      if (snapshot.release !== expectedRelease) throw new Error("Canary leader release SHA mismatch.");
      if (snapshot.ownerLabel !== "github-actions") throw new Error("Canary leader owner label mismatch.");
      const ownerId = required(snapshot.ownerId, "canary ownerId");
      if (!/^gha-\d+-\d+:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(ownerId)) throw new Error(`Unexpected canary owner ID: ${ownerId}.`);
      if (!owners.has(ownerId)) {
        owners.set(ownerId, new Set());
        ownerOrder.push(ownerId);
      }
      if (ownerOrder.length > expectedOwners) throw new Error("Canary created more worker owners than expected.");
      if (snapshot.heartbeatAt) owners.get(ownerId).add(snapshot.heartbeatAt);
      if (ownerOrder.length === 1 && snapshot.expiresAt) firstOwnerLastExpiresAt = snapshot.expiresAt;
      if (ownerOrder.length === 2 && secondOwnerFirstDbNow === null) secondOwnerFirstDbNow = snapshot.dbNow;
      finalZeroSamples = 0;
    } else if (ownerOrder.length === expectedOwners) {
      finalZeroSamples += 1;
    }

    const enoughHeartbeats = ownerOrder.length === expectedOwners
      && ownerOrder.every((ownerId) => owners.get(ownerId).size >= expectedHeartbeats);
    if (enoughHeartbeats && finalZeroSamples >= 2) {
      const handoffGapMs = firstOwnerLastExpiresAt && secondOwnerFirstDbNow
        ? postgresTimestampMs(secondOwnerFirstDbNow) - postgresTimestampMs(firstOwnerLastExpiresAt)
        : null;
      const summary = {
        type: "summary",
        success: true,
        release: expectedRelease,
        owners: ownerOrder.map((ownerId) => ({ ownerId, heartbeatSamples: owners.get(ownerId).size })),
        maximumActiveLeaders,
        finalActiveLeaders: 0,
        finalProcessing: 0,
        handoffGapMs,
        firstOwnerLastExpiresAt,
        secondOwnerFirstDbNow
      };
      await appendFile(outputPath, `${JSON.stringify(summary)}\n`);
      log("canary.observer_complete", summary);
      return summary;
    }
    await sleepImpl(pollMs);
  }
  throw new Error("Canary observer timed out before proving two graceful lease owners and final zero leadership.");
}

async function main() {
  const command = process.argv[2];
  if (command === "session") return runWorkerSession();
  if (command === "gate") return runLeaseGate();
  if (command === "watchdog") return runWatchdog();
  if (command === "observer") return runCanaryObserver();
  throw new Error("Usage: notifications-self-handoff.mjs <session|gate|watchdog|observer>");
}

const isEntrypoint = process.argv[1]
  && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (isEntrypoint) {
  main().then(result => log("orchestrator.complete", result)).catch((error) => {
    process.stderr.write(`${JSON.stringify({ event: "orchestrator.failed", error: redactError(error) })}\n`);
    process.exitCode = 1;
  });
}
