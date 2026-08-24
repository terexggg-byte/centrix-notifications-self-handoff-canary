import fs from "node:fs/promises";
import { PrismaClient } from "@prisma/client";
import { projectParentPortalNotification } from "../lib/server/parent-portal-notification-core.mjs";

const EXPECTED_RELEASE_SHA = "23052e2e82b418969256c0599b5b02228b4c56bb";
const TENANT_ID = "parent-push-release-gate-center";
const STUDENT_ID = "parent-push-release-gate-student";
const IDEMPOTENCY_KEY = "parent-push-candidate-23052:android-closed-app:attendance-late";
const LEASE_ID = "notifications-worker-parent-push-qa";
const mode = process.argv[2];
const prisma = new PrismaClient();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const msBetween = (from, to) => from && to
  ? new Date(to).getTime() - new Date(from).getTime()
  : null;

function required(name) {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required.`);
  return value;
}

async function clock() {
  const [row] = await prisma.$queryRawUnsafe(`
    SELECT current_setting('neon.branch_id', true) AS "branchId",
           transaction_timestamp() AS "dbNow"
  `);
  if (row?.branchId !== required("RELEASE_GATE_NEON_BRANCH_ID")) {
    throw new Error("Unexpected Neon branch; refusing final Android gate.");
  }
  return row;
}

async function seed() {
  if (required("RELEASE_SHA") !== EXPECTED_RELEASE_SHA) {
    throw new Error("Unexpected release SHA; refusing fixture generation.");
  }
  const dbClock = await clock();
  const [subscriptions, activeLeaders, processing, runnable, existing] = await Promise.all([
    prisma.parentPortalPushSubscription.count({
      where: {
        disabledAt: null,
        deviceSession: { studentLinks: { some: { tenantId: TENANT_ID, studentId: STUDENT_ID } } }
      }
    }),
    prisma.notificationWorkerLease.count({
      where: { id: LEASE_ID, expiresAt: { gt: dbClock.dbNow } }
    }),
    prisma.notificationDelivery.count({ where: { status: "PROCESSING" } }),
    prisma.notificationDelivery.count({ where: { status: { in: ["QUEUED", "RETRY"] } } }),
    prisma.notificationJob.findUnique({
      where: { tenantId_idempotencyKey: { tenantId: TENANT_ID, idempotencyKey: IDEMPOTENCY_KEY } },
      select: { id: true }
    })
  ]);

  if (subscriptions !== 1 || activeLeaders !== 0 || processing !== 0 || runnable !== 0 || existing) {
    throw new Error(
      `Unsafe final gate preflight: subscriptions=${subscriptions}, leaders=${activeLeaders}, ` +
      `processing=${processing}, runnable=${runnable}, existing=${Boolean(existing)}.`
    );
  }

  const job = await prisma.notificationJob.create({
    data: {
      tenantId: TENANT_ID,
      eventType: "ATTENDANCE_LATE",
      entityType: "PARENT_PUSH_FINAL_ANDROID_GATE",
      entityId: "attendance-late-candidate-23052",
      variables: { studentName: "طالب QA", lateDuration: "5 دقيقة" },
      recipientContext: { student: { id: STUDENT_ID, fullName: "طالب QA" } },
      idempotencyKey: IDEMPOTENCY_KEY
    }
  });
  const projection = await projectParentPortalNotification(prisma, job.id);
  if (projection.skipped) throw new Error("Final Android projection was skipped.");

  const deliveries = await prisma.notificationDelivery.findMany({
    where: { jobId: job.id },
    select: { id: true, channel: true, status: true, createdAt: true }
  });
  if (deliveries.length !== 1 || deliveries[0].channel !== "PUSH" || deliveries[0].status !== "QUEUED") {
    throw new Error(`Unexpected final delivery shape: ${deliveries.map((item) => `${item.channel}:${item.status}`).join(",")}.`);
  }

  process.stdout.write(`${JSON.stringify({
    event: "qa.final_android_gate.enqueued",
    dbNow: dbClock.dbNow.toISOString(),
    releaseSha: EXPECTED_RELEASE_SHA,
    jobId: job.id,
    deliveryId: deliveries[0].id,
    eventType: job.eventType,
    channel: deliveries[0].channel,
    status: deliveries[0].status
  })}\n`);
}

function safeStage(entry) {
  if (!entry || typeof entry !== "object") return null;
  return {
    timestamp: entry.timestamp ?? null,
    permission: entry.permission ?? null,
    registrationScope: entry.registrationScope ?? null,
    activeWorkerState: entry.activeWorkerState ?? null,
    waitingWorkerState: entry.waitingWorkerState ?? null,
    subscriptionFingerprint: typeof entry.subscriptionFingerprint === "string"
      ? entry.subscriptionFingerprint.slice(0, 16)
      : null
  };
}

async function snapshot(maxActiveLeaders) {
  const dbClock = await clock();
  const job = await prisma.notificationJob.findUnique({
    where: { tenantId_idempotencyKey: { tenantId: TENANT_ID, idempotencyKey: IDEMPOTENCY_KEY } },
    select: {
      id: true,
      createdAt: true,
      eventType: true,
      deliveries: {
        where: { channel: "PUSH" },
        select: {
          id: true,
          status: true,
          createdAt: true,
          sentAt: true,
          retryCount: true,
          metadata: true,
          parentPortalNotification: { select: { route: true, readAt: true } },
          pushSubscription: { select: { endpointHash: true } }
        }
      }
    }
  });
  const [activeLeaders, processing] = await Promise.all([
    prisma.notificationWorkerLease.count({
      where: { id: LEASE_ID, expiresAt: { gt: dbClock.dbNow } }
    }),
    prisma.notificationDelivery.count({ where: { status: "PROCESSING" } })
  ]);
  const observedMax = Math.max(maxActiveLeaders, activeLeaders);
  if (observedMax > 1) throw new Error("Split-brain detected during final Android gate.");
  const delivery = job?.deliveries[0];
  if (!job || !delivery) return { complete: false, maxActiveLeaders: observedMax, result: null };

  const metadata = delivery.metadata && typeof delivery.metadata === "object" ? delivery.metadata : {};
  const transport = metadata.pushTransport && typeof metadata.pushTransport === "object"
    ? metadata.pushTransport
    : {};
  const diagnostics = metadata.parentPushDiagnostics && typeof metadata.parentPushDiagnostics === "object"
    ? metadata.parentPushDiagnostics
    : {};
  const stages = diagnostics.stages && typeof diagnostics.stages === "object" ? diagnostics.stages : {};
  const received = stages.received;
  const decoded = stages.payload_decoded;
  const shown = stages.showNotification_resolved;
  const failed = stages.showNotification_failed;
  const clicked = stages.notificationclick;
  const providerAcceptedAt = transport.providerAcceptedAt || delivery.sentAt?.toISOString() || null;
  const route = delivery.parentPortalNotification?.route ?? null;
  const duplicateDeliveries = job.deliveries.length > 1 ? job.deliveries.length - 1 : 0;
  const result = {
    event: "qa.final_android_gate.evidence",
    dbNow: dbClock.dbNow.toISOString(),
    releaseSha: transport.releaseSha ?? null,
    jobId: job.id,
    deliveryId: delivery.id,
    eventType: job.eventType,
    status: delivery.status,
    providerStatus: transport.lastAttempt?.statusCode ?? null,
    retryCount: delivery.retryCount,
    duplicates: duplicateDeliveries,
    subscriptionFingerprint: delivery.pushSubscription?.endpointHash?.slice(0, 16) ?? null,
    route,
    readAt: delivery.parentPortalNotification?.readAt?.toISOString() ?? null,
    stages: {
      received: safeStage(received),
      payload_decoded: safeStage(decoded),
      showNotification_resolved: safeStage(shown),
      showNotification_failed: safeStage(failed),
      notificationclick: safeStage(clicked)
    },
    timingsMs: {
      deliveryToProvider: msBetween(delivery.createdAt, providerAcceptedAt),
      providerToReceived: msBetween(providerAcceptedAt, received?.timestamp),
      receivedToShowNotification: msBetween(received?.timestamp, shown?.timestamp),
      deliveryToShowNotification: msBetween(delivery.createdAt, shown?.timestamp)
    },
    activeLeaders,
    maxActiveLeaders: observedMax,
    processing
  };

  if (failed) throw new Error(`showNotification failed: ${failed.errorCode ?? "unknown"}.`);
  const routeIsCorrect = typeof route === "string" && route.includes(STUDENT_ID) && route.includes("attendance");
  const complete = delivery.status === "SENT"
    && Boolean(received)
    && Boolean(decoded)
    && Boolean(shown)
    && Boolean(clicked)
    && Boolean(delivery.parentPortalNotification?.readAt)
    && routeIsCorrect
    && duplicateDeliveries === 0
    && transport.releaseSha === EXPECTED_RELEASE_SHA;
  return { complete, maxActiveLeaders: observedMax, result };
}

async function observe() {
  if (required("RELEASE_SHA") !== EXPECTED_RELEASE_SHA) {
    throw new Error("Unexpected release SHA; refusing evidence collection.");
  }
  const timeoutMs = Number(process.env.QA_FINAL_GATE_TIMEOUT_MS || 900_000);
  const deadline = Date.now() + timeoutMs;
  let maxActiveLeaders = 0;
  let latest = null;
  while (Date.now() < deadline) {
    const sample = await snapshot(maxActiveLeaders);
    maxActiveLeaders = sample.maxActiveLeaders;
    latest = sample.result;
    if (sample.complete) {
      const outputPath = required("QA_FINAL_GATE_EVIDENCE_PATH");
      await fs.writeFile(outputPath, `${JSON.stringify(latest, null, 2)}\n`, { mode: 0o600 });
      process.stdout.write(`${JSON.stringify(latest)}\n`);
      return;
    }
    await sleep(2_000);
  }
  if (latest && process.env.QA_FINAL_GATE_EVIDENCE_PATH) {
    await fs.writeFile(process.env.QA_FINAL_GATE_EVIDENCE_PATH, `${JSON.stringify(latest, null, 2)}\n`, { mode: 0o600 });
  }
  throw new Error("Timed out before the final Android device ACK and click chain completed.");
}

try {
  if (mode === "seed") await seed();
  else if (mode === "observe") await observe();
  else throw new Error("Usage: parent-push-qa-final-android-gate.mjs <seed|observe>");
} finally {
  await prisma.$disconnect();
}
