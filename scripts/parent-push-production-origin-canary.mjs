#!/usr/bin/env node

import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";

const RELEASE_SHA = "23052e2e82b418969256c0599b5b02228b4c56bb";
const TTL_SECONDS = 24 * 60 * 60;
const URGENCY = "high";
const POLL_MS = 2_000;
const TIMEOUT_MS = 12 * 60_000;

function required(value, name) {
  const normalized = String(value || "").trim();
  if (!normalized) throw new Error(`${name} is required.`);
  return normalized;
}

function jsonRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function diagnosticsStages(metadata) {
  const diagnostics = jsonRecord(jsonRecord(metadata).parentPushDiagnostics);
  return jsonRecord(diagnostics.stages);
}

function safeFingerprint(value) {
  return createHash("sha256").update(String(value)).digest("hex").slice(0, 16);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function dbTimestamp(value) {
  return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
}

const rcDir = path.resolve(required(process.env.CENTRIX_RC_DIR, "CENTRIX_RC_DIR"));
const outputPath = path.resolve(required(process.env.CANARY_EVIDENCE_PATH, "CANARY_EVIDENCE_PATH"));
const qaCenterName = required(process.env.QA_CENTER_NAME, "QA_CENTER_NAME");
const databaseUrl = required(process.env.DATABASE_URL, "DATABASE_URL");
const vapidPublicKey = required(process.env.WEB_PUSH_VAPID_PUBLIC_KEY, "WEB_PUSH_VAPID_PUBLIC_KEY");
const vapidPrivateKey = required(process.env.WEB_PUSH_VAPID_PRIVATE_KEY, "WEB_PUSH_VAPID_PRIVATE_KEY");
const vapidSubject = required(process.env.WEB_PUSH_VAPID_SUBJECT, "WEB_PUSH_VAPID_SUBJECT");

if (process.env.RELEASE_SHA !== RELEASE_SHA) {
  throw new Error("Release SHA mismatch; refusing Production-origin Canary send.");
}
if (!qaCenterName.toLowerCase().includes("qa")) {
  throw new Error("QA center name must contain QA; refusing a non-QA target.");
}

const requireFromRc = createRequire(path.join(rcDir, "package.json"));
const { PrismaClient } = requireFromRc("@prisma/client");
const webpush = requireFromRc("web-push");
const { buildParentPushPayload } = await import(
  pathToFileURL(path.join(rcDir, "scripts", "notifications-worker-runtime.mjs")).href
);
const prisma = new PrismaClient({ datasources: { db: { url: databaseUrl } } });
const evidence = {
  releaseSha: RELEASE_SHA,
  test: "production-origin-closed-pwa",
  centerClass: "qa-only",
  provider: null,
  device: null
};

try {
  const [clock] = await prisma.$queryRaw`
    SELECT transaction_timestamp() AS "dbNow", current_database() AS "databaseName"
  `;
  if (!clock?.dbNow) throw new Error("PostgreSQL server time is unavailable.");

  const centers = await prisma.center.findMany({
    where: { name: qaCenterName, isArchived: false },
    select: { id: true, name: true }
  });
  if (centers.length !== 1) {
    throw new Error(`Expected exactly one QA center; found ${centers.length}.`);
  }
  const center = centers[0];
  const subscriptions = await prisma.parentPortalPushSubscription.findMany({
    where: {
      disabledAt: null,
      deviceSession: { studentLinks: { some: { tenantId: center.id } } }
    },
    select: {
      id: true,
      endpoint: true,
      endpointHash: true,
      p256dh: true,
      auth: true,
      expirationTime: true,
      deviceSessionId: true,
      createdAt: true
    },
    orderBy: { createdAt: "desc" }
  });
  if (subscriptions.length !== 1) {
    throw new Error(`Expected exactly one active Android QA subscription; found ${subscriptions.length}.`);
  }
  const subscription = subscriptions[0];
  if (safeFingerprint(subscription.endpoint) !== subscription.endpointHash.slice(0, 16)) {
    throw new Error("QA subscription endpoint fingerprint mismatch.");
  }

  const deliveries = await prisma.notificationDelivery.findMany({
    where: {
      tenantId: center.id,
      channel: "PUSH",
      eventType: "ATTENDANCE_LATE",
      status: "SENT",
      pushSubscriptionId: subscription.id,
      parentPortalNotificationId: { not: null }
    },
    select: {
      id: true,
      recipientAddress: true,
      metadata: true,
      sentAt: true,
      parentPortalNotification: {
        select: {
          id: true,
          route: true,
          readAt: true,
          deviceSessionId: true
        }
      }
    },
    orderBy: { sentAt: "desc" },
    take: 40
  });
  const delivery = deliveries.find((candidate) => {
    const notification = candidate.parentPortalNotification;
    const stages = diagnosticsStages(candidate.metadata);
    return notification
      && notification.readAt === null
      && Object.keys(stages).length === 0
      && /^\/parent\/student\/[^/]+\/attendance(?:\?[^#]*)?$/.test(notification.route);
  });
  if (!delivery?.parentPortalNotification) {
    throw new Error("No unread ACK-free QA ATTENDANCE_LATE notification is available.");
  }
  if (delivery.recipientAddress !== subscription.endpointHash) {
    throw new Error("QA delivery does not belong to the active subscription.");
  }

  const notification = delivery.parentPortalNotification;
  const unreadCount = await prisma.parentPortalNotification.count({
    where: { deviceSessionId: notification.deviceSessionId, readAt: null }
  });
  const payload = JSON.stringify(buildParentPushPayload({
    notificationId: notification.id,
    eventType: "ATTENDANCE_LATE",
    route: notification.route,
    unreadCount
  }));
  const parsedPayload = JSON.parse(payload);
  if (parsedPayload.title !== "تحديث الحضور" || parsedPayload.body !== "تم تحديث سجل الحضور في Centrix.") {
    throw new Error("Candidate browser-safe attendance copy mismatch.");
  }

  const pushSubscription = {
    endpoint: subscription.endpoint,
    expirationTime: subscription.expirationTime?.getTime() ?? null,
    keys: { p256dh: subscription.p256dh, auth: subscription.auth }
  };
  const workerOptions = { TTL: TTL_SECONDS, urgency: URGENCY };
  const directReferenceOptions = { TTL: 60, urgency: URGENCY };
  const generateSafeEnvelope = (options) => {
    const details = webpush.generateRequestDetails(pushSubscription, payload, {
      vapidDetails: {
        subject: vapidSubject,
        publicKey: vapidPublicKey,
        privateKey: vapidPrivateKey
      },
      ...options
    });
    return {
      method: details.method,
      provider: new URL(details.endpoint).hostname.includes("google") ? "FCM" : "other",
      contentEncoding: details.headers["Content-Encoding"],
      ttl: Number(details.headers.TTL),
      urgency: details.headers.Urgency,
      topicPresent: Boolean(details.headers.Topic),
      payloadBytes: Buffer.byteLength(payload, "utf8"),
      encryptedBodyBytes: details.body?.length ?? 0,
      authorizationScheme: String(details.headers.Authorization || "").split(" ")[0] || null
    };
  };

  evidence.dbNow = dbTimestamp(clock.dbNow);
  evidence.deliveryId = delivery.id;
  evidence.notificationId = notification.id;
  evidence.subscriptionFingerprint = subscription.endpointHash.slice(0, 16);
  evidence.vapidPublicFingerprint = safeFingerprint(vapidPublicKey);
  evidence.vapidSubject = vapidSubject;
  evidence.routeKind = "student-attendance";
  evidence.workerEnvelope = generateSafeEnvelope(workerOptions);
  evidence.directReferenceEnvelope = generateSafeEnvelope(directReferenceOptions);

  webpush.setVapidDetails(vapidSubject, vapidPublicKey, vapidPrivateKey);
  const providerRequestStartedAt = new Date();
  const providerStartedMonotonic = performance.now();
  const response = await webpush.sendNotification(pushSubscription, payload, workerOptions);
  const providerAcceptedAt = new Date();
  evidence.provider = {
    statusCode: Number(response?.statusCode || 201),
    requestStartedAt: providerRequestStartedAt.toISOString(),
    acceptedAt: providerAcceptedAt.toISOString(),
    latencyMs: Math.round((performance.now() - providerStartedMonotonic) * 100) / 100
  };
  process.stdout.write(`${JSON.stringify({
    event: "production_origin_canary.provider_accepted",
    releaseSha: RELEASE_SHA,
    deliveryId: delivery.id,
    subscriptionFingerprint: evidence.subscriptionFingerprint,
    workerEnvelope: evidence.workerEnvelope,
    directReferenceEnvelope: evidence.directReferenceEnvelope,
    provider: evidence.provider
  })}\n`);

  const deadline = Date.now() + TIMEOUT_MS;
  let chainComplete = false;
  while (Date.now() < deadline) {
    const current = await prisma.notificationDelivery.findUnique({
      where: { id: delivery.id },
      select: {
        metadata: true,
        parentPortalNotification: { select: { readAt: true, route: true } }
      }
    });
    const stages = diagnosticsStages(current?.metadata);
    evidence.device = {
      stages,
      readAt: current?.parentPortalNotification?.readAt?.toISOString() || null,
      routeKind: "student-attendance"
    };
    if (stages.showNotification_failed) {
      await writeFile(outputPath, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
      throw new Error("Service Worker reported showNotification_failed.");
    }
    const requiredStages = ["received", "payload_decoded", "showNotification_resolved", "notificationclick"];
    if (requiredStages.every((stage) => Boolean(stages[stage]))) {
      if (!current?.parentPortalNotification?.readAt) {
        throw new Error("notificationclick was ACKed but readAt was not updated.");
      }
      if (current.parentPortalNotification.route !== notification.route) {
        throw new Error("Notification route changed during the Canary.");
      }
      const receivedAt = Date.parse(stages.received.timestamp);
      const shownAt = Date.parse(stages.showNotification_resolved.timestamp);
      evidence.metrics = {
        deliveryToProviderMs: null,
        deliveryToProviderStatus: "NOT_MEASURED_EXISTING_QA_DELIVERY",
        canaryRequestToProviderMs: evidence.provider.latencyMs,
        providerToReceivedMs: receivedAt - providerAcceptedAt.getTime(),
        receivedToShowNotificationMs: shownAt - receivedAt,
        providerToShowNotificationMs: shownAt - providerAcceptedAt.getTime()
      };
      await writeFile(outputPath, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
      process.stdout.write(`${JSON.stringify({
        event: "production_origin_canary.ack_chain_complete",
        stages: requiredStages,
        readAtUpdated: true,
        metrics: evidence.metrics
      })}\n`);
      chainComplete = true;
      process.exitCode = 0;
      break;
    }
    await sleep(POLL_MS);
  }
  if (!chainComplete) {
    await writeFile(outputPath, `${JSON.stringify(evidence, null, 2)}\n`, { mode: 0o600 });
    throw new Error("Timed out waiting for the full device ACK and click chain.");
  }
} finally {
  await prisma.$disconnect();
}
