#!/usr/bin/env node
// Orchestration-owned, read-only verifier. Uses the unchanged approved RC2 client/lease reader.
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { verifyRecoveryArtifact } from './notifications-recovery-artifact.mjs';
const protocol = 'centrix.lease-gate.v1';
let db;
try {
  await verifyRecoveryArtifact();
  const rc = path.resolve(process.env.CENTRIX_RC_DIR);
  const require = createRequire(path.join(rc, 'package.json'));
  const { PrismaClient } = require('@prisma/client');
  const { readPostgresLeaseStatus } = await import(pathToFileURL(path.join(rc, 'scripts/notifications-worker-postgres-lease.mjs')));
  const leaseId = process.env.NOTIFICATIONS_WORKER_LEASE_ID || 'notifications-worker';
  if (!process.env.DATABASE_URL || !/^[A-Za-z0-9_.:-]+$/.test(leaseId)) throw Object.assign(Error('Required database/lease configuration missing or invalid'), { code: 'LEASE_GATE_CONFIG' });
  db = new PrismaClient();
  const snapshot = await db.$transaction(async tx => {
    await tx.$executeRawUnsafe('SET TRANSACTION READ ONLY');
    await tx.$executeRawUnsafe("SET LOCAL statement_timeout='15000ms'");
    const status = await readPostgresLeaseStatus({ prisma: tx, leaseId });
    const lease = await tx.notificationWorkerLease.findUnique({ where: { id: leaseId }, select: { ownerId: true, epoch: true } });
    const [counts] = await tx.$queryRaw`
      SELECT
        (SELECT count(*)::int FROM "NotificationDelivery" WHERE status='PROCESSING') AS processing,
        (SELECT count(*)::int FROM "NotificationDelivery" WHERE "lockToken" IS NOT NULL OR "lockedAt" IS NOT NULL OR "processingStartedAt" IS NOT NULL) AS "ambiguousLocks",
        (SELECT count(*)::int FROM "CardRequestNotificationAttempt" WHERE status='UNKNOWN' OR (status='PENDING' AND "attemptedAt" IS NOT NULL)) AS "ambiguousCardAttempts"`;
    const control = await tx.whatsAppRuntimeControl.findUnique({ where: { id: 'whatsapp' } });
    return { dbNow: status.dbNow.toISOString(), leaseId, active: status.active, ownerId: lease?.ownerId || null, epoch: lease?.epoch ?? null,
      heartbeatAt: status.lease?.heartbeatAt.toISOString() || null, expiresAt: status.lease?.expiresAt.toISOString() || null,
      ...counts, contractVersion: control?.contractVersion, outboundEnabled: control?.outboundEnabled };
  }, { isolationLevel: 'RepeatableRead', timeout: 20000 });
  if (snapshot.contractVersion !== 1 || snapshot.outboundEnabled !== false) throw Object.assign(Error('Phase 1 contract or outbound hold invalid'), { code: 'LEASE_GATE_SCHEMA' });
  const blocked = snapshot.active || snapshot.processing !== 0 || snapshot.ambiguousLocks !== 0 || snapshot.ambiguousCardAttempts !== 0;
  console.log(JSON.stringify({ protocol, state: blocked ? 'blocked' : 'clear', snapshot }));
  process.exitCode = blocked ? 2 : 0;
} catch (error) {
  // Never print Prisma messages/query details: they can contain database credentials.
  const code = String(error.code || error.errorCode || (error.message?.startsWith('LEASE_GATE_ARTIFACT') ? 'LEASE_GATE_ARTIFACT' : 'LEASE_GATE_INVALID'));
  const transient = ['P1001','P1002','P1008','P1017','P2024','P2034'].includes(code);
  const artifact = ['MODULE_NOT_FOUND','ERR_MODULE_NOT_FOUND','ENOENT','EACCES','LEASE_GATE_ARTIFACT'].includes(code);
  const state = transient ? 'transient' : 'fatal';
  const message = artifact ? 'Required verifier/RC2 artifact or generated Prisma client missing' : transient ? 'Temporary database connection/transaction failure' : 'Verifier configuration, output or schema is incompatible';
  const output = { protocol, state, code, message };
  console.log(JSON.stringify(output)); console.error(JSON.stringify(output));
  process.exitCode = transient ? 3 : artifact ? 4 : 5;
} finally { await db?.$disconnect(); }
