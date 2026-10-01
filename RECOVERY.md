# RC2 notifications recovery

Application stays pinned to 953c5fe4d919251877f538f6f970d032b304b745.
No application build, migration, backlog replay or outbound enablement is part of this patch.

The old gate referenced an application CLI present only on another branch. The
orchestration now owns its read-only verifier, importing RC2's existing PostgreSQL
lease reader and generated Prisma client. The artifact manifest checks exact RC2
hashes for every entrypoint and required ownership/health module before gate/start.

The gate requires two expired, unchanged lease observations separated by at least
one configured renewal interval, using database timestamps. It rejects any
PROCESSING delivery, lockToken/lockedAt/processingStartedAt or ambiguous card send
attempt, and requires contractVersion=1 and outboundEnabled=false. Runtime lease
acquisition and all writes remain protected by RC2's existing fencing guards.

Exit/protocol contract: clear=0; blocked=2 (retry until deadline); transient=3
(maximum three failed probes); artifact=4 and schema/config=5 (immediate failure).
Malformed/contradictory JSON, missing dependencies and exit/state mismatch fail
immediately. Logs expose only operational counts/times and safe error codes.

Tests:
- tests/notifications-recovery.test.mjs: A–F, artifact errors and watchdog ownership.
- tests/recovery-chain.integration.mjs: actual RC2 checkout -> gate -> wrapper ->
  worker, local PostgreSQL, synthetic Baileys/Push, polls, restart/epoch and backlog.
- tests/recovery-db-retry.integration.mjs: actual verifier with unavailable local
  database followed by recovery. No customer transport permitted in tests.

Recovery order: assert production SHA/baseline/outbound, publish this orchestration,
start one terminal session with watchdog disarmed, observe multiple successful
polls, coordinated stop/restart after gate proves expiry, then arm compatible
watchdog. Never restore legacy/RC1 or bypass gate. Do not mutate auth/held backlog.

Final cloud runtime gate:
- The production session step uses `exec node`, so cancellation reaches the
  supervisor directly. It forwards SIGTERM to the unchanged RC2 wrapper.
- `Centrix direct runtime evidence` is a dedicated GitHub Check Run, updated from
  runner-loopback /live and /ready probes every 15 seconds. It exports only
  owner/epoch/process UUID/revision/pids, poll/error counters and outbound hold.
  This is readable through the Checks REST API while the worker job stays active;
  no public health endpoint, new database table or application change is needed.
- The protected `Notifications Controlled Retirement` workflow binds a short-lived
  request to a fresh evidence check ID and exact run/owner/epoch/process UUID/SHA.
  The running supervisor stops its child first, observes RC2 stopping/heartbeat
  stop/worker exit, verifies no child pids remain, then publishes acknowledgment.
  Only then does the session finish naturally. A forced/incomplete stop is failure.
- GitHub cancellation has a hard runner timeout and is never represented as a
  guaranteed successful job exit. Use controlled retirement for audited handoff;
  successors still require the existing PostgreSQL expiry/renewal-safety gate.
- `notifications-cloud-runtime-gate.yml` runs the same bash-exec supervisor -> RC2
  wrapper -> worker chain on a real GitHub runner with a LOCAL synthetic PostgreSQL
  service, mock WhatsApp/Push and real GitHub Checks API. No Production DB or auth
  is supplied. It exercises SIGINT/SIGTERM, controlled retirement, cancellation-like
  signals, DB outage, stale callbacks, frozen heartbeat, successor gate and watchdog.
