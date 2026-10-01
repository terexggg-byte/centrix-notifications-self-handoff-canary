import { readFile, access } from 'node:fs/promises';
import { constants } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
export async function verifyRecoveryArtifact({ env = process.env } = {}) {
  const own = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const rc = path.resolve(env.CENTRIX_RC_DIR || '');
  const manifest = JSON.parse(await readFile(path.join(own, 'scripts/notifications-recovery-artifact.json'), 'utf8'));
  if (!env.CENTRIX_RC_DIR || env.RELEASE_SHA !== manifest.release) throw Error('LEASE_GATE_ARTIFACT: approved RC2 release and checkout required');
  for (const relative of manifest.orchestration) {
    try { await access(path.join(own, relative), constants.R_OK); }
    catch { throw Error(`LEASE_GATE_ARTIFACT: missing orchestration file ${relative}`); }
  }
  for (const [relative, expected] of Object.entries(manifest.application)) {
    let bytes;
    try { bytes = await readFile(path.join(rc, relative)); }
    catch { throw Error(`LEASE_GATE_ARTIFACT: missing application file ${relative}`); }
    if (createHash('sha256').update(bytes).digest('hex') !== expected) throw Error(`LEASE_GATE_ARTIFACT: RC2 hash mismatch ${relative}`);
  }
  return { release: manifest.release, verifiedFiles: manifest.orchestration.length + Object.keys(manifest.application).length };
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { console.log(JSON.stringify({ event: 'artifact.verified', ...await verifyRecoveryArtifact() })); }
  catch (error) { console.error(error.message); process.exitCode = 4; }
}
