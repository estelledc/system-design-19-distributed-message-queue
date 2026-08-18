import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { initializeDatabase, resetDatabase } from '../src/repository.js';

const { Pool } = pg;
const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error('DATABASE_URL is required');

const apiToken = 'synthetic-smoke-token-00000001';
const replicaRoot = await mkdtemp(join(tmpdir(), 'message-queue-smoke-'));
const pool = new Pool({ connectionString: databaseUrl, max: 6, statement_timeout: 10_000 });
const collectedLogs = [];

async function start(extra = {}) {
  const child = spawn(process.execPath, ['src/main.js', 'serve'], {
    cwd: new URL('..', import.meta.url),
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      MQ_API_TOKEN: apiToken,
      MQ_REPLICA_ROOT: replicaRoot,
      PORT: '0',
      ...extra,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let buffer = '';
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('server did not become ready')), 10_000);
    child.once('error', reject);
    child.stdout.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      while (buffer.includes('\n')) {
        const index = buffer.indexOf('\n');
        const line = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        collectedLogs.push(line);
        try {
          const event = JSON.parse(line);
          if (event.event === 'server_ready') {
            clearTimeout(timeout);
            resolve({ child, origin: `http://127.0.0.1:${event.port}` });
          }
        } catch {
          // Only a structured readiness event can satisfy startup.
        }
      }
    });
    child.stderr.on('data', (chunk) => collectedLogs.push(chunk.toString('utf8').trim()));
    child.once('exit', (code, signal) => {
      clearTimeout(timeout);
      reject(new Error(`server exited before ready: ${code ?? signal}`));
    });
  });
}

async function stop(server) {
  if (server.child.exitCode !== null || server.child.signalCode !== null) return;
  server.child.kill('SIGTERM');
  await new Promise((resolve) => server.child.once('exit', resolve));
}

async function waitForExit(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise((resolve) => child.once('exit', resolve));
}

async function jsonRequest(origin, path, { method = 'GET', key, leaderEpoch, body, expectedStatus } = {}) {
  const headers = { Authorization: `Bearer ${apiToken}` };
  if (key) headers['Idempotency-Key'] = key;
  if (leaderEpoch !== undefined) headers['X-Leader-Epoch'] = String(leaderEpoch);
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const response = await fetch(`${origin}${path}`, {
    method,
    headers,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(10_000),
  });
  const parsed = await response.json();
  if (expectedStatus !== undefined) {
    assert.equal(response.status, expectedStatus, JSON.stringify(parsed));
  } else if (!response.ok) {
    throw new Error(`request failed with ${response.status}: ${parsed.error}`);
  }
  return { response, body: parsed };
}

const firstBatch = {
  producerId: 'private-producer-smoke',
  producerEpoch: 0,
  baseSequence: 0,
  records: [
    { key: 'private-key-a', headers: [{ name: 'kind', value: 'private-header-a' }], value: 'private-body-a' },
    { key: 'private-key-b', headers: [{ name: 'kind', value: 'private-header-b' }], value: 'private-body-b' },
  ],
};
const secondBatch = {
  producerId: 'private-producer-smoke',
  producerEpoch: 0,
  baseSequence: 2,
  records: [
    { key: 'private-key-c', headers: [{ name: 'kind', value: 'private-header-c' }], value: 'private-body-c' },
  ],
};

try {
  await initializeDatabase(pool);
  await resetDatabase(pool, 'fenced-message-queue-lab-reset');

  let server = await start();
  await jsonRequest(server.origin, '/v1/producers', { method: 'POST', body: { producerId: firstBatch.producerId } });
  await stop(server);

  const crashingReplica = await start({ MQ_CRASH_AFTER_FIRST_REPLICA_RECEIPT: '1' });
  await assert.rejects(jsonRequest(crashingReplica.origin, '/v1/records', {
    method: 'POST', leaderEpoch: 1, body: firstBatch,
  }));
  await waitForExit(crashingReplica.child);
  const partial = await pool.query(`
    SELECT intent.status, partition.high_watermark,
      (SELECT count(*) FROM mq_replica_receipts) AS receipt_count
    FROM mq_publish_intents AS intent
    CROSS JOIN mq_partitions AS partition
  `);
  assert.equal(partial.rowCount, 1);
  assert.deepEqual(
    {
      status: partial.rows[0].status,
      highWatermark: Number(partial.rows[0].high_watermark),
      receiptCount: Number(partial.rows[0].receipt_count),
    },
    { status: 'replicating', highWatermark: 0, receiptCount: 1 },
  );

  server = await start();
  const resumed = await jsonRequest(server.origin, '/v1/records', {
    method: 'POST', leaderEpoch: 1, body: firstBatch,
  });
  assert.equal(resumed.body.baseOffset, 0);
  assert.equal(resumed.body.nextOffset, 2);
  await stop(server);

  const crashingCommit = await start({ MQ_CRASH_AFTER_PUBLISH_COMMIT: '1' });
  await assert.rejects(jsonRequest(crashingCommit.origin, '/v1/records', {
    method: 'POST', leaderEpoch: 1, body: secondBatch,
  }));
  await waitForExit(crashingCommit.child);
  const committedAfterLoss = await pool.query(`
    SELECT high_watermark, (SELECT count(*) FROM mq_batches) AS batch_count
    FROM mq_partitions
  `);
  assert.deepEqual(
    {
      highWatermark: Number(committedAfterLoss.rows[0].high_watermark),
      batchCount: Number(committedAfterLoss.rows[0].batch_count),
    },
    { highWatermark: 3, batchCount: 2 },
  );

  server = await start();
  const publishReplay = await jsonRequest(server.origin, '/v1/records', {
    method: 'POST', leaderEpoch: 1, body: secondBatch,
  });
  assert.equal(publishReplay.body.replayed, true);
  assert.equal(publishReplay.body.baseOffset, 2);
  const memberA = await jsonRequest(server.origin, '/v1/groups/private-group-smoke/members', {
    method: 'POST', key: 'smoke-join-key-0001', body: { memberId: 'private-member-a' },
  });
  const fetchedA = await jsonRequest(server.origin, '/v1/groups/private-group-smoke/fetch', {
    method: 'POST',
    key: 'smoke-fetch-key-001',
    body: {
      memberId: 'private-member-a', generation: memberA.body.generation,
      offset: 0, maxRecords: 8, maxBytes: 65_536,
    },
  });
  assert.deepEqual(fetchedA.body.records.map((record) => record.offset), [0, 1, 2]);
  const memberB = await jsonRequest(server.origin, '/v1/groups/private-group-smoke/members', {
    method: 'POST', key: 'smoke-join-key-0002', body: { memberId: 'private-member-b' },
  });
  const staleCheckpoint = await jsonRequest(server.origin, '/v1/groups/private-group-smoke/checkpoints', {
    method: 'POST',
    key: 'smoke-checkpoint-001',
    expectedStatus: 409,
    body: { memberId: 'private-member-a', generation: memberA.body.generation, nextOffset: 3 },
  });
  assert.equal(staleCheckpoint.body.error, 'fenced');
  const fetchedB = await jsonRequest(server.origin, '/v1/groups/private-group-smoke/fetch', {
    method: 'POST',
    key: 'smoke-fetch-key-002',
    body: {
      memberId: 'private-member-b', generation: memberB.body.generation,
      offset: 0, maxRecords: 8, maxBytes: 65_536,
    },
  });
  assert.deepEqual(fetchedB.body.records.map((record) => record.offset), [0, 1, 2]);
  await stop(server);

  const crashingCheckpoint = await start({ MQ_CRASH_AFTER_CHECKPOINT_COMMIT: '1' });
  await assert.rejects(jsonRequest(crashingCheckpoint.origin, '/v1/groups/private-group-smoke/checkpoints', {
    method: 'POST',
    key: 'smoke-checkpoint-002',
    body: { memberId: 'private-member-b', generation: memberB.body.generation, nextOffset: 3 },
  }));
  await waitForExit(crashingCheckpoint.child);
  assert.equal(Number((await pool.query(`SELECT checkpoint_offset FROM mq_groups`)).rows[0].checkpoint_offset), 3);

  server = await start();
  const checkpointReplay = await jsonRequest(server.origin, '/v1/groups/private-group-smoke/checkpoints', {
    method: 'POST',
    key: 'smoke-checkpoint-002',
    body: { memberId: 'private-member-b', generation: memberB.body.generation, nextOffset: 3 },
  });
  assert.equal(checkpointReplay.body.replayed, true);
  assert.equal(checkpointReplay.body.nextOffset, 3);
  await stop(server);

  const summary = await pool.query(`
    SELECT partition.high_watermark,
      (SELECT count(*) FROM mq_batches) AS batches,
      (SELECT count(*) FROM mq_fetch_receipts WHERE status = 'complete') AS completed_fetches,
      (SELECT count(*) FROM mq_checkpoint_receipts) AS checkpoints
    FROM mq_partitions AS partition
  `);
  assert.deepEqual(
    {
      highWatermark: Number(summary.rows[0].high_watermark),
      batches: Number(summary.rows[0].batches),
      completedFetches: Number(summary.rows[0].completed_fetches),
      checkpoints: Number(summary.rows[0].checkpoints),
    },
    { highWatermark: 3, batches: 2, completedFetches: 2, checkpoints: 1 },
  );

  const logText = collectedLogs.join('\n');
  for (const forbidden of [
    apiToken,
    firstBatch.producerId,
    'private-group-smoke',
    'private-member-a',
    'private-member-b',
    ...firstBatch.records.flatMap((record) => [record.key, record.headers[0].value, record.value]),
    ...secondBatch.records.flatMap((record) => [record.key, record.headers[0].value, record.value]),
    resumed.body.batchDigest,
    publishReplay.body.batchDigest,
  ]) assert.equal(logText.includes(forbidden), false, `ordinary logs disclosed ${forbidden}`);
  assert.equal(/[0-9a-f]{64}/.test(logText), false, 'ordinary logs disclosed a digest');

  process.stdout.write(`${JSON.stringify({
    evidence: 'true_process_queue_recovery',
    minorityReceiptRecovered: true,
    publishResponseLossReplayed: true,
    staleMemberCheckpointFenced: true,
    recordsVisiblyReplayedAfterRebalance: 3,
    checkpointResponseLossReplayed: true,
    highWatermark: 3,
    batches: 2,
    completedFetches: 2,
    checkpoints: 1,
  })}\n`);
} finally {
  await pool.end();
  await rm(replicaRoot, { recursive: true, force: true });
}
