import assert from 'node:assert/strict';
import { performance } from 'node:perf_hooks';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { ReplicaSet } from '../src/replica-store.js';
import { initializeDatabase, QueueRepository, resetDatabase } from '../src/repository.js';
import { QueueService } from '../src/service.js';

const { Pool } = pg;
const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error('DATABASE_URL is required');

const batchCount = 32;
const recordsPerBatch = 16;
const totalRecords = batchCount * recordsPerBatch;
const valueBytes = 1_024;
const root = await mkdtemp(join(tmpdir(), 'message-queue-benchmark-'));
const pool = new Pool({ connectionString: databaseUrl, max: 12, statement_timeout: 10_000 });

function percentile(values, fraction) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

function rounded(value) {
  return Number(value.toFixed(3));
}

try {
  await initializeDatabase(pool);
  await resetDatabase(pool, 'fenced-message-queue-lab-reset');
  const repository = new QueueRepository(pool);
  const service = new QueueService(repository, new ReplicaSet(root));
  const version = await pool.query(`SHOW server_version`);
  await service.createProducer({ producerId: 'benchmark-producer' });

  const publishLatencies = [];
  const publishStarted = performance.now();
  for (let batch = 0; batch < batchCount; batch += 1) {
    const started = performance.now();
    const result = await service.publish({
      expectedLeaderEpoch: 1,
      body: {
        producerId: 'benchmark-producer',
        producerEpoch: 0,
        baseSequence: batch * recordsPerBatch,
        records: Array.from({ length: recordsPerBatch }, (_, record) => ({
          key: `benchmark-${batch}-${record}`,
          headers: [{ name: 'kind', value: 'bounded-fixture' }],
          value: `${String(batch).padStart(2, '0')}:${String(record).padStart(2, '0')}:${'x'.repeat(valueBytes - 6)}`,
        })),
      },
    });
    assert.equal(result.baseOffset, batch * recordsPerBatch);
    assert.equal(result.replicaCount, 3);
    publishLatencies.push(performance.now() - started);
  }
  const publishMs = performance.now() - publishStarted;

  const joined = await service.joinGroup({
    groupId: 'benchmark-group',
    requestKey: 'benchmark-join-001',
    body: { memberId: 'benchmark-member' },
  });
  const fetchLatencies = [];
  let cursor = 0;
  let fetches = 0;
  const fetchStarted = performance.now();
  while (cursor < totalRecords) {
    const started = performance.now();
    const fetched = await service.fetch({
      groupId: 'benchmark-group',
      requestKey: `benchmark-fetch-${String(fetches).padStart(3, '0')}`,
      body: {
        memberId: 'benchmark-member',
        generation: joined.generation,
        offset: cursor,
        maxRecords: 128,
        maxBytes: 1024 * 1024,
      },
    });
    assert.ok(fetched.nextOffset > cursor);
    assert.deepEqual(
      fetched.records.map((record) => record.offset),
      Array.from({ length: fetched.nextOffset - cursor }, (_, index) => cursor + index),
    );
    cursor = fetched.nextOffset;
    fetches += 1;
    fetchLatencies.push(performance.now() - started);
  }
  const fetchMs = performance.now() - fetchStarted;
  await service.checkpoint({
    groupId: 'benchmark-group',
    requestKey: 'benchmark-checkpoint-001',
    body: { memberId: 'benchmark-member', generation: joined.generation, nextOffset: cursor },
  });

  const counts = await pool.query(`
    SELECT partition.high_watermark,
      (SELECT count(*) FROM mq_batches) AS batches,
      (SELECT count(*) FROM mq_replica_receipts) AS receipts,
      (SELECT count(*) FROM mq_fetch_receipts) AS fetches
    FROM mq_partitions AS partition
  `);
  assert.deepEqual(
    {
      highWatermark: Number(counts.rows[0].high_watermark),
      batches: Number(counts.rows[0].batches),
      receipts: Number(counts.rows[0].receipts),
      fetches: Number(counts.rows[0].fetches),
    },
    { highWatermark: totalRecords, batches: batchCount, receipts: batchCount * 3, fetches },
  );

  process.stdout.write(`${JSON.stringify({
    evidence: 'bounded_queue_benchmark_observation',
    node: process.version,
    postgresql: version.rows[0].server_version,
    fixture: {
      partitions: 1,
      replicaDirectories: 3,
      batches: batchCount,
      recordsPerBatch,
      records: totalRecords,
      valueBytes,
      fetches,
      replicaReceipts: batchCount * 3,
    },
    publishRecordsPerSecond: rounded(totalRecords / (publishMs / 1_000)),
    publishBatchLatencyMs: {
      p50: rounded(percentile(publishLatencies, 0.5)),
      p95: rounded(percentile(publishLatencies, 0.95)),
      max: rounded(Math.max(...publishLatencies)),
    },
    fetchRecordsPerSecond: rounded(totalRecords / (fetchMs / 1_000)),
    fetchLatencyMs: {
      p50: rounded(percentile(fetchLatencies, 0.5)),
      p95: rounded(percentile(fetchLatencies, 0.95)),
      max: rounded(Math.max(...fetchLatencies)),
    },
    exclusions: [
      'network latency',
      'independent hosts or disks',
      'broker failover or consensus',
      'compression and segment indexes',
      'multi-partition balancing',
      'production extrapolation or SLA',
    ],
  })}\n`);
} finally {
  await pool.end();
  await rm(root, { recursive: true, force: true });
}
