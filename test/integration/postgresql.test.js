import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { after, before } from 'node:test';
import pg from 'pg';
import { digestValue } from '../../src/contracts.js';
import { ReplicaSet } from '../../src/replica-store.js';
import { initializeDatabase, QueueRepository, resetDatabase } from '../../src/repository.js';
import { QueueService } from '../../src/service.js';

const { Pool } = pg;
const databaseUrl = process.env.DATABASE_URL;
assert.ok(databaseUrl, 'DATABASE_URL is required; infrastructure tests never skip');
const pool = new Pool({ connectionString: databaseUrl, max: 12, statement_timeout: 10_000 });

before(async () => {
  await initializeDatabase(pool);
});

after(async () => {
  await pool.end();
});

async function fixture(t) {
  await resetDatabase(pool, 'fenced-message-queue-lab-reset');
  const root = await mkdtemp(join(tmpdir(), 'mq-postgres-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const logs = [];
  const repository = new QueueRepository(pool);
  const replicas = new ReplicaSet(root);
  const service = new QueueService(repository, replicas, (entry) => logs.push(entry));
  return { root, logs, repository, replicas, service };
}

function publish({ sequence = 0, suffix = 'a', count = 2, producerEpoch = 0, producerId = 'producer-a' } = {}) {
  return {
    producerId,
    producerEpoch,
    baseSequence: sequence,
    records: Array.from({ length: count }, (_, index) => ({
      key: `key-${suffix}-${index}`,
      headers: [{ name: 'kind', value: `kind-${suffix}` }],
      value: `value-${suffix}-${index}`,
    })),
  };
}

test('PostgreSQL 17 is real and schema starts with one idle fenced partition', async (t) => {
  const { repository } = await fixture(t);
  const version = await pool.query(`SHOW server_version`);
  assert.match(version.rows[0].server_version, /^17\./);
  assert.deepEqual(await repository.getState(), {
    topic: 'events',
    partition: 0,
    leaderId: 'replica-a',
    leaderEpoch: 1,
    nextOffset: 0,
    highWatermark: 0,
    logStartOffset: 0,
    activeIntentId: null,
    stateRevision: 0,
    replicatingIntents: 0,
    committedBatches: 0,
    groupCount: 0,
  });
});

test('one replica stays invisible; exact retry reaches two and changed intent conflicts', async (t) => {
  const { repository, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  const body = publish();
  await assert.rejects(service.publish({ expectedLeaderEpoch: 1, body, replicaIds: ['replica-a'] }), {
    code: 'insufficient_replicas',
  });
  assert.equal((await repository.getState()).highWatermark, 0);
  const committed = await service.publish({ expectedLeaderEpoch: 1, body, replicaIds: ['replica-b'] });
  assert.equal(committed.baseOffset, 0);
  assert.equal(committed.nextOffset, 2);
  assert.equal(committed.replicaCount, 2);
  const replay = await service.publish({ expectedLeaderEpoch: 1, body });
  assert.equal(replay.replayed, true);
  assert.equal(replay.batchDigest, committed.batchDigest);
  await assert.rejects(service.publish({ expectedLeaderEpoch: 1, body: publish({ suffix: 'changed' }) }), { code: 'conflict' });
});

test('leader rotation discards old-epoch receipt authority but reuses immutable bytes', async (t) => {
  const { repository, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  const body = publish({ count: 1 });
  await assert.rejects(service.publish({ expectedLeaderEpoch: 1, body, replicaIds: ['replica-a'] }), {
    code: 'insufficient_replicas',
  });
  const rotated = await service.rotateLeader({
    principal: 'lab-operator',
    requestKey: 'rotate-key-0001',
    body: { expectedEpoch: 1, newLeaderId: 'replica-b' },
  });
  assert.equal(rotated.leaderEpoch, 2);
  await assert.rejects(service.publish({ expectedLeaderEpoch: 1, body }), { code: 'fenced' });
  const committed = await service.publish({
    expectedLeaderEpoch: 2,
    body,
    replicaIds: ['replica-a', 'replica-b'],
  });
  assert.equal(committed.leaderEpoch, 2);
  assert.equal((await repository.getState()).highWatermark, 1);
  const counts = await pool.query(`
    SELECT leader_epoch, count(*)::int AS count
    FROM mq_replica_receipts GROUP BY leader_epoch ORDER BY leader_epoch
  `);
  assert.deepEqual(counts.rows, [{ leader_epoch: '1', count: 1 }, { leader_epoch: '2', count: 2 }]);
});

test('concurrent exact publishers converge and another producer cannot bypass the active head', async (t) => {
  const { repository, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  await service.createProducer({ producerId: 'producer-b' });
  const blockedBody = publish({ producerId: 'producer-a', count: 1 });
  await assert.rejects(service.publish({ expectedLeaderEpoch: 1, body: blockedBody, replicaIds: ['replica-a'] }), {
    code: 'insufficient_replicas',
  });
  await assert.rejects(service.publish({
    expectedLeaderEpoch: 1,
    body: publish({ producerId: 'producer-b', suffix: 'b', count: 1 }),
  }), { code: 'partition_busy' });
  await service.publish({ expectedLeaderEpoch: 1, body: blockedBody, replicaIds: ['replica-b'] });
  const nextBody = publish({ producerId: 'producer-b', suffix: 'b', count: 1 });
  const results = await Promise.all(Array.from({ length: 6 }, () => service.publish({ expectedLeaderEpoch: 1, body: nextBody })));
  assert.equal(new Set(results.map((result) => result.batchDigest)).size, 1);
  assert.equal((await repository.getState()).highWatermark, 2);
  const batches = await pool.query(`SELECT base_offset, next_offset FROM mq_batches ORDER BY base_offset`);
  assert.deepEqual(batches.rows, [{ base_offset: '0', next_offset: '1' }, { base_offset: '1', next_offset: '2' }]);
});

test('producer epoch rotation fences stale writes and preserves old committed replay', async (t) => {
  const { service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  const old = publish({ count: 1 });
  const committed = await service.publish({ expectedLeaderEpoch: 1, body: old });
  const bumped = await service.bumpProducerEpoch({
    principal: 'lab-operator',
    requestKey: 'producer-epoch-0001',
    producerId: 'producer-a',
    body: { expectedEpoch: 0 },
  });
  assert.equal(bumped.epoch, 1);
  assert.equal((await service.publish({ expectedLeaderEpoch: 1, body: old })).batchDigest, committed.batchDigest);
  await assert.rejects(service.publish({
    expectedLeaderEpoch: 1,
    body: publish({ sequence: 1, suffix: 'stale', count: 1 }),
  }), { code: 'fenced' });
  const current = await service.publish({
    expectedLeaderEpoch: 1,
    body: publish({ producerEpoch: 1, sequence: 0, suffix: 'new', count: 1 }),
  });
  assert.equal(current.baseOffset, 1);
});

test('consumer fetch returns committed partition order and exact fetch replay', async (t) => {
  const { service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  await service.publish({ expectedLeaderEpoch: 1, body: publish({ count: 3 }) });
  const joined = await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0001', body: { memberId: 'member-a' },
  });
  const body = { memberId: 'member-a', generation: joined.generation, offset: 0, maxRecords: 2, maxBytes: 8192 };
  const first = await service.fetch({ groupId: 'group-a', requestKey: 'fetch-key-0001', body });
  assert.deepEqual(first.records.map((record) => record.offset), [0, 1]);
  assert.equal(first.nextOffset, 2);
  const replay = await service.fetch({ groupId: 'group-a', requestKey: 'fetch-key-0001', body });
  assert.equal(replay.replayed, true);
  assert.deepEqual(replay.records, first.records);
  const second = await service.fetch({
    groupId: 'group-a',
    requestKey: 'fetch-key-0002',
    body: { ...body, offset: 2, maxRecords: 2 },
  });
  assert.deepEqual(second.records.map((record) => record.offset), [2]);
});

test('rebalance after fetch fences stale checkpoint and visibly replays to new owner', async (t) => {
  const { repository, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  await service.publish({ expectedLeaderEpoch: 1, body: publish({ count: 2 }) });
  const first = await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0001', body: { memberId: 'member-a' },
  });
  const fetched = await service.fetch({
    groupId: 'group-a',
    requestKey: 'fetch-key-0001',
    body: { memberId: 'member-a', generation: first.generation, offset: 0, maxRecords: 2, maxBytes: 8192 },
  });
  const second = await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0002', body: { memberId: 'member-b' },
  });
  await assert.rejects(service.checkpoint({
    groupId: 'group-a',
    requestKey: 'checkpoint-0001',
    body: { memberId: 'member-a', generation: first.generation, nextOffset: fetched.nextOffset },
  }), { code: 'fenced' });
  const replayed = await service.fetch({
    groupId: 'group-a',
    requestKey: 'fetch-key-0002',
    body: { memberId: 'member-b', generation: second.generation, offset: 0, maxRecords: 2, maxBytes: 8192 },
  });
  assert.deepEqual(replayed.records.map((record) => record.offset), [0, 1]);
  await service.checkpoint({
    groupId: 'group-a',
    requestKey: 'checkpoint-0002',
    body: { memberId: 'member-b', generation: second.generation, nextOffset: 2 },
  });
  assert.equal((await repository.getGroup('group-a')).checkpointOffset, 2);
});

test('rebalance explicitly revokes an accepted fetch receipt before it can advance a cursor', async (t) => {
  const { repository, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  await service.publish({ expectedLeaderEpoch: 1, body: publish({ count: 1 }) });
  const first = await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0001', body: { memberId: 'member-a' },
  });
  const input = {
    memberId: 'member-a', generation: first.generation, offset: 0, maxRecords: 1, maxBytes: 8192,
  };
  await repository.beginFetch({
    groupId: 'group-a',
    requestKey: 'fetch-key-0001',
    intentDigest: digestValue({ operation: 'fetch', groupId: 'group-a', ...input }),
    ...input,
  });
  await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0002', body: { memberId: 'member-b' },
  });
  const receipt = await pool.query(`SELECT status, returned_next_offset FROM mq_fetch_receipts`);
  assert.deepEqual(receipt.rows, [{ status: 'revoked', returned_next_offset: null }]);
  await assert.rejects(service.fetch({
    groupId: 'group-a', requestKey: 'fetch-key-0001', body: input,
  }), { code: 'fenced' });
  assert.equal((await repository.getGroup('group-a')).fetchCursor, 0);
});

test('checkpoint response-loss replay is stable and changed intent conflicts', async (t) => {
  const { service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  await service.publish({ expectedLeaderEpoch: 1, body: publish({ count: 1 }) });
  const joined = await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0001', body: { memberId: 'member-a' },
  });
  await service.fetch({
    groupId: 'group-a', requestKey: 'fetch-key-0001',
    body: { memberId: 'member-a', generation: joined.generation, offset: 0, maxRecords: 1, maxBytes: 8192 },
  });
  const input = {
    groupId: 'group-a', requestKey: 'checkpoint-0001',
    body: { memberId: 'member-a', generation: joined.generation, nextOffset: 1 },
  };
  const first = await service.checkpoint(input);
  const replay = await service.checkpoint(input);
  assert.equal(first.nextOffset, 1);
  assert.equal(replay.replayed, true);
  await assert.rejects(service.checkpoint({ ...input, body: { ...input.body, nextOffset: 0 } }), { code: 'conflict' });
});

test('retention exposes lag as offset_out_of_range while a new group starts at log start', async (t) => {
  const { repository, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  await service.publish({ expectedLeaderEpoch: 1, body: publish({ count: 2 }) });
  const lagging = await service.joinGroup({
    groupId: 'lagging-group', requestKey: 'join-lagging-001', body: { memberId: 'member-old' },
  });
  const state = await repository.getState();
  await service.advanceRetention({
    principal: 'lab-operator',
    requestKey: 'retention-key-001',
    body: { expectedRevision: state.stateRevision, newLogStart: 1 },
  });
  await assert.rejects(service.fetch({
    groupId: 'lagging-group', requestKey: 'fetch-lagging-01',
    body: { memberId: 'member-old', generation: lagging.generation, offset: 0, maxRecords: 2, maxBytes: 8192 },
  }), { code: 'offset_out_of_range' });
  const current = await service.joinGroup({
    groupId: 'current-group', requestKey: 'join-current-001', body: { memberId: 'member-new' },
  });
  assert.equal(current.checkpointOffset, 1);
  const fetched = await service.fetch({
    groupId: 'current-group', requestKey: 'fetch-current-01',
    body: { memberId: 'member-new', generation: current.generation, offset: 1, maxRecords: 2, maxBytes: 8192 },
  });
  assert.deepEqual(fetched.records.map((record) => record.offset), [1]);
});

test('one corrupt replica falls back; loss of every committed copy fails closed', async (t) => {
  const { replicas, service } = await fixture(t);
  await service.createProducer({ producerId: 'producer-a' });
  const committed = await service.publish({ expectedLeaderEpoch: 1, body: publish({ count: 1 }) });
  await writeFile(replicas.objectPath('replica-a', committed.batchDigest), 'corrupt-a');
  const first = await service.joinGroup({
    groupId: 'group-a', requestKey: 'join-group-0001', body: { memberId: 'member-a' },
  });
  const fetched = await service.fetch({
    groupId: 'group-a', requestKey: 'fetch-key-0001',
    body: { memberId: 'member-a', generation: first.generation, offset: 0, maxRecords: 1, maxBytes: 8192 },
  });
  assert.equal(fetched.records.length, 1);
  await writeFile(replicas.objectPath('replica-b', committed.batchDigest), 'corrupt-b');
  await writeFile(replicas.objectPath('replica-c', committed.batchDigest), 'corrupt-c');
  const second = await service.joinGroup({
    groupId: 'group-b', requestKey: 'join-group-0002', body: { memberId: 'member-b' },
  });
  await assert.rejects(service.fetch({
    groupId: 'group-b', requestKey: 'fetch-key-0002',
    body: { memberId: 'member-b', generation: second.generation, offset: 0, maxRecords: 1, maxBytes: 8192 },
  }), { code: 'replica_corrupt_or_missing' });
});

test('ordinary service logs exclude synthetic identities, keys, bodies, and digests', async (t) => {
  const { logs, service } = await fixture(t);
  await service.createProducer({ producerId: 'private-producer-sentinel' });
  const body = {
    producerId: 'private-producer-sentinel',
    producerEpoch: 0,
    baseSequence: 0,
    records: [{ key: 'private-key-sentinel', headers: [{ name: 'kind', value: 'private-header-sentinel' }], value: 'private-body-sentinel' }],
  };
  const result = await service.publish({ expectedLeaderEpoch: 1, body });
  const serialized = JSON.stringify(logs);
  for (const forbidden of [
    'private-producer-sentinel', 'private-key-sentinel', 'private-header-sentinel', 'private-body-sentinel', result.batchDigest,
  ]) assert.doesNotMatch(serialized, new RegExp(forbidden));
  assert.match(serialized, /batch_committed/);
});
