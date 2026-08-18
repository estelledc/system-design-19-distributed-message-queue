import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { queueContract } from './contracts.js';
import {
  conflict,
  fenced,
  insufficientReplicas,
  notFound,
  offsetOutOfRange,
  outOfOrder,
  partitionBusy,
  precondition,
} from './errors.js';

const schemaUrl = new URL('../sql/schema.sql', import.meta.url);

function integer(value) {
  const result = Number(value);
  if (!Number.isSafeInteger(result)) throw new Error('database integer exceeded the safe range');
  return result;
}

async function rollback(client) {
  await client.query('ROLLBACK').catch(() => {});
}

function intentFromRow(row, replayed = false) {
  return {
    intentId: row.intent_id,
    topic: row.topic,
    partition: integer(row.partition_id),
    producerId: row.producer_id,
    producerEpoch: integer(row.producer_epoch),
    baseSequence: integer(row.base_sequence),
    baseOffset: integer(row.base_offset),
    nextOffset: integer(row.base_offset) + integer(row.record_count),
    recordCount: integer(row.record_count),
    batchDigest: row.batch_digest,
    batchBytes: integer(row.batch_bytes),
    status: row.status,
    startedLeaderEpoch: integer(row.started_leader_epoch),
    committedLeaderEpoch: row.committed_leader_epoch === null ? null : integer(row.committed_leader_epoch),
    result: row.result_json,
    replayed,
  };
}

function fetchFromRow(row, replayed = false) {
  return {
    fetchId: row.fetch_id,
    groupId: row.group_id,
    requestKey: row.request_key,
    memberId: row.member_id,
    generation: integer(row.generation),
    requestedOffset: integer(row.requested_offset),
    frozenLogStart: integer(row.frozen_log_start),
    frozenHighWatermark: integer(row.frozen_high_watermark),
    maxRecords: integer(row.max_records),
    maxBytes: integer(row.max_bytes),
    status: row.status,
    returnedNextOffset: row.returned_next_offset === null ? null : integer(row.returned_next_offset),
    resultCount: row.result_count === null ? null : integer(row.result_count),
    replayed,
  };
}

export async function initializeDatabase(pool) {
  await pool.query(await readFile(fileURLToPath(schemaUrl), 'utf8'));
}

export async function resetDatabase(pool, confirmation) {
  if (confirmation !== 'fenced-message-queue-lab-reset') {
    throw new Error('refusing to reset without the explicit lab confirmation');
  }
  await pool.query(`
    TRUNCATE mq_checkpoint_receipts, mq_fetch_receipts, mq_join_receipts,
      mq_group_assignments, mq_groups, mq_mutation_receipts, mq_batches,
      mq_replica_receipts, mq_publish_intents, mq_producers, mq_partitions CASCADE;
    INSERT INTO mq_partitions (
      topic, partition_id, leader_id, leader_epoch,
      next_offset, high_watermark, log_start_offset, state_revision, updated_at
    ) VALUES ('events', 0, 'replica-a', 1, 0, 0, 0, 0, clock_timestamp());
  `);
}

export class QueueRepository {
  constructor(pool) {
    this.pool = pool;
  }

  async #databaseNow(client) {
    const result = await client.query(`SELECT clock_timestamp() AS now`);
    return new Date(result.rows[0].now);
  }

  async #advisory(client, value) {
    await client.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [value]);
  }

  async #lockPartition(client, mode = 'UPDATE') {
    const result = await client.query(`
      SELECT *
      FROM mq_partitions
      WHERE topic = $1 AND partition_id = $2
      FOR ${mode}
    `, [queueContract.topic, queueContract.partition]);
    if (!result.rowCount) throw new Error('queue partition is missing');
    return result.rows[0];
  }

  async #existingMutation(client, principal, requestKey, intentDigest) {
    const found = await client.query(
      `SELECT * FROM mq_mutation_receipts WHERE principal = $1 AND request_key = $2`,
      [principal, requestKey],
    );
    if (!found.rowCount) return null;
    if (found.rows[0].intent_digest !== intentDigest) throw conflict();
    return { ...found.rows[0].result_json, replayed: true };
  }

  async #recordMutation(client, { principal, requestKey, intentDigest, operation, result, now }) {
    await client.query(`
      INSERT INTO mq_mutation_receipts (
        principal, request_key, intent_digest, operation, result_json, created_at
      ) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
    `, [principal, requestKey, intentDigest, operation, JSON.stringify(result), now]);
    return { ...result, replayed: false };
  }

  async getState() {
    const result = await this.pool.query(`
      SELECT partition.*,
        (SELECT count(*) FROM mq_publish_intents WHERE status = 'replicating') AS replicating_intents,
        (SELECT count(*) FROM mq_batches) AS committed_batches,
        (SELECT count(*) FROM mq_groups) AS group_count
      FROM mq_partitions AS partition
      WHERE topic = $1 AND partition_id = $2
    `, [queueContract.topic, queueContract.partition]);
    const row = result.rows[0];
    if (!row) throw new Error('queue partition is missing');
    return {
      topic: row.topic,
      partition: integer(row.partition_id),
      leaderId: row.leader_id,
      leaderEpoch: integer(row.leader_epoch),
      nextOffset: integer(row.next_offset),
      highWatermark: integer(row.high_watermark),
      logStartOffset: integer(row.log_start_offset),
      activeIntentId: row.active_intent_id,
      stateRevision: integer(row.state_revision),
      replicatingIntents: integer(row.replicating_intents),
      committedBatches: integer(row.committed_batches),
      groupCount: integer(row.group_count),
    };
  }

  async createProducer(producerId) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `producer:${producerId.length}:${producerId}`);
      const existing = await client.query(`SELECT * FROM mq_producers WHERE producer_id = $1`, [producerId]);
      if (existing.rowCount) {
        await client.query('COMMIT');
        return {
          producerId,
          epoch: integer(existing.rows[0].epoch),
          nextSequence: integer(existing.rows[0].next_sequence),
          created: false,
        };
      }
      const now = await this.#databaseNow(client);
      await client.query(`
        INSERT INTO mq_producers (producer_id, epoch, next_sequence, created_at, updated_at)
        VALUES ($1, 0, 0, $2, $2)
      `, [producerId, now]);
      await client.query('COMMIT');
      return { producerId, epoch: 0, nextSequence: 0, created: true };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async bumpProducerEpoch({ principal, requestKey, intentDigest, producerId, expectedEpoch }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `mutation:${principal.length}:${principal}:${requestKey.length}:${requestKey}`);
      let result = await this.#existingMutation(client, principal, requestKey, intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const partition = await this.#lockPartition(client);
      if (partition.active_intent_id) {
        const active = await client.query(`SELECT producer_id FROM mq_publish_intents WHERE intent_id = $1`, [partition.active_intent_id]);
        if (active.rows[0]?.producer_id === producerId) throw partitionBusy();
      }
      const producer = await client.query(`SELECT * FROM mq_producers WHERE producer_id = $1 FOR UPDATE`, [producerId]);
      if (!producer.rowCount) throw notFound();
      const current = integer(producer.rows[0].epoch);
      if (current !== expectedEpoch) throw fenced('producer epoch', current);
      const now = await this.#databaseNow(client);
      const nextEpoch = current + 1;
      await client.query(`
        UPDATE mq_producers SET epoch = $2, next_sequence = 0, updated_at = $3 WHERE producer_id = $1
      `, [producerId, nextEpoch, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest,
        operation: 'bump_producer_epoch',
        result: { operation: 'bump_producer_epoch', producerId, epoch: nextEpoch, nextSequence: 0 },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async reservePublish({ producerId, producerEpoch, baseSequence, recordCount, batchDigest, bytes, expectedLeaderEpoch }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `publish:${producerId.length}:${producerId}:${producerEpoch}:${baseSequence}`);
      const existing = await client.query(`
        SELECT * FROM mq_publish_intents
        WHERE producer_id = $1 AND producer_epoch = $2 AND base_sequence = $3
      `, [producerId, producerEpoch, baseSequence]);
      if (existing.rowCount) {
        const row = existing.rows[0];
        if (row.batch_digest !== batchDigest || integer(row.record_count) !== recordCount || integer(row.batch_bytes) !== bytes.length) {
          throw conflict();
        }
        await client.query('COMMIT');
        return intentFromRow(row, true);
      }

      const partition = await this.#lockPartition(client);
      const currentLeaderEpoch = integer(partition.leader_epoch);
      if (currentLeaderEpoch !== expectedLeaderEpoch) throw fenced('leader epoch', currentLeaderEpoch);
      const producer = await client.query(`SELECT * FROM mq_producers WHERE producer_id = $1 FOR UPDATE`, [producerId]);
      if (!producer.rowCount) throw notFound();
      const currentEpoch = integer(producer.rows[0].epoch);
      if (currentEpoch !== producerEpoch) throw fenced('producer epoch', currentEpoch);
      const expectedSequence = integer(producer.rows[0].next_sequence);
      if (baseSequence !== expectedSequence) throw outOfOrder('producer sequence', expectedSequence);
      if (partition.active_intent_id !== null) throw partitionBusy();
      if (integer(partition.next_offset) !== integer(partition.high_watermark)) {
        throw new Error('partition offset invariant failed before reservation');
      }

      const intentId = randomUUID();
      const now = await this.#databaseNow(client);
      const baseOffset = integer(partition.high_watermark);
      const inserted = await client.query(`
        INSERT INTO mq_publish_intents (
          intent_id, topic, partition_id, producer_id, producer_epoch, base_sequence,
          base_offset, record_count, batch_digest, batch_bytes, staged_bytes,
          status, started_leader_epoch, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'replicating', $12, $13)
        RETURNING *
      `, [
        intentId,
        queueContract.topic,
        queueContract.partition,
        producerId,
        producerEpoch,
        baseSequence,
        baseOffset,
        recordCount,
        batchDigest,
        bytes.length,
        bytes,
        integer(partition.leader_epoch),
        now,
      ]);
      await client.query(`
        UPDATE mq_partitions
        SET active_intent_id = $3, next_offset = $4, updated_at = $5
        WHERE topic = $1 AND partition_id = $2
      `, [queueContract.topic, queueContract.partition, intentId, baseOffset + recordCount, now]);
      await client.query('COMMIT');
      return intentFromRow(inserted.rows[0], false);
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async loadReplicationWork({ intentId, expectedLeaderEpoch }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const partition = await this.#lockPartition(client, 'SHARE');
      const currentEpoch = integer(partition.leader_epoch);
      if (currentEpoch !== expectedLeaderEpoch) throw fenced('leader epoch', currentEpoch);
      const found = await client.query(`SELECT * FROM mq_publish_intents WHERE intent_id = $1`, [intentId]);
      if (!found.rowCount) throw notFound();
      const row = found.rows[0];
      if (row.status === 'committed') {
        await client.query('COMMIT');
        return { ...intentFromRow(row, true), bytes: null };
      }
      if (partition.active_intent_id !== intentId || row.staged_bytes === null) throw conflict('intent is not the active partition head');
      await client.query('COMMIT');
      return { ...intentFromRow(row, false), bytes: Buffer.from(row.staged_bytes) };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async recordReplicaReceipt({ intentId, expectedLeaderEpoch, replicaId, batchDigest, batchBytes }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const partition = await this.#lockPartition(client);
      const currentEpoch = integer(partition.leader_epoch);
      if (currentEpoch !== expectedLeaderEpoch) throw fenced('leader epoch', currentEpoch);
      const found = await client.query(`SELECT * FROM mq_publish_intents WHERE intent_id = $1 FOR UPDATE`, [intentId]);
      if (!found.rowCount) throw notFound();
      const intent = found.rows[0];
      if (intent.status === 'committed') {
        await client.query('COMMIT');
        return { intent: intentFromRow(intent, true), verifiedReplicas: queueContract.requiredReplicas };
      }
      if (
        partition.active_intent_id !== intentId
        || intent.batch_digest !== batchDigest
        || integer(intent.batch_bytes) !== batchBytes
      ) throw conflict('replica receipt does not match the active intent');
      const now = await this.#databaseNow(client);
      await client.query(`
        INSERT INTO mq_replica_receipts (
          intent_id, replica_id, leader_epoch, batch_digest, batch_bytes, verified_at
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (intent_id, replica_id, leader_epoch) DO NOTHING
      `, [intentId, replicaId, expectedLeaderEpoch, batchDigest, batchBytes, now]);
      const count = await client.query(`
        SELECT count(*) AS count
        FROM mq_replica_receipts
        WHERE intent_id = $1 AND leader_epoch = $2
          AND batch_digest = $3 AND batch_bytes = $4
      `, [intentId, expectedLeaderEpoch, batchDigest, batchBytes]);
      await client.query('COMMIT');
      return { intent: intentFromRow(intent, false), verifiedReplicas: integer(count.rows[0].count) };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async currentReplicaReceipts({ intentId, expectedLeaderEpoch }) {
    const partition = await this.getState();
    if (partition.leaderEpoch !== expectedLeaderEpoch) throw fenced('leader epoch', partition.leaderEpoch);
    const result = await this.pool.query(`
      SELECT receipt.replica_id, receipt.batch_digest, receipt.batch_bytes
      FROM mq_replica_receipts AS receipt
      JOIN mq_publish_intents AS intent ON intent.intent_id = receipt.intent_id
      WHERE receipt.intent_id = $1 AND receipt.leader_epoch = $2
        AND receipt.batch_digest = intent.batch_digest
        AND receipt.batch_bytes = intent.batch_bytes
      ORDER BY receipt.replica_id
    `, [intentId, expectedLeaderEpoch]);
    return result.rows.map((row) => ({
      replicaId: row.replica_id,
      batchDigest: row.batch_digest,
      batchBytes: integer(row.batch_bytes),
    }));
  }

  async commitPublish({ intentId, expectedLeaderEpoch, verifiedReplicaIds }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const partition = await this.#lockPartition(client);
      const found = await client.query(`SELECT * FROM mq_publish_intents WHERE intent_id = $1 FOR UPDATE`, [intentId]);
      if (!found.rowCount) throw notFound();
      const intent = found.rows[0];
      if (intent.status === 'committed') {
        await client.query('COMMIT');
        return { ...intent.result_json, replayed: true };
      }
      const currentEpoch = integer(partition.leader_epoch);
      if (currentEpoch !== expectedLeaderEpoch) throw fenced('leader epoch', currentEpoch);
      if (partition.active_intent_id !== intentId) throw conflict('intent is not the active partition head');
      const receipts = await client.query(`
        SELECT replica_id
        FROM mq_replica_receipts
        WHERE intent_id = $1 AND leader_epoch = $2
          AND batch_digest = $3 AND batch_bytes = $4
      `, [intentId, expectedLeaderEpoch, intent.batch_digest, integer(intent.batch_bytes)]);
      const receiptIds = new Set(receipts.rows.map((row) => row.replica_id));
      const verified = [...new Set(verifiedReplicaIds)].filter((replicaId) => receiptIds.has(replicaId));
      if (verified.length < queueContract.requiredReplicas) {
        throw insufficientReplicas(queueContract.requiredReplicas, verified.length);
      }
      const baseOffset = integer(intent.base_offset);
      const recordCount = integer(intent.record_count);
      const nextOffset = baseOffset + recordCount;
      if (integer(partition.high_watermark) !== baseOffset || integer(partition.next_offset) !== nextOffset) {
        throw new Error('partition committed-prefix invariant failed during finalize');
      }
      const producer = await client.query(`SELECT * FROM mq_producers WHERE producer_id = $1 FOR UPDATE`, [intent.producer_id]);
      if (!producer.rowCount) throw notFound();
      if (
        integer(producer.rows[0].epoch) !== integer(intent.producer_epoch)
        || integer(producer.rows[0].next_sequence) !== integer(intent.base_sequence)
      ) throw conflict('producer state changed before finalize');

      const now = await this.#databaseNow(client);
      await client.query(`
        INSERT INTO mq_batches (
          topic, partition_id, base_offset, next_offset, record_count,
          batch_digest, batch_bytes, intent_id, committed_leader_epoch, committed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `, [
        queueContract.topic,
        queueContract.partition,
        baseOffset,
        nextOffset,
        recordCount,
        intent.batch_digest,
        integer(intent.batch_bytes),
        intentId,
        expectedLeaderEpoch,
        now,
      ]);
      const result = {
        phase: 'batch_committed',
        topic: queueContract.topic,
        partition: queueContract.partition,
        producerId: intent.producer_id,
        producerEpoch: integer(intent.producer_epoch),
        baseSequence: integer(intent.base_sequence),
        baseOffset,
        nextOffset,
        recordCount,
        batchDigest: intent.batch_digest,
        leaderEpoch: expectedLeaderEpoch,
        replicaCount: verified.length,
      };
      await client.query(`
        UPDATE mq_publish_intents
        SET status = 'committed', staged_bytes = NULL, committed_leader_epoch = $2,
            result_json = $3::jsonb, committed_at = $4
        WHERE intent_id = $1
      `, [intentId, expectedLeaderEpoch, JSON.stringify(result), now]);
      await client.query(`
        UPDATE mq_partitions
        SET high_watermark = $3, active_intent_id = NULL, updated_at = $4
        WHERE topic = $1 AND partition_id = $2
      `, [queueContract.topic, queueContract.partition, nextOffset, now]);
      await client.query(`
        UPDATE mq_producers
        SET next_sequence = $2, updated_at = $3
        WHERE producer_id = $1
      `, [intent.producer_id, integer(intent.base_sequence) + recordCount, now]);
      await client.query('COMMIT');
      return { ...result, replayed: false };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async rotateLeader({ principal, requestKey, intentDigest, expectedEpoch, newLeaderId }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `mutation:${principal.length}:${principal}:${requestKey.length}:${requestKey}`);
      let result = await this.#existingMutation(client, principal, requestKey, intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const partition = await this.#lockPartition(client);
      const current = integer(partition.leader_epoch);
      if (current !== expectedEpoch) throw fenced('leader epoch', current);
      if (partition.leader_id === newLeaderId) throw conflict('new leader must differ from the current leader');
      const now = await this.#databaseNow(client);
      const nextEpoch = current + 1;
      const nextRevision = integer(partition.state_revision) + 1;
      await client.query(`
        UPDATE mq_partitions
        SET leader_id = $3, leader_epoch = $4, state_revision = $5, updated_at = $6
        WHERE topic = $1 AND partition_id = $2
      `, [queueContract.topic, queueContract.partition, newLeaderId, nextEpoch, nextRevision, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest,
        operation: 'rotate_leader',
        result: {
          operation: 'rotate_leader',
          previousLeaderId: partition.leader_id,
          leaderId: newLeaderId,
          leaderEpoch: nextEpoch,
          stateRevision: nextRevision,
          activeIntentId: partition.active_intent_id,
        },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async joinGroup({ groupId, memberId, requestKey, intentDigest }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `group:${groupId.length}:${groupId}`);
      const existing = await client.query(`
        SELECT receipt.*, assignment.status AS assignment_status
        FROM mq_join_receipts AS receipt
        JOIN mq_group_assignments AS assignment
          ON assignment.group_id = receipt.group_id AND assignment.generation = receipt.generation
        WHERE receipt.group_id = $1 AND receipt.request_key = $2
      `, [groupId, requestKey]);
      if (existing.rowCount) {
        if (existing.rows[0].intent_digest !== intentDigest) throw conflict();
        await client.query('COMMIT');
        return { ...existing.rows[0].result_json, assignmentStatus: existing.rows[0].assignment_status, replayed: true };
      }
      const now = await this.#databaseNow(client);
      let group = await client.query(`SELECT * FROM mq_groups WHERE group_id = $1 FOR UPDATE`, [groupId]);
      if (!group.rowCount) {
        const partition = await this.#lockPartition(client, 'SHARE');
        await client.query(`
          INSERT INTO mq_groups (
            group_id, current_generation, current_member_id, checkpoint_offset, created_at, updated_at
          ) VALUES ($1, 0, NULL, $2, $3, $3)
        `, [groupId, integer(partition.log_start_offset), now]);
        group = await client.query(`SELECT * FROM mq_groups WHERE group_id = $1 FOR UPDATE`, [groupId]);
      }
      const previousGeneration = integer(group.rows[0].current_generation);
      if (previousGeneration > 0) {
        await client.query(`
          UPDATE mq_group_assignments
          SET status = 'revoked', revoked_at = $3
          WHERE group_id = $1 AND generation = $2 AND status = 'current'
        `, [groupId, previousGeneration, now]);
        await client.query(`
          UPDATE mq_fetch_receipts
          SET status = 'revoked', completed_at = $3
          WHERE group_id = $1 AND generation = $2 AND status = 'accepted'
        `, [groupId, previousGeneration, now]);
      }
      const generation = previousGeneration + 1;
      const checkpoint = integer(group.rows[0].checkpoint_offset);
      await client.query(`
        INSERT INTO mq_group_assignments (
          group_id, generation, member_id, status, fetch_cursor, assigned_at
        ) VALUES ($1, $2, $3, 'current', $4, $5)
      `, [groupId, generation, memberId, checkpoint, now]);
      await client.query(`
        UPDATE mq_groups
        SET current_generation = $2, current_member_id = $3, updated_at = $4
        WHERE group_id = $1
      `, [groupId, generation, memberId, now]);
      const result = {
        phase: 'assignment_created',
        groupId,
        memberId,
        generation,
        checkpointOffset: checkpoint,
        previousGeneration: previousGeneration || null,
      };
      await client.query(`
        INSERT INTO mq_join_receipts (
          group_id, request_key, intent_digest, member_id, generation, result_json, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
      `, [groupId, requestKey, intentDigest, memberId, generation, JSON.stringify(result), now]);
      await client.query('COMMIT');
      return { ...result, assignmentStatus: 'current', replayed: false };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async beginFetch({ groupId, memberId, generation, requestKey, intentDigest, offset, maxRecords, maxBytes }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `fetch:${groupId.length}:${groupId}:${requestKey.length}:${requestKey}`);
      const existing = await client.query(`
        SELECT * FROM mq_fetch_receipts WHERE group_id = $1 AND request_key = $2
      `, [groupId, requestKey]);
      if (existing.rowCount) {
        if (existing.rows[0].intent_digest !== intentDigest) throw conflict();
        await client.query('COMMIT');
        return fetchFromRow(existing.rows[0], true);
      }
      const group = await client.query(`SELECT * FROM mq_groups WHERE group_id = $1 FOR UPDATE`, [groupId]);
      if (!group.rowCount) throw notFound();
      const currentGeneration = integer(group.rows[0].current_generation);
      if (currentGeneration !== generation || group.rows[0].current_member_id !== memberId) {
        throw fenced('group generation', currentGeneration);
      }
      const assignment = await client.query(`
        SELECT * FROM mq_group_assignments
        WHERE group_id = $1 AND generation = $2 AND status = 'current'
        FOR UPDATE
      `, [groupId, generation]);
      if (!assignment.rowCount || assignment.rows[0].member_id !== memberId) throw fenced('group generation', currentGeneration);
      const partition = await this.#lockPartition(client, 'SHARE');
      const logStart = integer(partition.log_start_offset);
      const highWatermark = integer(partition.high_watermark);
      if (integer(group.rows[0].checkpoint_offset) < logStart || offset < logStart || offset > highWatermark) {
        throw offsetOutOfRange(logStart, highWatermark);
      }
      const cursor = integer(assignment.rows[0].fetch_cursor);
      if (offset !== cursor) throw outOfOrder('fetch offset', cursor);
      const now = await this.#databaseNow(client);
      const fetchId = randomUUID();
      const inserted = await client.query(`
        INSERT INTO mq_fetch_receipts (
          fetch_id, group_id, request_key, intent_digest, member_id, generation,
          requested_offset, frozen_log_start, frozen_high_watermark,
          max_records, max_bytes, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'accepted', $12)
        RETURNING *
      `, [
        fetchId,
        groupId,
        requestKey,
        intentDigest,
        memberId,
        generation,
        offset,
        logStart,
        highWatermark,
        maxRecords,
        maxBytes,
        now,
      ]);
      await client.query('COMMIT');
      return fetchFromRow(inserted.rows[0], false);
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async listBatches({ startOffset, upperOffset }) {
    const result = await this.pool.query(`
      SELECT batch.*,
        ARRAY(
          SELECT DISTINCT receipt.replica_id
          FROM mq_replica_receipts AS receipt
          WHERE receipt.intent_id = batch.intent_id
            AND receipt.leader_epoch = batch.committed_leader_epoch
            AND receipt.batch_digest = batch.batch_digest
            AND receipt.batch_bytes = batch.batch_bytes
          ORDER BY receipt.replica_id
        ) AS replica_ids
      FROM mq_batches AS batch
      WHERE batch.topic = $1 AND batch.partition_id = $2
        AND batch.next_offset > $3 AND batch.base_offset < $4
      ORDER BY batch.base_offset
    `, [queueContract.topic, queueContract.partition, startOffset, upperOffset]);
    return result.rows.map((row) => ({
      baseOffset: integer(row.base_offset),
      nextOffset: integer(row.next_offset),
      recordCount: integer(row.record_count),
      batchDigest: row.batch_digest,
      batchBytes: integer(row.batch_bytes),
      replicaIds: row.replica_ids,
    }));
  }

  async completeFetch({ fetchId, returnedNextOffset, resultCount }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const found = await client.query(`SELECT * FROM mq_fetch_receipts WHERE fetch_id = $1 FOR UPDATE`, [fetchId]);
      if (!found.rowCount) throw notFound();
      const row = found.rows[0];
      if (row.status === 'complete') {
        await client.query('COMMIT');
        return { receipt: fetchFromRow(row, true), revoked: false };
      }
      if (row.status === 'revoked') {
        await client.query('COMMIT');
        return { receipt: fetchFromRow(row, true), revoked: true };
      }
      if (
        returnedNextOffset < integer(row.requested_offset)
        || returnedNextOffset > integer(row.frozen_high_watermark)
        || resultCount < 0
        || resultCount > integer(row.max_records)
      ) throw conflict('fetch result is outside its frozen boundary');
      const group = await client.query(`SELECT * FROM mq_groups WHERE group_id = $1 FOR UPDATE`, [row.group_id]);
      const currentGeneration = integer(group.rows[0].current_generation);
      const now = await this.#databaseNow(client);
      if (currentGeneration !== integer(row.generation) || group.rows[0].current_member_id !== row.member_id) {
        const updated = await client.query(`
          UPDATE mq_fetch_receipts
          SET status = 'revoked', completed_at = $2
          WHERE fetch_id = $1
          RETURNING *
        `, [fetchId, now]);
        await client.query('COMMIT');
        return { receipt: fetchFromRow(updated.rows[0], false), revoked: true, currentGeneration };
      }
      const assignment = await client.query(`
        SELECT * FROM mq_group_assignments
        WHERE group_id = $1 AND generation = $2 AND status = 'current'
        FOR UPDATE
      `, [row.group_id, integer(row.generation)]);
      if (!assignment.rowCount || integer(assignment.rows[0].fetch_cursor) !== integer(row.requested_offset)) {
        throw conflict('assignment cursor changed while fetch was in flight');
      }
      await client.query(`
        UPDATE mq_group_assignments
        SET fetch_cursor = $3
        WHERE group_id = $1 AND generation = $2
      `, [row.group_id, integer(row.generation), returnedNextOffset]);
      const updated = await client.query(`
        UPDATE mq_fetch_receipts
        SET status = 'complete', returned_next_offset = $2, result_count = $3, completed_at = $4
        WHERE fetch_id = $1
        RETURNING *
      `, [fetchId, returnedNextOffset, resultCount, now]);
      await client.query('COMMIT');
      return { receipt: fetchFromRow(updated.rows[0], false), revoked: false };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async checkpoint({ groupId, memberId, generation, requestKey, intentDigest, nextOffset }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `checkpoint:${groupId.length}:${groupId}:${requestKey.length}:${requestKey}`);
      const existing = await client.query(`
        SELECT * FROM mq_checkpoint_receipts WHERE group_id = $1 AND request_key = $2
      `, [groupId, requestKey]);
      if (existing.rowCount) {
        if (existing.rows[0].intent_digest !== intentDigest) throw conflict();
        await client.query('COMMIT');
        return { ...existing.rows[0].result_json, replayed: true };
      }
      const group = await client.query(`SELECT * FROM mq_groups WHERE group_id = $1 FOR UPDATE`, [groupId]);
      if (!group.rowCount) throw notFound();
      const currentGeneration = integer(group.rows[0].current_generation);
      if (currentGeneration !== generation || group.rows[0].current_member_id !== memberId) {
        throw fenced('group generation', currentGeneration);
      }
      const assignment = await client.query(`
        SELECT * FROM mq_group_assignments
        WHERE group_id = $1 AND generation = $2 AND status = 'current'
        FOR UPDATE
      `, [groupId, generation]);
      if (!assignment.rowCount || assignment.rows[0].member_id !== memberId) throw fenced('group generation', currentGeneration);
      const partition = await this.#lockPartition(client, 'SHARE');
      const logStart = integer(partition.log_start_offset);
      const highWatermark = integer(partition.high_watermark);
      const currentOffset = integer(group.rows[0].checkpoint_offset);
      if (currentOffset < logStart || nextOffset < logStart) throw offsetOutOfRange(logStart, highWatermark);
      if (nextOffset < currentOffset || nextOffset > integer(assignment.rows[0].fetch_cursor) || nextOffset > highWatermark) {
        throw conflict('checkpoint is outside the fetched monotonic range');
      }
      const now = await this.#databaseNow(client);
      await client.query(`
        UPDATE mq_groups SET checkpoint_offset = $2, updated_at = $3 WHERE group_id = $1
      `, [groupId, nextOffset, now]);
      const result = {
        phase: 'offset_committed',
        groupId,
        memberId,
        generation,
        previousOffset: currentOffset,
        nextOffset,
      };
      await client.query(`
        INSERT INTO mq_checkpoint_receipts (
          group_id, request_key, intent_digest, member_id, generation,
          next_offset, result_json, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
      `, [groupId, requestKey, intentDigest, memberId, generation, nextOffset, JSON.stringify(result), now]);
      await client.query('COMMIT');
      return { ...result, replayed: false };
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async advanceRetention({ principal, requestKey, intentDigest, expectedRevision, newLogStart }) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await this.#advisory(client, `mutation:${principal.length}:${principal}:${requestKey.length}:${requestKey}`);
      let result = await this.#existingMutation(client, principal, requestKey, intentDigest);
      if (result) {
        await client.query('COMMIT');
        return result;
      }
      const partition = await this.#lockPartition(client);
      const currentRevision = integer(partition.state_revision);
      if (currentRevision !== expectedRevision) throw precondition(currentRevision);
      const currentStart = integer(partition.log_start_offset);
      const highWatermark = integer(partition.high_watermark);
      if (newLogStart < currentStart || newLogStart > highWatermark) {
        throw conflict('retention boundary must advance within the committed range');
      }
      const nextRevision = currentRevision + 1;
      const now = await this.#databaseNow(client);
      await client.query(`
        UPDATE mq_partitions
        SET log_start_offset = $3, state_revision = $4, updated_at = $5
        WHERE topic = $1 AND partition_id = $2
      `, [queueContract.topic, queueContract.partition, newLogStart, nextRevision, now]);
      result = await this.#recordMutation(client, {
        principal,
        requestKey,
        intentDigest,
        operation: 'advance_retention',
        result: {
          operation: 'advance_retention',
          previousLogStart: currentStart,
          logStartOffset: newLogStart,
          highWatermark,
          stateRevision: nextRevision,
        },
        now,
      });
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await rollback(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async getGroup(groupId) {
    const result = await this.pool.query(`
      SELECT group_state.*, assignment.fetch_cursor, assignment.status AS assignment_status
      FROM mq_groups AS group_state
      LEFT JOIN mq_group_assignments AS assignment
        ON assignment.group_id = group_state.group_id
       AND assignment.generation = group_state.current_generation
      WHERE group_state.group_id = $1
    `, [groupId]);
    if (!result.rowCount) throw notFound();
    const row = result.rows[0];
    return {
      groupId: row.group_id,
      generation: integer(row.current_generation),
      memberId: row.current_member_id,
      checkpointOffset: integer(row.checkpoint_offset),
      fetchCursor: row.fetch_cursor === null ? null : integer(row.fetch_cursor),
      assignmentStatus: row.assignment_status,
    };
  }
}
