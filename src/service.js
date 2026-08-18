import {
  decodeBatch,
  digestValue,
  normalizeCheckpoint,
  normalizeFetch,
  normalizeJoin,
  normalizeLeader,
  normalizeProducer,
  normalizeProducerEpoch,
  normalizePublish,
  normalizeRetention,
  queueContract,
  recordWireBytes,
  validateIdentifier,
  validateRequestKey,
} from './contracts.js';
import { corrupted, fenced, insufficientReplicas, offsetOutOfRange } from './errors.js';

function maybeCrash(name) {
  if (process.env[name] === '1') process.kill(process.pid, 'SIGKILL');
}

export class QueueService {
  constructor(repository, replicaSet, logger = () => {}) {
    this.repository = repository;
    this.replicaSet = replicaSet;
    this.logger = logger;
  }

  async createProducer(body) {
    const input = normalizeProducer(body);
    const result = await this.repository.createProducer(input.producerId);
    this.logger({ event: 'producer_ready', created: result.created });
    return result;
  }

  async bumpProducerEpoch({ principal, requestKey, producerId, body }) {
    validateRequestKey(requestKey);
    normalizeProducer({ producerId });
    const input = normalizeProducerEpoch(body);
    const intentDigest = digestValue({ operation: 'bump_producer_epoch', producerId, ...input });
    const result = await this.repository.bumpProducerEpoch({
      principal,
      requestKey,
      intentDigest,
      producerId,
      expectedEpoch: input.expectedEpoch,
    });
    this.logger({ event: 'producer_epoch_bumped', replayed: result.replayed });
    return result;
  }

  async #verifiedReplicaIds(intentId, expectedLeaderEpoch, expectedDigest, expectedBytes) {
    const receipts = await this.repository.currentReplicaReceipts({ intentId, expectedLeaderEpoch });
    const verified = [];
    for (const receipt of receipts) {
      if (receipt.batchDigest !== expectedDigest || receipt.batchBytes !== expectedBytes) continue;
      try {
        const bytes = await this.replicaSet.read(receipt.replicaId, expectedDigest);
        if (bytes.length === expectedBytes) verified.push(receipt.replicaId);
      } catch {
        this.logger({ event: 'replica_read_failed', failureClass: 'corrupt_or_missing' });
      }
    }
    return verified;
  }

  async publish({ expectedLeaderEpoch, body, replicaIds = queueContract.replicaIds }) {
    const input = normalizePublish(body);
    const intent = await this.repository.reservePublish({
      producerId: input.producerId,
      producerEpoch: input.producerEpoch,
      baseSequence: input.baseSequence,
      recordCount: input.records.length,
      batchDigest: input.digest,
      bytes: input.bytes,
      expectedLeaderEpoch,
    });
    if (intent.status === 'committed') {
      this.logger({ event: 'publish_replayed', phase: 'committed' });
      return { ...intent.result, replayed: true };
    }
    this.logger({ event: 'publish_intent_reserved', recordCount: input.records.length, replayed: intent.replayed });

    let receiptsRecorded = 0;
    for (const replicaId of [...new Set(replicaIds)]) {
      try {
        const work = await this.repository.loadReplicationWork({ intentId: intent.intentId, expectedLeaderEpoch });
        if (work.status === 'committed') return { ...work.result, replayed: true };
        const object = await this.replicaSet.put(replicaId, work.bytes);
        const receipt = await this.repository.recordReplicaReceipt({
          intentId: intent.intentId,
          expectedLeaderEpoch,
          replicaId,
          batchDigest: object.digest,
          batchBytes: object.bytes,
        });
        receiptsRecorded = receipt.verifiedReplicas;
        this.logger({ event: 'replica_verified', verifiedReplicaCount: receiptsRecorded });
        if (receiptsRecorded === 1) maybeCrash('MQ_CRASH_AFTER_FIRST_REPLICA_RECEIPT');
      } catch (error) {
        if (error?.code === 'fenced') throw error;
        this.logger({ event: 'replica_write_failed', failureClass: error?.code ?? 'write_failed' });
      }
    }

    const verifiedReplicaIds = await this.#verifiedReplicaIds(
      intent.intentId,
      expectedLeaderEpoch,
      input.digest,
      input.bytes.length,
    );
    if (verifiedReplicaIds.length < queueContract.requiredReplicas) {
      throw insufficientReplicas(queueContract.requiredReplicas, verifiedReplicaIds.length);
    }
    const result = await this.repository.commitPublish({
      intentId: intent.intentId,
      expectedLeaderEpoch,
      verifiedReplicaIds,
    });
    this.logger({ event: 'batch_committed', recordCount: input.records.length, replicaCount: result.replicaCount });
    maybeCrash('MQ_CRASH_AFTER_PUBLISH_COMMIT');
    return result;
  }

  async rotateLeader({ principal, requestKey, body }) {
    validateRequestKey(requestKey);
    const input = normalizeLeader(body);
    const result = await this.repository.rotateLeader({
      principal,
      requestKey,
      intentDigest: digestValue({ operation: 'rotate_leader', ...input }),
      ...input,
    });
    this.logger({ event: 'leader_rotated', replayed: result.replayed });
    return result;
  }

  async joinGroup({ groupId, requestKey, body }) {
    validateRequestKey(requestKey);
    validateIdentifier(groupId, 'groupId');
    const input = normalizeJoin(body);
    const result = await this.repository.joinGroup({
      groupId,
      memberId: input.memberId,
      requestKey,
      intentDigest: digestValue({ operation: 'join_group', groupId, ...input }),
    });
    this.logger({ event: 'group_assignment_created', replayed: result.replayed, assignmentStatus: result.assignmentStatus });
    return result;
  }

  async #collectRecords({ startOffset, upperOffset, maxRecords, maxBytes, stopOffset = null }) {
    const effectiveUpper = stopOffset === null ? upperOffset : Math.min(upperOffset, stopOffset);
    if (startOffset === effectiveUpper) return { records: [], nextOffset: startOffset, replicaFailures: 0 };
    const batches = await this.repository.listBatches({ startOffset, upperOffset: effectiveUpper });
    const records = [];
    let cursor = startOffset;
    let usedBytes = 0;
    let replicaFailures = 0;
    let limitReached = false;

    for (const metadata of batches) {
      if (cursor >= effectiveUpper || records.length >= maxRecords) break;
      if (metadata.baseOffset > cursor || metadata.nextOffset <= cursor) throw corrupted();
      const object = await this.replicaSet.readAny(metadata.batchDigest, metadata.replicaIds);
      replicaFailures += object.failures.length;
      const batch = decodeBatch(object.bytes, metadata.batchDigest);
      if (batch.records.length !== metadata.recordCount) throw corrupted();
      const firstIndex = cursor - metadata.baseOffset;
      for (let index = firstIndex; index < batch.records.length; index += 1) {
        const offset = metadata.baseOffset + index;
        if (offset >= effectiveUpper || records.length >= maxRecords) break;
        const record = { offset, ...batch.records[index] };
        const bytes = recordWireBytes(record);
        if (usedBytes + bytes > maxBytes) {
          limitReached = true;
          break;
        }
        records.push(record);
        usedBytes += bytes;
        cursor = offset + 1;
      }
      if (cursor < metadata.nextOffset && (records.length >= maxRecords || limitReached)) break;
    }
    if (cursor < effectiveUpper && records.length < maxRecords && !limitReached) throw corrupted();
    return { records, nextOffset: cursor, replicaFailures };
  }

  async fetch({ groupId, requestKey, body }) {
    validateRequestKey(requestKey);
    validateIdentifier(groupId, 'groupId');
    const input = normalizeFetch(body);
    const intentDigest = digestValue({ operation: 'fetch', groupId, ...input });
    const receipt = await this.repository.beginFetch({ groupId, requestKey, intentDigest, ...input });

    const group = await this.repository.getGroup(groupId);
    if (group.generation !== input.generation || group.memberId !== input.memberId) {
      throw fenced('group generation', group.generation);
    }
    const state = await this.repository.getState();
    if (receipt.requestedOffset < state.logStartOffset) throw offsetOutOfRange(state.logStartOffset, state.highWatermark);
    if (receipt.status === 'revoked') throw fenced('group generation', group.generation);

    const collected = await this.#collectRecords({
      startOffset: receipt.requestedOffset,
      upperOffset: receipt.frozenHighWatermark,
      maxRecords: receipt.maxRecords,
      maxBytes: receipt.maxBytes,
      stopOffset: receipt.status === 'complete' ? receipt.returnedNextOffset : null,
    });
    if (receipt.status === 'complete') {
      if (collected.nextOffset !== receipt.returnedNextOffset || collected.records.length !== receipt.resultCount) throw corrupted();
      this.logger({ event: 'fetch_replayed', recordCount: collected.records.length });
      return {
        phase: 'fetch_returned',
        groupId,
        memberId: input.memberId,
        generation: input.generation,
        requestedOffset: receipt.requestedOffset,
        frozenHighWatermark: receipt.frozenHighWatermark,
        nextOffset: receipt.returnedNextOffset,
        records: collected.records,
        replayed: true,
      };
    }

    const completed = await this.repository.completeFetch({
      fetchId: receipt.fetchId,
      returnedNextOffset: collected.nextOffset,
      resultCount: collected.records.length,
    });
    if (completed.revoked) throw fenced('group generation', completed.currentGeneration);
    this.logger({
      event: 'fetch_returned',
      recordCount: collected.records.length,
      replicaFailureCount: collected.replicaFailures,
    });
    return {
      phase: 'fetch_returned',
      groupId,
      memberId: input.memberId,
      generation: input.generation,
      requestedOffset: receipt.requestedOffset,
      frozenHighWatermark: receipt.frozenHighWatermark,
      nextOffset: collected.nextOffset,
      records: collected.records,
      replayed: false,
    };
  }

  async checkpoint({ groupId, requestKey, body }) {
    validateRequestKey(requestKey);
    validateIdentifier(groupId, 'groupId');
    const input = normalizeCheckpoint(body);
    const result = await this.repository.checkpoint({
      groupId,
      requestKey,
      intentDigest: digestValue({ operation: 'checkpoint', groupId, ...input }),
      ...input,
    });
    this.logger({ event: 'offset_committed', replayed: result.replayed });
    maybeCrash('MQ_CRASH_AFTER_CHECKPOINT_COMMIT');
    return result;
  }

  async advanceRetention({ principal, requestKey, body }) {
    validateRequestKey(requestKey);
    const input = normalizeRetention(body);
    const result = await this.repository.advanceRetention({
      principal,
      requestKey,
      intentDigest: digestValue({ operation: 'advance_retention', ...input }),
      ...input,
    });
    this.logger({ event: 'retention_advanced', replayed: result.replayed });
    return result;
  }
}
