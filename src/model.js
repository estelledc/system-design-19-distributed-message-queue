import { conflict, fenced, insufficientReplicas, offsetOutOfRange, outOfOrder, partitionBusy } from './errors.js';

export class QueueModel {
  constructor() {
    this.leaderEpoch = 1;
    this.highWatermark = 0;
    this.nextOffset = 0;
    this.logStart = 0;
    this.active = null;
    this.producers = new Map();
    this.committed = [];
    this.groups = new Map();
  }

  createProducer(id) {
    if (!this.producers.has(id)) this.producers.set(id, { epoch: 0, nextSequence: 0, receipts: new Map() });
    return { ...this.producers.get(id) };
  }

  bumpProducer(id, expectedEpoch) {
    const producer = this.producers.get(id);
    if (!producer) throw conflict('producer is missing');
    if (this.active?.producerId === id) throw partitionBusy();
    if (producer.epoch !== expectedEpoch) throw fenced('producer epoch', producer.epoch);
    producer.epoch += 1;
    producer.nextSequence = 0;
    return producer.epoch;
  }

  reserve({ producerId, epoch, sequence, count, digest }) {
    const producer = this.producers.get(producerId);
    if (!producer) throw conflict('producer is missing');
    const identity = `${epoch}:${sequence}`;
    const existing = producer.receipts.get(identity);
    if (existing) {
      if (existing.digest !== digest || existing.count !== count) throw conflict();
      return existing;
    }
    if (producer.epoch !== epoch) throw fenced('producer epoch', producer.epoch);
    if (producer.nextSequence !== sequence) throw outOfOrder('producer sequence', producer.nextSequence);
    if (this.active) throw partitionBusy();
    const intent = {
      producerId,
      epoch,
      sequence,
      count,
      digest,
      baseOffset: this.highWatermark,
      status: 'replicating',
      receiptsByEpoch: new Map(),
    };
    producer.receipts.set(identity, intent);
    this.active = intent;
    this.nextOffset = this.highWatermark + count;
    return intent;
  }

  replicate(expectedEpoch, replicaId) {
    if (expectedEpoch !== this.leaderEpoch) throw fenced('leader epoch', this.leaderEpoch);
    if (!this.active) throw conflict('there is no active intent');
    const receipts = this.active.receiptsByEpoch.get(expectedEpoch) ?? new Set();
    receipts.add(replicaId);
    this.active.receiptsByEpoch.set(expectedEpoch, receipts);
    return receipts.size;
  }

  rotateLeader(expectedEpoch) {
    if (expectedEpoch !== this.leaderEpoch) throw fenced('leader epoch', this.leaderEpoch);
    this.leaderEpoch += 1;
    return this.leaderEpoch;
  }

  commit(expectedEpoch) {
    if (expectedEpoch !== this.leaderEpoch) throw fenced('leader epoch', this.leaderEpoch);
    if (!this.active) throw conflict('there is no active intent');
    const receipts = this.active.receiptsByEpoch.get(expectedEpoch)?.size ?? 0;
    if (receipts < 2) throw insufficientReplicas(2, receipts);
    const producer = this.producers.get(this.active.producerId);
    this.active.status = 'committed';
    this.active.nextOffset = this.active.baseOffset + this.active.count;
    this.committed.push(this.active);
    this.highWatermark = this.active.nextOffset;
    this.nextOffset = this.highWatermark;
    producer.nextSequence = this.active.sequence + this.active.count;
    const result = this.active;
    this.active = null;
    return result;
  }

  join(groupId, memberId) {
    const group = this.groups.get(groupId) ?? { generation: 0, memberId: null, checkpoint: this.logStart, cursor: this.logStart };
    group.generation += 1;
    group.memberId = memberId;
    group.cursor = group.checkpoint;
    this.groups.set(groupId, group);
    return { ...group };
  }

  fetch(groupId, memberId, generation, offset, maximum) {
    const group = this.groups.get(groupId);
    if (!group) throw conflict('group is missing');
    if (group.generation !== generation || group.memberId !== memberId) throw fenced('group generation', group.generation);
    if (group.checkpoint < this.logStart || offset < this.logStart || offset > this.highWatermark) {
      throw offsetOutOfRange(this.logStart, this.highWatermark);
    }
    if (offset !== group.cursor) throw outOfOrder('fetch offset', group.cursor);
    const nextOffset = Math.min(this.highWatermark, offset + maximum);
    group.cursor = nextOffset;
    return { offset, nextOffset };
  }

  checkpoint(groupId, memberId, generation, nextOffset) {
    const group = this.groups.get(groupId);
    if (!group) throw conflict('group is missing');
    if (group.generation !== generation || group.memberId !== memberId) throw fenced('group generation', group.generation);
    if (group.checkpoint < this.logStart || nextOffset < this.logStart) {
      throw offsetOutOfRange(this.logStart, this.highWatermark);
    }
    if (nextOffset < group.checkpoint || nextOffset > group.cursor || nextOffset > this.highWatermark) {
      throw conflict('checkpoint is outside the fetched range');
    }
    group.checkpoint = nextOffset;
    return nextOffset;
  }

  retain(newLogStart) {
    if (newLogStart < this.logStart || newLogStart > this.highWatermark) throw conflict('invalid retention boundary');
    this.logStart = newLogStart;
  }

  assertInvariants() {
    if (!(0 <= this.logStart && this.logStart <= this.highWatermark && this.highWatermark <= this.nextOffset)) {
      throw new Error('offset range invariant failed');
    }
    if (!this.active && this.nextOffset !== this.highWatermark) throw new Error('idle partition contains an offset hole');
    if (this.active && this.nextOffset !== this.active.baseOffset + this.active.count) {
      throw new Error('active intent reservation invariant failed');
    }
    let expected = 0;
    for (const batch of this.committed) {
      if (batch.baseOffset !== expected || batch.status !== 'committed') throw new Error('committed prefix is not contiguous');
      expected = batch.nextOffset;
    }
    if (expected !== this.highWatermark) throw new Error('high watermark disagrees with committed batches');
    for (const group of this.groups.values()) {
      if (group.checkpoint > group.cursor || group.cursor > this.highWatermark) throw new Error('group cursor invariant failed');
    }
  }
}
