import assert from 'node:assert/strict';
import test from 'node:test';
import { QueueModel } from '../../src/model.js';

test('committed prefix waits for two current-epoch replica receipts', () => {
  const model = new QueueModel();
  model.createProducer('p');
  model.reserve({ producerId: 'p', epoch: 0, sequence: 0, count: 2, digest: 'a' });
  model.replicate(1, 'replica-a');
  assert.throws(() => model.commit(1), { code: 'insufficient_replicas' });
  assert.equal(model.highWatermark, 0);
  model.rotateLeader(1);
  model.replicate(2, 'replica-a');
  assert.throws(() => model.commit(1), { code: 'fenced' });
  model.replicate(2, 'replica-b');
  model.commit(2);
  assert.equal(model.highWatermark, 2);
  model.assertInvariants();
});

test('producer replay and group takeover preserve identity while fencing stale progress', () => {
  const model = new QueueModel();
  model.createProducer('p');
  const first = model.reserve({ producerId: 'p', epoch: 0, sequence: 0, count: 1, digest: 'a' });
  assert.equal(model.reserve({ producerId: 'p', epoch: 0, sequence: 0, count: 1, digest: 'a' }), first);
  assert.throws(() => model.reserve({ producerId: 'p', epoch: 0, sequence: 0, count: 1, digest: 'b' }), { code: 'conflict' });
  model.replicate(1, 'replica-a');
  model.replicate(1, 'replica-b');
  model.commit(1);
  const firstMember = model.join('g', 'm1');
  model.fetch('g', 'm1', firstMember.generation, 0, 1);
  const secondMember = model.join('g', 'm2');
  assert.throws(() => model.checkpoint('g', 'm1', firstMember.generation, 1), { code: 'fenced' });
  assert.deepEqual(model.fetch('g', 'm2', secondMember.generation, 0, 1), { offset: 0, nextOffset: 1 });
  model.checkpoint('g', 'm2', secondMember.generation, 1);
  model.assertInvariants();
});

test('randomized state-machine oracle preserves prefix, epoch, generation, and retention invariants', () => {
  let state = 0x9e3779b9;
  const random = (maximum) => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) % maximum;
  };
  for (let fixture = 0; fixture < 200; fixture += 1) {
    const model = new QueueModel();
    model.createProducer('p');
    let memberSerial = 0;
    for (let step = 0; step < 120; step += 1) {
      try {
        switch (random(8)) {
          case 0:
            model.reserve({
              producerId: 'p',
              epoch: model.producers.get('p').epoch,
              sequence: model.producers.get('p').nextSequence,
              count: random(4) + 1,
              digest: `d-${fixture}-${step}`,
            });
            break;
          case 1:
            if (model.active) model.replicate(model.leaderEpoch, `replica-${String.fromCharCode(97 + random(3))}`);
            break;
          case 2:
            if (model.active) model.commit(model.leaderEpoch);
            break;
          case 3:
            model.rotateLeader(model.leaderEpoch);
            break;
          case 4:
            memberSerial += 1;
            model.join('g', `m-${memberSerial}`);
            break;
          case 5: {
            const group = model.groups.get('g');
            if (group) model.fetch('g', group.memberId, group.generation, group.cursor, random(4) + 1);
            break;
          }
          case 6: {
            const group = model.groups.get('g');
            if (group) model.checkpoint('g', group.memberId, group.generation, group.cursor);
            break;
          }
          case 7:
            model.retain(model.logStart + random(model.highWatermark - model.logStart + 1));
            break;
          default:
            throw new Error('unreachable random action');
        }
      } catch (error) {
        assert.ok(['conflict', 'fenced', 'insufficient_replicas', 'offset_out_of_range', 'out_of_order', 'partition_busy'].includes(error.code));
      }
      model.assertInvariants();
    }
  }
});
