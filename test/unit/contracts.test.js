import assert from 'node:assert/strict';
import test from 'node:test';
import {
  canonicalBytes,
  decodeBatch,
  digestValue,
  normalizeFetch,
  normalizePublish,
  normalizeRetention,
} from '../../src/contracts.js';

function publish(overrides = {}) {
  return {
    producerId: 'producer-a',
    producerEpoch: 0,
    baseSequence: 0,
    records: [{ key: 'key-a', headers: [{ name: 'kind', value: 'alpha' }], value: 'value-a' }],
    ...overrides,
  };
}

test('canonical publish normalizes header order and is byte stable', () => {
  const first = normalizePublish(publish({
    records: [{
      key: 'key-a',
      headers: [{ name: 'z-last', value: '2' }, { name: 'a-first', value: '1' }],
      value: 'cafe\u0301',
    }],
  }));
  const second = normalizePublish(publish({
    records: [{
      value: 'café',
      headers: [{ value: '1', name: 'a-first' }, { value: '2', name: 'z-last' }],
      key: 'key-a',
    }],
  }));
  assert.equal(first.digest, second.digest);
  assert.deepEqual(first.batch, second.batch);
});

test('publish identity changes when record bytes change', () => {
  assert.notEqual(normalizePublish(publish()).digest, normalizePublish(publish({
    records: [{ key: 'key-a', headers: [{ name: 'kind', value: 'alpha' }], value: 'value-b' }],
  })).digest);
});

test('stored batch decoder rejects non-canonical and digest-mismatched bytes', () => {
  const normalized = normalizePublish(publish());
  assert.deepEqual(decodeBatch(normalized.bytes, normalized.digest), normalized.batch);
  assert.throws(() => decodeBatch(Buffer.from(JSON.stringify(normalized.batch)), normalized.digest), { code: 'replica_corrupt_or_missing' });
  assert.throws(() => decodeBatch(normalized.bytes, '0'.repeat(64)), { code: 'replica_corrupt_or_missing' });
});

test('contracts reject unknown fields, duplicate headers, controls, and unsafe integers', () => {
  assert.throws(() => normalizePublish({ ...publish(), surprise: true }), { code: 'invalid_request' });
  assert.throws(() => normalizePublish(publish({
    records: [{ key: 'a', headers: [{ name: 'same', value: '1' }, { name: 'same', value: '2' }], value: 'x' }],
  })), { code: 'invalid_request' });
  assert.throws(() => normalizePublish(publish({
    records: [{ key: 'a', headers: [], value: 'bad\u0000value' }],
  })), { code: 'invalid_request' });
  assert.throws(() => normalizePublish(publish({ baseSequence: Number.MAX_SAFE_INTEGER + 1 })), { code: 'invalid_request' });
});

test('fetch and retention bounds fail closed', () => {
  assert.throws(() => normalizeFetch({ memberId: 'm', generation: 1, offset: 0, maxRecords: 1, maxBytes: 1024 }), {
    code: 'invalid_request',
  });
  assert.throws(() => normalizeRetention({ expectedRevision: 0, newLogStart: -1 }), { code: 'invalid_request' });
});

test('canonical JSON sorts object keys recursively', () => {
  assert.equal(canonicalBytes({ z: 1, a: { y: 2, b: 3 } }).toString('utf8'), '{"a":{"b":3,"y":2},"z":1}\n');
  assert.equal(digestValue({ b: 2, a: 1 }), digestValue({ a: 1, b: 2 }));
});
