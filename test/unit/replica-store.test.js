import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { sha256 } from '../../src/crypto.js';
import { ReplicaSet } from '../../src/replica-store.js';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'mq-replicas-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  return { root, store: new ReplicaSet(root) };
}

test('replica writes are immutable, synced, and exactly replayable', async (t) => {
  const { store } = await fixture(t);
  const bytes = Buffer.from('canonical-batch\n');
  const first = await store.put('replica-a', bytes);
  const second = await store.put('replica-a', bytes);
  assert.deepEqual(second, first);
  assert.deepEqual(await store.read('replica-a', first.digest), bytes);
});

test('concurrent same-byte writers converge without replacement', async (t) => {
  const { store } = await fixture(t);
  const bytes = Buffer.alloc(4096, 7);
  const results = await Promise.all(Array.from({ length: 12 }, () => store.put('replica-b', bytes)));
  assert.equal(new Set(results.map((result) => result.digest)).size, 1);
  assert.equal((await readFile(store.objectPath('replica-b', results[0].digest))).length, bytes.length);
});

test('corruption is rejected and another replica can serve', async (t) => {
  const { store } = await fixture(t);
  const bytes = Buffer.from('surviving-batch\n');
  const object = await store.put('replica-a', bytes);
  await store.put('replica-b', bytes);
  await writeFile(store.objectPath('replica-a', object.digest), 'corrupt');
  await assert.rejects(store.read('replica-a', object.digest), { code: 'replica_corrupt_or_missing' });
  const recovered = await store.readAny(object.digest, ['replica-a', 'replica-b']);
  assert.equal(recovered.replicaId, 'replica-b');
  assert.deepEqual(recovered.bytes, bytes);
  assert.equal(recovered.failures.length, 1);
});

test('invalid replica and digest identities fail before filesystem access', async (t) => {
  const { store } = await fixture(t);
  await assert.rejects(store.put('../escape', Buffer.from('x')), { code: 'invalid_request' });
  assert.throws(() => store.objectPath('replica-a', sha256('x').slice(1)), /invalid object digest/);
});
