import assert from 'node:assert/strict';
import test from 'node:test';
import { createQueueServer } from '../../src/http.js';

async function fixture(t, overrides = {}) {
  const logs = [];
  const service = {
    createProducer: async (body) => ({ ...body, epoch: 0, nextSequence: 0, created: true }),
    publish: async () => ({ phase: 'batch_committed', replayed: false }),
    bumpProducerEpoch: async () => ({ phase: 'epoch_bumped' }),
    rotateLeader: async () => ({ phase: 'leader_rotated' }),
    joinGroup: async () => ({ phase: 'assignment_created', replayed: false }),
    fetch: async () => ({ phase: 'fetch_returned', records: [] }),
    checkpoint: async () => ({ phase: 'offset_committed' }),
    advanceRetention: async () => ({ phase: 'retention_advanced' }),
    ...overrides,
  };
  const repository = {
    getState: async () => ({ leaderEpoch: 1, highWatermark: 0 }),
    getGroup: async (groupId) => ({ groupId, generation: 1 }),
  };
  const server = createQueueServer({ service, repository, apiToken: 'synthetic-token-1234', logger: (entry) => logs.push(entry) });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const { port } = server.address();
  return { base: `http://127.0.0.1:${port}`, logs };
}

async function request(base, path, { method = 'GET', body, headers = {}, auth = true } = {}) {
  return fetch(`${base}${path}`, {
    method,
    headers: {
      ...(auth ? { authorization: 'Bearer synthetic-token-1234' } : {}),
      ...(body ? { 'content-type': 'application/json' } : {}),
      ...headers,
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
}

test('health is public while queue state requires exact bearer authentication', async (t) => {
  const { base } = await fixture(t);
  assert.equal((await request(base, '/health', { auth: false })).status, 200);
  assert.equal((await request(base, '/v1/state', { auth: false })).status, 401);
  assert.equal((await request(base, '/v1/state')).status, 200);
});

test('publish requires a positive leader epoch and returns no-store JSON', async (t) => {
  const { base } = await fixture(t);
  const body = { producerId: 'p', producerEpoch: 0, baseSequence: 0, records: [] };
  assert.equal((await request(base, '/v1/records', { method: 'POST', body })).status, 400);
  const response = await request(base, '/v1/records', {
    method: 'POST',
    body,
    headers: { 'x-leader-epoch': '1' },
  });
  assert.equal(response.status, 201);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.match(response.headers.get('content-type'), /^application\/json/);
});

test('group routes decode IDs and require stable mutation keys', async (t) => {
  const { base } = await fixture(t);
  const missing = await request(base, '/v1/groups/group-a/members', { method: 'POST', body: { memberId: 'member-a' } });
  assert.equal(missing.status, 400);
  const joined = await request(base, '/v1/groups/group-a/members', {
    method: 'POST',
    body: { memberId: 'member-a' },
    headers: { 'idempotency-key': 'join-key-0001' },
  });
  assert.equal(joined.status, 201);
  assert.equal((await request(base, '/v1/groups/group-a')).status, 200);
});

test('unexpected failures return a generic body and never expose exception text', async (t) => {
  const { base, logs } = await fixture(t, {
    createProducer: async () => { throw new Error('private-sentinel-body'); },
  });
  const response = await request(base, '/v1/producers', { method: 'POST', body: { producerId: 'p' } });
  assert.equal(response.status, 500);
  const text = await response.text();
  assert.doesNotMatch(text, /private-sentinel-body/);
  assert.doesNotMatch(JSON.stringify(logs), /private-sentinel-body/);
});
