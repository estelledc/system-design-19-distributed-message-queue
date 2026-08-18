import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFile, readdir, stat } from 'node:fs/promises';
import { dirname, join, normalize } from 'node:path';

const root = new URL('..', import.meta.url);
const expected = [
  '.github/dependabot.yml',
  '.github/workflows/ci.yml',
  '.gitignore',
  '.node-version',
  'AGENTS.md',
  'LICENSE',
  'README.md',
  'SECURITY.md',
  'compose.yaml',
  'docs/adr/0001-postgres-fenced-quorum-batches-and-generation-checkpoints.md',
  'docs/api.md',
  'docs/architecture.md',
  'docs/closed-book-contract.md',
  'docs/operations.md',
  'docs/requirements.md',
  'docs/research-log.md',
  'docs/threat-model.md',
  'docs/verification.md',
  'package-lock.json',
  'package.json',
  'scripts/infra-benchmark.mjs',
  'scripts/infra-smoke.mjs',
  'scripts/repo-check.mjs',
  'sql/schema.sql',
  'src/contracts.js',
  'src/crypto.js',
  'src/errors.js',
  'src/http.js',
  'src/index.js',
  'src/main.js',
  'src/model.js',
  'src/replica-store.js',
  'src/repository.js',
  'src/service.js',
  'test/integration/postgresql.test.js',
  'test/unit/contracts.test.js',
  'test/unit/http.test.js',
  'test/unit/model.test.js',
  'test/unit/replica-store.test.js',
];

async function walk(directory, prefix = '') {
  const paths = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.name === '.git' || entry.name === 'node_modules') continue;
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) paths.push(...await walk(new URL(`${entry.name}/`, directory), relative));
    else paths.push(relative);
  }
  return paths;
}

for (const path of expected) {
  assert.equal((await stat(new URL(path, root))).isFile(), true, `missing required file: ${path}`);
}

const packageJson = JSON.parse(await readFile(new URL('package.json', root), 'utf8'));
assert.equal(packageJson.private, true);
assert.equal(packageJson.engines.node, '>=22');
assert.deepEqual(packageJson.dependencies, { pg: '8.23.0' });
assert.equal(
  packageJson.repository.url,
  'git+https://github.com/estelledc/system-design-19-distributed-message-queue.git',
);
for (const script of ['lint', 'test', 'test:infra', 'smoke:infra', 'benchmark:infra', 'audit', 'check', 'check:ci']) {
  assert.equal(typeof packageJson.scripts[script], 'string', `missing package script: ${script}`);
}
const lock = JSON.parse(await readFile(new URL('package-lock.json', root), 'utf8'));
assert.equal(lock.lockfileVersion, 3);
assert.deepEqual(lock.packages[''].dependencies, packageJson.dependencies);

const workflow = await readFile(new URL('.github/workflows/ci.yml', root), 'utf8');
assert.match(workflow, /node: \[22, 24, 26\]/);
assert.match(workflow, /postgres:17\.11-alpine@sha256:[0-9a-f]{64}/);
assert.match(workflow, /permissions:\n  contents: read/);
const actionUses = [...workflow.matchAll(/uses: [^@\n]+@([^\s#]+)/g)].map((match) => match[1]);
assert.ok(actionUses.length >= 2);
assert.ok(actionUses.every((reference) => /^[0-9a-f]{40}$/.test(reference)), 'actions must use full commit pins');

const compose = await readFile(new URL('compose.yaml', root), 'utf8');
assert.match(compose, /postgres:17\.11-alpine@sha256:[0-9a-f]{64}/);
assert.match(compose, /pg_isready -U postgres -d message_queue/);

const schema = await readFile(new URL('sql/schema.sql', root), 'utf8');
for (const contract of [
  "status IN ('replicating', 'committed')",
  "status IN ('accepted', 'complete', 'revoked')",
  'max_bytes BETWEEN 8192 AND 1048576',
  'UNIQUE (producer_id, producer_epoch, base_sequence)',
  'CREATE UNIQUE INDEX IF NOT EXISTS mq_one_current_assignment',
  'CHECK (log_start_offset <= high_watermark AND high_watermark <= next_offset)',
  "operation IN ('bump_producer_epoch', 'rotate_leader', 'advance_retention')",
]) assert.ok(schema.includes(contract), `missing schema contract: ${contract}`);

const repository = await readFile(new URL('src/repository.js', root), 'utf8');
for (const contract of [
  'pg_advisory_xact_lock',
  '${producerId.length}:${producerId}:${producerEpoch}:${baseSequence}',
  'async #lockPartition(client',
  "throw fenced('leader epoch', currentLeaderEpoch)",
  "throw fenced('group generation', currentGeneration)",
  'partition.high_watermark',
  "status = 'committed'",
]) assert.ok(repository.includes(contract), `missing repository contract: ${contract}`);

const replicaStore = await readFile(new URL('src/replica-store.js', root), 'utf8');
for (const contract of ['handle.sync()', 'await link(temporary, target)', 'syncDirectory(directory)', 'sha256(readback)']) {
  assert.ok(replicaStore.includes(contract), `missing replica-store contract: ${contract}`);
}

const service = await readFile(new URL('src/service.js', root), 'utf8');
for (const contract of [
  "MQ_CRASH_AFTER_FIRST_REPLICA_RECEIPT",
  "MQ_CRASH_AFTER_PUBLISH_COMMIT",
  "MQ_CRASH_AFTER_CHECKPOINT_COMMIT",
  'queueContract.requiredReplicas',
  'frozenHighWatermark',
]) assert.ok(service.includes(contract), `missing service contract: ${contract}`);
const logBodies = [...service.matchAll(/this\.logger\(\{([\s\S]*?)\}\)/g)].map((match) => match[1]);
assert.ok(logBodies.length >= 12, 'expected explicit service log sites');
for (const body of logBodies) {
  for (const forbiddenField of [
    'producerId', 'groupId', 'memberId', 'requestKey', 'batchDigest', 'objectPath', 'records', 'body', 'headers',
  ]) {
    const fieldPattern = new RegExp(`\\b${forbiddenField}\\s*:`);
    assert.equal(fieldPattern.test(body), false, `ordinary log includes forbidden field: ${forbiddenField}`);
  }
}

const research = await readFile(new URL('docs/research-log.md', root), 'utf8');
for (const pinned of [
  '9d8388721e7231442763ad37398b8d82224aa68f',
  'ee67d0162dc340a3fc0b2508a0e2adfcb3aaa7b9',
  '9e26cf82770fcc19a8ca24e51c8b54decff861d1',
]) assert.ok(research.includes(pinned), `research log is missing fixed source identity: ${pinned}`);

const files = await walk(root);
const portable = files.filter((path) => /\.(?:md|js|mjs|json|sql|ya?ml)$/.test(path));
const forbidden = [
  /\/Users\//,
  /\/private\/tmp\//,
  /file:\/\//,
  /ghp_[A-Za-z0-9]{20,}/,
  /github_pat_[A-Za-z0-9_]{20,}/,
  /AKIA[0-9A-Z]{16}/,
  new RegExp(['Co', 'Authored-By:'].join('-')),
];
for (const path of portable) {
  const contents = await readFile(new URL(path, root), 'utf8');
  for (const pattern of forbidden) assert.equal(pattern.test(contents), false, `${path} contains forbidden portable data`);
}

for (const path of files.filter((value) => value.endsWith('.md'))) {
  const contents = await readFile(new URL(path, root), 'utf8');
  for (const match of contents.matchAll(/\[[^\]]+\]\(([^)]+)\)/g)) {
    const target = match[1].split('#', 1)[0];
    if (!target || /^(?:https?:|mailto:)/.test(target)) continue;
    const resolved = normalize(join(dirname(path), decodeURIComponent(target)));
    assert.equal((await stat(new URL(resolved, root))).isFile(), true, `${path} has broken link: ${target}`);
  }
}

for (const path of files.filter((value) => /\.(?:js|mjs)$/.test(value))) {
  execFileSync(process.execPath, ['--check', path], { cwd: root, stdio: 'inherit' });
}
execFileSync('git', ['diff', '--check'], { cwd: root, stdio: 'inherit' });
process.stdout.write(`${JSON.stringify({
  evidence: 'repository_policy_check',
  files: files.length,
  serviceLogSitesChecked: logBodies.length,
  markdownLinksChecked: true,
  syntaxChecked: true,
})}\n`);
