import { sha256 } from './crypto.js';
import { corrupted, invalid } from './errors.js';

export const queueContract = Object.freeze({
  topic: 'events',
  partition: 0,
  version: 1,
  replicaIds: Object.freeze(['replica-a', 'replica-b', 'replica-c']),
  requiredReplicas: 2,
});

export const limits = Object.freeze({
  requestBodyBytes: 256 * 1024,
  requestKeyChars: 128,
  identifierChars: 64,
  maximumRecordsPerBatch: 64,
  maximumBatchBytes: 256 * 1024,
  maximumRecordValueBytes: 4 * 1024,
  maximumRecordKeyBytes: 256,
  maximumHeaders: 8,
  maximumHeaderNameChars: 48,
  maximumHeaderValueBytes: 256,
  maximumFetchRecords: 128,
  maximumFetchBytes: 1024 * 1024,
});

function plainObject(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) {
    throw invalid(`${name} must be a JSON object`);
  }
  return value;
}

export function exactKeys(value, expected, name) {
  plainObject(value, name);
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) {
    throw invalid(`${name} must contain exactly: ${wanted.join(', ')}`);
  }
  return value;
}

function text(value, name, maximumBytes, pattern = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/) {
  if (typeof value !== 'string' || !value.length || Buffer.byteLength(value, 'utf8') > maximumBytes || !pattern.test(value)) {
    throw invalid(`${name} is invalid`);
  }
  return value;
}

function safeInteger(value, name, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw invalid(`${name} must be a safe integer from ${minimum} through ${maximum}`);
  }
  return value;
}

function printableText(value, name, maximumBytes, allowEmpty = false) {
  if (typeof value !== 'string' || (!allowEmpty && !value.length) || Buffer.byteLength(value, 'utf8') > maximumBytes) {
    throw invalid(`${name} is invalid`);
  }
  if (/\u0000|[\u0001-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)) {
    throw invalid(`${name} contains a forbidden control character`);
  }
  return value.normalize('NFC');
}

export function validateIdentifier(value, name = 'identifier') {
  return text(value, name, limits.identifierChars);
}

export function validateRequestKey(value) {
  return text(value, 'Idempotency-Key', limits.requestKeyChars, /^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$/);
}

export function validateReplicaId(value) {
  if (!queueContract.replicaIds.includes(value)) throw invalid('replicaId is invalid');
  return value;
}

export function canonicalStringify(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw invalid('canonical JSON cannot contain NaN or infinity');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalStringify).join(',')}]`;
  plainObject(value, 'canonical value');
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalStringify(value[key])}`).join(',')}}`;
}

export function canonicalBytes(value) {
  return Buffer.from(`${canonicalStringify(value)}\n`, 'utf8');
}

export function digestValue(value) {
  return sha256(canonicalBytes(value));
}

function normalizeRecord(raw, index) {
  exactKeys(raw, ['key', 'headers', 'value'], `records[${index}]`);
  const key = raw.key === null ? null : printableText(raw.key, `records[${index}].key`, limits.maximumRecordKeyBytes);
  if (!Array.isArray(raw.headers) || raw.headers.length > limits.maximumHeaders) {
    throw invalid(`records[${index}].headers must contain at most ${limits.maximumHeaders} entries`);
  }
  const headers = raw.headers.map((entry, headerIndex) => {
    exactKeys(entry, ['name', 'value'], `records[${index}].headers[${headerIndex}]`);
    return {
      name: text(
        entry.name,
        `records[${index}].headers[${headerIndex}].name`,
        limits.maximumHeaderNameChars,
        /^[a-z][a-z0-9-]*$/,
      ),
      value: printableText(
        entry.value,
        `records[${index}].headers[${headerIndex}].value`,
        limits.maximumHeaderValueBytes,
        true,
      ),
    };
  }).sort((left, right) => left.name.localeCompare(right.name));
  if (new Set(headers.map((entry) => entry.name)).size !== headers.length) {
    throw invalid(`records[${index}].headers names must be unique`);
  }
  return {
    key,
    headers,
    value: printableText(raw.value, `records[${index}].value`, limits.maximumRecordValueBytes, true),
  };
}

export function normalizePublish(body) {
  exactKeys(body, ['producerId', 'producerEpoch', 'baseSequence', 'records'], 'publish body');
  if (!Array.isArray(body.records) || body.records.length < 1 || body.records.length > limits.maximumRecordsPerBatch) {
    throw invalid(`records must contain 1 through ${limits.maximumRecordsPerBatch} entries`);
  }
  const normalized = {
    producerId: validateIdentifier(body.producerId, 'producerId'),
    producerEpoch: safeInteger(body.producerEpoch, 'producerEpoch'),
    baseSequence: safeInteger(body.baseSequence, 'baseSequence'),
    records: body.records.map(normalizeRecord),
  };
  const batch = {
    version: queueContract.version,
    topic: queueContract.topic,
    partition: queueContract.partition,
    producerId: normalized.producerId,
    producerEpoch: normalized.producerEpoch,
    baseSequence: normalized.baseSequence,
    records: normalized.records,
  };
  const bytes = canonicalBytes(batch);
  if (bytes.length > limits.maximumBatchBytes) throw invalid('canonical batch is too large');
  return { ...normalized, batch, bytes, digest: sha256(bytes) };
}

export function normalizeProducer(body) {
  exactKeys(body, ['producerId'], 'producer body');
  return { producerId: validateIdentifier(body.producerId, 'producerId') };
}

export function normalizeProducerEpoch(body) {
  exactKeys(body, ['expectedEpoch'], 'producer epoch body');
  return { expectedEpoch: safeInteger(body.expectedEpoch, 'expectedEpoch') };
}

export function normalizeLeader(body) {
  exactKeys(body, ['expectedEpoch', 'newLeaderId'], 'leadership body');
  return {
    expectedEpoch: safeInteger(body.expectedEpoch, 'expectedEpoch', 1),
    newLeaderId: validateReplicaId(body.newLeaderId),
  };
}

export function normalizeJoin(body) {
  exactKeys(body, ['memberId'], 'join body');
  return { memberId: validateIdentifier(body.memberId, 'memberId') };
}

export function normalizeFetch(body) {
  exactKeys(body, ['memberId', 'generation', 'offset', 'maxRecords', 'maxBytes'], 'fetch body');
  return {
    memberId: validateIdentifier(body.memberId, 'memberId'),
    generation: safeInteger(body.generation, 'generation', 1),
    offset: safeInteger(body.offset, 'offset'),
    maxRecords: safeInteger(body.maxRecords, 'maxRecords', 1, limits.maximumFetchRecords),
    maxBytes: safeInteger(body.maxBytes, 'maxBytes', 8 * 1024, limits.maximumFetchBytes),
  };
}

export function normalizeCheckpoint(body) {
  exactKeys(body, ['memberId', 'generation', 'nextOffset'], 'checkpoint body');
  return {
    memberId: validateIdentifier(body.memberId, 'memberId'),
    generation: safeInteger(body.generation, 'generation', 1),
    nextOffset: safeInteger(body.nextOffset, 'nextOffset'),
  };
}

export function normalizeRetention(body) {
  exactKeys(body, ['expectedRevision', 'newLogStart'], 'retention body');
  return {
    expectedRevision: safeInteger(body.expectedRevision, 'expectedRevision'),
    newLogStart: safeInteger(body.newLogStart, 'newLogStart'),
  };
}

export function decodeBatch(bytes, expectedDigest) {
  if (!Buffer.isBuffer(bytes) || sha256(bytes) !== expectedDigest) throw corrupted();
  let value;
  try {
    value = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw corrupted();
  }
  const canonical = canonicalBytes(value);
  if (!canonical.equals(bytes)) throw corrupted();
  try {
    exactKeys(value, ['version', 'topic', 'partition', 'producerId', 'producerEpoch', 'baseSequence', 'records'], 'stored batch');
  } catch {
    throw corrupted();
  }
  if (value.version !== queueContract.version || value.topic !== queueContract.topic || value.partition !== queueContract.partition) {
    throw corrupted();
  }
  let normalized;
  try {
    normalized = normalizePublish({
      producerId: value.producerId,
      producerEpoch: value.producerEpoch,
      baseSequence: value.baseSequence,
      records: value.records,
    });
  } catch {
    throw corrupted();
  }
  if (!normalized.bytes.equals(bytes) || normalized.digest !== expectedDigest) throw corrupted();
  return normalized.batch;
}

export function recordWireBytes(record) {
  return canonicalBytes(record).length;
}
