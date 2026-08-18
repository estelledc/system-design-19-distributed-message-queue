import { createServer } from 'node:http';
import { equalHex, sha256 } from './crypto.js';
import { limits } from './contracts.js';
import { invalid, notFound, QueueError, unauthorized } from './errors.js';

async function readJson(request) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of request) {
    bytes += chunk.length;
    if (bytes > limits.requestBodyBytes) throw invalid('request body is too large');
    chunks.push(chunk);
  }
  if (!bytes) throw invalid('JSON request body is required');
  let value;
  try {
    value = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw invalid('request body must be valid JSON');
  }
  return value;
}

function writeJson(response, status, value, extraHeaders = {}) {
  const bytes = Buffer.from(`${JSON.stringify(value)}\n`, 'utf8');
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-length': bytes.length,
    'content-type': 'application/json; charset=utf-8',
    ...extraHeaders,
  });
  response.end(bytes);
}

function header(request, name) {
  const value = request.headers[name.toLowerCase()];
  if (typeof value !== 'string' || !value.length) throw invalid(`${name} header is required`);
  return value;
}

function leaderEpoch(request) {
  const value = Number(header(request, 'X-Leader-Epoch'));
  if (!Number.isSafeInteger(value) || value < 1) throw invalid('X-Leader-Epoch must be a positive safe integer');
  return value;
}

function authorize(request, expectedToken) {
  const authorization = request.headers.authorization;
  if (typeof authorization !== 'string' || !authorization.startsWith('Bearer ')) throw unauthorized();
  const candidate = authorization.slice('Bearer '.length);
  if (!candidate.length || !equalHex(sha256(candidate), sha256(expectedToken))) throw unauthorized();
  return 'lab-operator';
}

function decodeSegment(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    throw invalid('path segment is invalid');
  }
}

export function createQueueServer({ service, repository, apiToken, logger = () => {} }) {
  if (typeof apiToken !== 'string' || apiToken.length < 16) throw new Error('MQ_API_TOKEN must contain at least 16 characters');
  return createServer(async (request, response) => {
    const started = performance.now();
    try {
      const url = new URL(request.url, 'http://queue.invalid');
      if (request.method === 'GET' && url.pathname === '/health') {
        writeJson(response, 200, { status: 'ok' });
        return;
      }
      const principal = authorize(request, apiToken);
      if (request.method === 'GET' && url.pathname === '/v1/state') {
        writeJson(response, 200, await repository.getState());
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/producers') {
        const result = await service.createProducer(await readJson(request));
        writeJson(response, result.created ? 201 : 200, result);
        return;
      }
      let match = url.pathname.match(/^\/v1\/producers\/([^/]+)\/epochs$/);
      if (request.method === 'POST' && match) {
        const result = await service.bumpProducerEpoch({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          producerId: decodeSegment(match[1]),
          body: await readJson(request),
        });
        writeJson(response, 200, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/records') {
        const result = await service.publish({ expectedLeaderEpoch: leaderEpoch(request), body: await readJson(request) });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/leadership') {
        const result = await service.rotateLeader({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, 200, result);
        return;
      }
      match = url.pathname.match(/^\/v1\/groups\/([^/]+)\/members$/);
      if (request.method === 'POST' && match) {
        const result = await service.joinGroup({
          groupId: decodeSegment(match[1]),
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, result.replayed ? 200 : 201, result);
        return;
      }
      match = url.pathname.match(/^\/v1\/groups\/([^/]+)$/);
      if (request.method === 'GET' && match) {
        writeJson(response, 200, await repository.getGroup(decodeSegment(match[1])));
        return;
      }
      match = url.pathname.match(/^\/v1\/groups\/([^/]+)\/fetch$/);
      if (request.method === 'POST' && match) {
        const result = await service.fetch({
          groupId: decodeSegment(match[1]),
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, 200, result);
        return;
      }
      match = url.pathname.match(/^\/v1\/groups\/([^/]+)\/checkpoints$/);
      if (request.method === 'POST' && match) {
        const result = await service.checkpoint({
          groupId: decodeSegment(match[1]),
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, 200, result);
        return;
      }
      if (request.method === 'POST' && url.pathname === '/v1/retention') {
        const result = await service.advanceRetention({
          principal,
          requestKey: header(request, 'Idempotency-Key'),
          body: await readJson(request),
        });
        writeJson(response, 200, result);
        return;
      }
      throw notFound();
    } catch (error) {
      const known = error instanceof QueueError;
      logger({
        event: 'request_failed',
        errorCode: known ? error.code : 'internal_error',
        status: known ? error.status : 500,
      });
      writeJson(response, known ? error.status : 500, {
        error: known ? error.code : 'internal_error',
        message: known ? error.message : 'internal server error',
        ...(known && error.details !== undefined ? { details: error.details } : {}),
      });
    } finally {
      logger({ event: 'request_finished', durationMs: Math.round((performance.now() - started) * 1000) / 1000 });
    }
  });
}
