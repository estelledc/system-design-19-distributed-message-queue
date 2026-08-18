import { resolve } from 'node:path';
import process from 'node:process';
import pg from 'pg';
import { createQueueServer } from './http.js';
import { ReplicaSet } from './replica-store.js';
import { initializeDatabase, QueueRepository } from './repository.js';
import { QueueService } from './service.js';

const { Pool } = pg;

function log(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

async function serve() {
  const databaseUrl = process.env.DATABASE_URL;
  const apiToken = process.env.MQ_API_TOKEN;
  const replicaRoot = process.env.MQ_REPLICA_ROOT;
  if (!databaseUrl) throw new Error('DATABASE_URL is required');
  if (!replicaRoot) throw new Error('MQ_REPLICA_ROOT is required');
  if (typeof apiToken !== 'string' || apiToken.length < 16) {
    throw new Error('MQ_API_TOKEN must contain at least 16 characters');
  }
  const pool = new Pool({ connectionString: databaseUrl, max: 10, statement_timeout: 10_000 });
  let server;
  try {
    await initializeDatabase(pool);
    const repository = new QueueRepository(pool);
    const service = new QueueService(repository, new ReplicaSet(resolve(replicaRoot)), log);
    server = createQueueServer({ service, repository, apiToken, logger: log });
    const port = Number(process.env.PORT ?? 3000);
    await new Promise((resolveListen, reject) => {
      server.once('error', reject);
      server.listen(port, '127.0.0.1', resolveListen);
    });
  } catch (error) {
    if (server?.listening) await new Promise((resolveClose) => server.close(resolveClose));
    await pool.end().catch(() => {});
    throw error;
  }
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('server did not expose a TCP listening address');
  log({ event: 'server_ready', port: address.port });

  let closePromise;
  const close = async () => {
    closePromise ??= (async () => {
      await new Promise((resolveClose) => server.close(resolveClose));
      await pool.end();
    })();
    return closePromise;
  };
  process.once('SIGTERM', () => close().then(() => process.exit(0)));
  process.once('SIGINT', () => close().then(() => process.exit(0)));
}

if (process.argv[2] !== 'serve') throw new Error('usage: node src/main.js serve');
serve().catch((error) => {
  log({ event: 'startup_failed', errorCode: error?.code ?? 'startup_error' });
  process.exitCode = 1;
});
