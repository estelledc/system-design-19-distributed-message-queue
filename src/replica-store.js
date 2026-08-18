import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { access, link, mkdir, open, readFile, unlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { equalHex, sha256 } from './crypto.js';
import { queueContract, validateReplicaId } from './contracts.js';
import { corrupted } from './errors.js';

async function exists(path) {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch (error) {
    if (error?.code === 'ENOENT') return false;
    throw error;
  }
}

async function syncDirectory(path) {
  const handle = await open(path, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export class ReplicaSet {
  constructor(root) {
    if (typeof root !== 'string' || !root.length) throw new Error('replica root is required');
    this.root = root;
  }

  objectKey(digest) {
    if (!/^[0-9a-f]{64}$/.test(digest)) throw new Error('invalid object digest');
    return `sha256/${digest.slice(0, 2)}/${digest}.batch`;
  }

  objectPath(replicaId, digest) {
    return join(this.root, validateReplicaId(replicaId), this.objectKey(digest));
  }

  async put(replicaId, bytes) {
    validateReplicaId(replicaId);
    const content = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    const digest = sha256(content);
    const target = this.objectPath(replicaId, digest);
    const directory = dirname(target);
    await mkdir(directory, { recursive: true, mode: 0o700 });

    if (!(await exists(target))) {
      const temporary = join(directory, `.${digest}.${process.pid}.${randomUUID()}.tmp`);
      const handle = await open(temporary, 'wx', 0o600);
      try {
        await handle.writeFile(content);
        await handle.sync();
      } finally {
        await handle.close();
      }
      try {
        await link(temporary, target);
      } catch (error) {
        if (error?.code !== 'EEXIST') throw error;
      } finally {
        await unlink(temporary).catch((error) => {
          if (error?.code !== 'ENOENT') throw error;
        });
      }
      await syncDirectory(directory);
    }

    const readback = await readFile(target);
    if (readback.length !== content.length || !equalHex(sha256(readback), digest)) throw corrupted();
    return { replicaId, digest, bytes: content.length, objectKey: this.objectKey(digest) };
  }

  async read(replicaId, digest) {
    validateReplicaId(replicaId);
    const content = await readFile(this.objectPath(replicaId, digest)).catch((error) => {
      if (error?.code === 'ENOENT') throw corrupted();
      throw error;
    });
    if (!equalHex(sha256(content), digest)) throw corrupted();
    return content;
  }

  async readAny(digest, candidates = queueContract.replicaIds) {
    const failures = [];
    for (const replicaId of candidates) {
      try {
        return { replicaId, bytes: await this.read(replicaId, digest), failures };
      } catch (error) {
        failures.push({ replicaId, code: error?.code ?? 'read_failed' });
      }
    }
    throw corrupted();
  }
}
