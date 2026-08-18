# Operations

## Local prerequisites

- Node.js 22 or newer;
- PostgreSQL 17;
- npm with lockfile support;
- a local directory for synthetic replica objects.

The supplied Compose service pins PostgreSQL 17.11 by image digest. It is a convenience for local work, not a deployment manifest.

## Start the lab

```sh
npm ci --ignore-scripts
docker compose up -d postgres
mkdir -p replica-data
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue
export MQ_REPLICA_ROOT="$PWD/replica-data"
export MQ_API_TOKEN=synthetic-local-token-00000001
npm start
```

The server binds `127.0.0.1:3000` unless `PORT` is set. Schema creation is idempotent. There is intentionally no production TLS,
remote binding, migration runner, secret manager, or graceful rolling-upgrade contract.

Stop the process, then use `docker compose down` to stop the database. Do not treat the checked-in Compose password as a reusable
credential; it is a local synthetic fixture.

## Verification commands

```sh
npm run check
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue npm run test:infra
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue npm run smoke:infra
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue npm run benchmark:infra
```

`npm run check:ci` runs all four groups in order. Infrastructure tests deliberately reset all lab tables and must use a disposable
database. Their reset function requires the exact internal confirmation string, but that guard is not authorization to point tests
at valuable data.

## Health interpretation

`GET /health` proves only that the HTTP event loop answered. Use authenticated `GET /v1/state` to inspect:

- leader epoch and operator revision;
- `highWatermark` versus `nextOffset`;
- active intent and replicating-intent count;
- committed batch and group counts.

An active intent with `nextOffset > highWatermark` is a reserved head, not corruption by itself. A growing age for that state means
the same publish identity needs recovery. v0.1 exposes no automatic worker or age metric, so inspection requires PostgreSQL.

## Recovery playbooks

### Publish failed with `insufficient_replicas`

1. Keep producer ID, epoch, base sequence, records, and expected leader epoch unchanged.
2. Inspect host capacity/permissions and replica object readability.
3. Retry the exact publish. It resumes the durable head and reuses correct immutable objects.
4. Do not submit another producer or future sequence to bypass the head; the service returns `partition_busy` or `out_of_order`.

### Leadership changed with an active head

1. Read current state and use the returned leader epoch.
2. Retry the exact original publish with the new epoch.
3. Expect current workers to create current-epoch receipts; old-epoch receipts remain historical but do not count.
4. Do not delete the intent or edit offsets manually. v0.1 has no safe abandonment/tombstone protocol.

### One committed replica is corrupt or missing

Fetch tries other receipt-backed copies and records only a failure class/count. Restore/repair is not implemented. If all usable
copies are gone, preserve the failure and stop: fabricating bytes from metadata or staged data would violate the committed-object
contract.

### Consumer is fenced after rebalance

Stop checkpoint attempts from the old member. The current member starts at the durable group checkpoint, so records fetched but not
checkpointed by the old generation may appear again. Downstream operations must supply their own deduplication or transaction.

### Group falls behind retention

`offset_out_of_range` is a data-loss boundary for that group. v0.1 intentionally has no automatic earliest/latest reset. A future
operator-reset API must be separately authorized and audited; direct table edits are outside the supported protocol.

## Backup, deletion, and durability boundaries

- PostgreSQL backup/restore is not implemented or tested.
- Logical retention does not delete object files or batch metadata.
- Manual object deletion can make committed data unreadable and is unsupported.
- File and directory sync plus readback were exercised, but host power-loss/controller behavior was not.
- The three replica directories share a host/filesystem. Copy count must not be presented as rack/region durability.

## Observability and privacy

Ordinary JSON logs include event class, status, duration, bounded record/replica counts, replay flag, and generic failure class. They
exclude tokens, producer/group/member/request identities, record material, digests, and paths. Debugging that requires payload or
identity inspection must use an explicit secure workflow; v0.1 does not provide one.

The process does not claim that `batch_committed` means the producer received the response, that `fetch_returned` means a handler
completed, or that `offset_committed` means a downstream effect occurred.

## Upgrade and rollback

There is no schema-version table or downgrade path. Treat v0.1 schema evolution as destructive lab work against a disposable
database. Before any compatible upgrade, add explicit migrations, mixed-version behavior, restore evidence, and rollback gates.
