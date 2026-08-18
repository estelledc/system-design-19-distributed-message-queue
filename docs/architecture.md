# Architecture

## Decision in one sentence

PostgreSQL chooses one legal history; immutable local batch objects hold committed payload bytes; producer epochs, leader epochs,
and consumer-group generations prevent stale actors from advancing that history.

```text
producer / consumer / operator
              |
       loopback HTTP + bearer
              |
        strict contracts
              |
         QueueService
         /          \
 PostgreSQL 17     ReplicaSet
 control/receipts   a/ b/ c/
         \          /
          committed prefix
```

The three directories exercise separate immutable object writes and readback checks, but they are not independent machines or
failure domains. PostgreSQL is the sole serialization, fencing, and availability authority.

## Components

### HTTP adapter

`src/http.js` exposes a loopback JSON API. `/health` is public; all `/v1` routes require an exact bearer token. It applies body
limits, required headers, typed error responses, and `Cache-Control: no-store`. Logs contain event class, bounded counts, failure
class, status, and duration only.

### Contract and canonical encoding

`src/contracts.js` rejects unknown fields and normalizes Unicode, header order, number ranges, and batch shape. Canonical JSON plus
a trailing newline becomes the stored byte string. SHA-256 is an immutable byte identity; it is not proof that the message is true,
authorized, legal, processed, or useful.

### PostgreSQL repository

`src/repository.js` owns the state transitions. Row locks protect the single partition, producer, and group rows. Transaction-scoped
advisory locks serialize stable identities so concurrent exact retries converge before checking/inserting their receipt.

The durable tables separate:

- partition leadership, offsets, active head, and operator revision;
- producer epoch/next sequence;
- publish intent, epoch-bound replica receipts, and committed batch metadata;
- operator mutation receipts;
- current group state, immutable assignment generations, join/fetch/checkpoint receipts.

### Replica store

`src/replica-store.js` maps `(replicaId, digest)` to an immutable object path. A new object is written with exclusive create, file
sync, hard-link-without-replacement, temporary cleanup, directory sync, and final readback digest. Fetch tries only replica IDs that
have matching commit-epoch receipts.

### Direct model

`src/model.js` is a small independent state machine used as an invariant oracle. It does not prove PostgreSQL or filesystem
behavior; it makes the intended prefix/epoch/generation rules cheap to exercise over many generated interleavings.

## Publish protocol

```text
validated request
      |
      v
lock stable producer identity + partition
      |
      +-- exact existing identity --> resume / replay / changed-intent conflict
      |
reserve [baseOffset, nextOffset), persist staged canonical bytes
      |
write + sync + read back immutable object on replica a/b/c
      |
record receipt only if leader epoch and active head are still current
      |
re-read at least two receipt-backed objects
      |
one PostgreSQL transaction:
  batch metadata + high watermark + producer sequence + response receipt
      |
committed response (or stable replay after response loss)
```

The partition permits only one active head. This intentionally avoids holes and suffix-truncation complexity, but serializes
publish throughput. If the process dies with one receipt, the durable staged bytes and head identity let an exact retry complete the
same batch. It cannot skip the head with another producer.

Leader rotation increments the epoch without discarding the intent's immutable bytes. Old-epoch receipts remain historical facts
but do not count. A current worker must obtain two current-epoch readback receipts before finalization.

## Committed-prefix boundary

`next_offset` includes the one reserved head; `high_watermark` includes only finalized batches. Consumers query only immutable
`mq_batches` rows below a fetch receipt's frozen high watermark. They never scan `mq_publish_intents.staged_bytes`.

This distinction makes the following states observable:

| State | Durable facts | Visible to fetch |
|---|---|---|
| reserved | intent, staged bytes, offset range | no |
| one replica verified | intent plus one epoch-bound receipt | no |
| two/three verified, before finalize | receipts exist, high watermark unchanged | no |
| finalized | batch metadata and high watermark committed | yes |
| response lost | same finalized state, client receipt uncertain | exact retry returns stored result |

## Consumer-group protocol

```text
join member A -> generation 1, cursor = checkpoint
fetch key F1  -> freeze [requested offset, current high watermark)
               -> read verified objects -> atomically advance generation-1 cursor

join member B -> revoke generation 1, create generation 2 at checkpoint
member A checkpoint -> fenced
member B fetch       -> replay from durable checkpoint is expected
member B checkpoint  -> monotonic durable queue progress
```

A fetch receipt freezes the upper bound and result interval, but it does not store payload bytes. An exact replay reconstructs that
interval from immutable objects and verifies the same result count/next offset. A rebalance can revoke an in-flight receipt before
completion; it cannot recall records already returned or undo application work.

Creating a new generation also marks any old accepted fetch receipt `revoked`; a concurrent completion performs the same check
under the group lock. Checkpoint means only “the queue stored the group's next offset.” It does not prove handler completion,
downstream database commit, notification delivery, or exactly-once business effect.

## Retention

Logical retention advances a versioned `log_start_offset`. Existing batch objects are not deleted. A group whose checkpoint or
requested offset is below the new boundary receives `offset_out_of_range` with current bounds. Reset policy and physical garbage
collection are deliberately separate future protocols.

## Consistency and failure model

### What the implementation establishes for its fixture

- one linearized partition history under one PostgreSQL instance;
- contiguous committed prefix and deterministic offset allocation;
- producer/leader/member fencing at every state-changing boundary;
- stable response-loss receipts for publish and checkpoint;
- at-least-once replay after rebalance or crash before checkpoint;
- corrupt/missing committed copy fallback, then fail-closed when no verified copy remains.

### What remains outside the model

- PostgreSQL failover, split brain, WAL/storage durability, or backup recovery;
- independent brokers, racks, disks, networks, quorum election, or consensus liveness;
- process crash during every filesystem syscall and host power loss;
- Byzantine actors, malicious database administrators, or SHA-256 compromise;
- atomicity with arbitrary application side effects;
- multi-partition ordering, balancing, hot-key mitigation, or global scale.

See [ADR 0001](adr/0001-postgres-fenced-quorum-batches-and-generation-checkpoints.md) for rejected alternatives and
[verification](verification.md) for the exact evidence boundary.
