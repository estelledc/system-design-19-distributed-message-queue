# Requirements: fenced committed-prefix queue v0.1

## Product slice

This repository implements one deliberately narrow retained-log queue:

- one synthetic topic, `events`;
- one partition, `0`;
- one active leader identity and monotonically increasing leader epoch;
- three local immutable replica directories with a two-readback commit gate;
- bounded canonical record batches from epoch/sequence-fenced producers;
- one current member per consumer group, generation-fenced fetch, and monotonic checkpoints;
- explicit logical retention with an `offset_out_of_range` result for lagging groups.

The goal is not feature breadth. The goal is to make the acknowledgement, committed-prefix, retry, takeover, and checkpoint
boundaries executable and falsifiable.

## Actors and required outcomes

### Producer

- A producer is created at epoch `0`, next sequence `0`.
- A publish identity is `(producerId, producerEpoch, baseSequence)` and binds one canonical batch digest, byte length, and record
  count.
- The current producer epoch and exact next sequence are required for a new append.
- An exact retry resumes the durable head intent or returns the immutable committed receipt.
- Changed bytes under an existing identity return `conflict`; stale epoch returns `fenced`; a sequence gap returns `out_of_order`.
- A successful publish response is possible only after at least two distinct current-leader-epoch replica objects are read back and
  the PostgreSQL commit advances the high watermark.

### Consumer-group member

- Joining creates a new group generation and makes one member current for the sole partition.
- A fetch requires the current member and generation, the exact assignment cursor, and explicit record/byte bounds.
- Fetch freezes the current committed upper bound in a durable receipt and returns records only below that bound.
- Reusing a fetch key with the same intent reconstructs the same offset interval from immutable batch objects; changed intent
  conflicts.
- A checkpoint requires the current member/generation and may move only monotonically between the committed group checkpoint and
  that generation's fetched cursor.
- Joining a newer member revokes the previous generation. The old member may have already executed code, but it cannot create a
  new checkpoint.

### Operator

- Leader rotation requires the current leader epoch and a stable mutation key; a new epoch invalidates old replica receipts as
  commit authority.
- Producer epoch rotation requires the current producer epoch and a stable mutation key.
- Retention requires the current partition revision and advances `log_start_offset` only within the committed range.
- Operator mutation retries return their stored result; changed intent under the same principal/key conflicts.
- State inspection exposes counts and offsets, not payloads or stable actor identities in ordinary logs.

## Safety invariants

1. `0 <= logStartOffset <= highWatermark <= nextOffset` at all times.
2. An idle partition has `nextOffset == highWatermark`; an active head reserves exactly one contiguous suffix.
3. Committed batches cover `[0, highWatermark)` without overlap, gap, or rewrite.
4. At most one uncommitted head intent exists for the partition.
5. Only distinct verified receipts for the current leader epoch count toward the two-replica gate.
6. A committed publish advances producer sequence and partition high watermark in the same PostgreSQL transaction.
7. Producer identity, operator mutation key, join key, fetch key, and checkpoint key are stable-intent identities, not best-effort
   deduplication hints.
8. A group checkpoint never moves backward, past its fetched cursor, or past the committed high watermark.
9. A new group generation explicitly revokes the old generation's accepted fetch receipts. A stale generation cannot finish a
   fetch receipt as current or create a checkpoint.
10. A requested/checkpointed offset below retention returns an explicit gap instead of silently jumping forward.
11. Committed fetch reads verified replica objects. Missing or corrupt copies cause fallback or a typed failure, never a read from
    the staged database copy.
12. Ordinary service logs exclude auth tokens, actor/request identities, keys, headers, values, digests, object paths, and fetched
    records.

## Bounded input contract

| Input | Limit |
|---|---:|
| HTTP request body | 256 KiB |
| Records per publish batch | 1–64 |
| Canonical batch | 256 KiB |
| Record value | 4 KiB UTF-8 |
| Record key | 256 bytes UTF-8 |
| Headers per record | 8 |
| Header value | 256 bytes UTF-8 |
| Fetch records | 1–128 |
| Fetch bytes | 8 KiB–1 MiB |
| Producer/group/member identifier | 64 bytes, restricted alphabet |
| Stable request key | 8–128 bytes, restricted alphabet |

Unknown fields, unsafe integers, duplicate header names, forbidden control characters, non-canonical stored bytes, invalid replica
IDs, and oversized work fail closed.

## Acceptance gates

| Requirement | Executable evidence |
|---|---|
| Canonical identity and strict bounds | pure contract tests |
| Prefix, epoch, generation, checkpoint, and retention invariants | direct model tests plus 200×120 deterministic randomized transitions |
| PostgreSQL locking and receipts | real PostgreSQL 17 integration tests; no infrastructure skip path |
| Current-epoch two-replica commit | integration tests with partial replication and leader rotation |
| Concurrent exact retry | six concurrent publishers against PostgreSQL |
| Corrupt replica fallback/fail-closed | real object corruption integration test |
| Process crash and response-loss recovery | child-process SIGKILL smoke across three failure windows |
| Log privacy | service and child-process sentinel scans plus repository policy check |
| Bounded performance observation | fixed 32×16 synthetic publish/fetch benchmark |
| Runtime portability | public Node 22/24/26 matrix with PostgreSQL 17.11 |

The final row is not satisfied until a public run and exact commit/run receipt exist. Local success is not public CI evidence.

## Explicit non-requirements

The lab does not implement or claim Kafka/AMQP compatibility, Raft, independent-host quorum durability, multi-partition balancing,
transactions across records/partitions, atomic downstream effects, exactly-once application outcomes, push delivery, compaction,
physical deletion, tiered storage, TLS termination, multi-tenant authorization, backup/restore, production deployment, availability
SLO, capacity, or external acceptance.

The hypothetical scale figures in the [closed-book contract](closed-book-contract.md) select questions only. They are not a load
target and are not extrapolated from the benchmark.
