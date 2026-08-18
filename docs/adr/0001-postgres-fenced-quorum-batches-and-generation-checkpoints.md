# ADR 0001: PostgreSQL-fenced quorum batches and generation checkpoints

- Status: accepted for v0.1
- Date: 2026-08-19

## Context

The fixed chapter usefully connects retained partition logs, batching, pull consumers, group offsets, replication, ISR, and
delivery semantics. It does not define the identities and epoch checks that join those components after response loss or takeover.
An old leader can still appear to append, a producer retry can become a second record, and an old group member can checkpoint after
a newer member owns the partition. A picture with three broker copies does not by itself prevent any of those outcomes.

The lab needs one executable slice that survives a process crash after one replica write, an epoch change during replication, a
publish commit whose response is lost, and a rebalance after fetch but before checkpoint. It does not need a Kafka wire protocol,
general-purpose consensus, many partitions, transactions, a production segment store, external side effects, or world-scale
throughput.

## Decision

### 1. PostgreSQL is the external serialization and fencing authority

One partition row stores leader ID/epoch, next reserved offset, committed high watermark, log start, active intent, and operator
revision. Producer, append-intent, batch, replica-receipt, group, membership, fetch, checkpoint, and retention rows reference that
authority.

The service locks the partition row to reserve/finalize the only active batch, rotate epoch, or advance retention. It locks the
group row to create a generation or advance its checkpoint. Every operation carries an expected epoch/generation/revision and
returns a typed stale result instead of guessing.

This design delegates availability, crash recovery, and linearization to one PostgreSQL instance. It is not a replicated broker
control plane and cannot claim PostgreSQL failover or distributed consensus.

### 2. Canonical batch objects are immutable replica bytes

A publish request contains one bounded array of synthetic records. Canonical encoding fixes header order, UTF-8 behavior, byte
limits, and record count. SHA-256 over the exact encoded batch is its immutable identity.

Each of three local replica directories writes that object through a temporary file in the destination directory, syncs the file,
hard-links it to the final digest name without replacement, syncs the directory, and reads the final bytes back to verify digest
and size. An existing correct object is reused; an existing wrong object is corruption.

These directories exercise independent worker/process and file-operation paths but share one host and filesystem. Two successful
readbacks do not prove survival of host, disk, controller-cache, rack, or region failure.

### 3. Append intent is durable before replica work

`(producer ID, producer epoch, base sequence)` binds the canonical batch digest/count. Reserving an intent locks the partition,
requires the producer's current epoch and exact next sequence, assigns the current high watermark as base offset, and temporarily
stores the encoded bytes in PostgreSQL.

Only one intent may be uncommitted. An exact retry resumes or returns that intent. Changed bytes/count conflict; a future sequence
is out of order; an older epoch is fenced. Because staged bytes are durable, a recovery worker can finish the head intent without
the original producer. This is deliberately serial and not a throughput architecture.

### 4. Replica receipts are epoch-fenced and commit requires two

A worker checks the expected leader epoch, writes/read-verifies one replica object, then starts a new database transaction. It
records a receipt only if the partition still has that epoch, the same active intent, and the same digest/size. An epoch change in
the file-write window leaves only an uncounted immutable object.

Finalization locks the partition again, requires at least two distinct verified receipts for the current epoch, re-reads those
objects, and verifies the intent still begins exactly at the committed high watermark. In one transaction it records immutable
batch metadata, advances the high watermark/next producer sequence, clears staged bytes, and stores the response-loss receipt.

This “two of three” rule is the lab's configured durability gate. PostgreSQL, not majority election, prevents divergent commit. It
must not be described as Kafka ISR, Raft, or an independent-host quorum.

### 5. Committed-prefix fetch reads verified replica bytes

Fetch validates the current group generation/member assignment and freezes the partition's log start and committed upper bound in
a durable fetch receipt. It returns only records whose batch metadata is committed below that bound. Bytes are read from a
verified replica object and checked against the stored digest before decoding.

If one replica is missing/corrupt, another verified replica may serve; the failure is observable and repair is a separate action.
If no verified object can be read, fetch fails rather than falling back to the staged database copy or claiming committed delivery.

### 6. Group generation fences checkpoint, not already-returned bytes

The sole partition has one active member per group. A stable join key binds member identity and creates one immutable generation
receipt. A different current join increments generation and revokes the prior member. Already-returned bytes cannot be recalled;
the correctness boundary is that a revoked member cannot create a new checkpoint.

Checkpoint uses a stable key, current member/generation, and next offset. It may advance monotonically only up to that generation's
greatest fetched next offset and the partition high watermark. Exact replay returns the original receipt; changed intent conflicts.
No broker event claims that application code or a downstream write actually completed.

### 7. Retention creates an explicit data-loss boundary

An operator advances `log_start_offset` monotonically under an expected partition revision and records a retention event. Fetch
below that boundary returns `offset_out_of_range` with the current permitted range; it never silently resets to earliest/latest.
Group recovery/reset, object garbage collection, and legal retention are separate future protocols.

### 8. Evidence stops at queue-owned receipts

Allowed facts include `intent_reserved`, `replica_verified`, `batch_committed`, `broker_response_written`, `fetch_returned`,
`assignment_revoked`, and `offset_committed` only when their matching gate passes.

Ordinary logs omit record keys/bodies/headers, topic/producer/group/member/request identities, digests, object paths, auth values,
and result payloads. No event may claim producer receipt, handler success, downstream effect, exactly-once application outcome,
human receipt, production deployment, SLA, or external acceptance.

## Consequences

### Positive

- Response loss cannot append a second batch or move sequence twice; stable producer identity recovers one immutable result.
- A stale leader process can create only an ignored same-digest object, not a current replica receipt or committed-prefix advance.
- Consumers never see a leader-local/one-replica batch because batch metadata appears only after the two-receipt finalize transaction.
- A revoked member cannot overwrite progress after takeover; replay remains visible and expected instead of being mislabeled a bug.
- Retention lag is an explicit out-of-range state rather than a silent offset reset.
- Every ambitious claim has a direct negative boundary: no consensus, Kafka compatibility, independent-host durability, external
  transaction, or production capacity.

### Costs and limits

- One active batch and one partition-row lock serialize all producers. The benchmark will measure that lab path, not a scalable
  broker design.
- PostgreSQL temporarily duplicates uncommitted batch bytes and is a single availability/durability authority.
- Three replica directories on one filesystem are not failure-independent; directory/file sync semantics still depend on the OS,
  filesystem, storage device, and CI environment.
- Immutable batch objects favor recovery clarity over sequential segment efficiency, sparse indexes, compaction, or zero-copy.
- One partition and one member assignment cannot demonstrate partition balancing or group scalability.
- A fetch receipt can outlive its assignment; checkpoint fencing prevents stale progress but cannot stop already-running external
  code or undo a side effect.
- Logical retention initially leaves physical objects behind. Safe garbage collection needs reference/liveness proof and is out of
  scope.

## Rejected alternatives

### Implement Raft from the chapter's broker diagram

Rejected because the chapter does not specify election restriction, log matching, leader completeness, persisted term/vote,
membership change, snapshots, network timing, or recovery. A rushed partial implementation would manufacture a consensus claim.

### Run one Kafka broker and call its success distributed evidence

Rejected because a single external broker would test a client API but not the selected leader/replica/fencing failure windows. A
real multi-broker Kafka compatibility lab remains valuable future work but is not the smallest proof here.

### Count any two object files without a database authority

Rejected because majority storage alone does not choose a unique leader or prevent different values at the same offset. The
external PostgreSQL epoch and intent row are part of the safety contract.

### Append mutable segment files in v0.1

Rejected because concurrent append locking, torn frames, index rebuild, suffix truncation, segment roll, and compaction would add a
second large correctness problem. Immutable batch objects give exact digest/retry evidence first.

### Store committed payload bytes permanently in PostgreSQL

Rejected because it would make the replica store decorative. PostgreSQL holds staged bytes only until commit; committed fetch must
read and verify a replica object.

### Let a new append bypass an abandoned head intent

Rejected because skipping creates a hole or requires a no-op/tombstone consensus contract. Recovery must finish the one durable
head intent before the partition accepts another.

### Accept checkpoints solely by numeric range

Rejected because an old member could commit after reassignment. Current member, generation, fetched upper bound, monotonic offset,
and stable checkpoint identity are all required.

### Automatically reset a lagging group to log start

Rejected because it converts retention loss into apparent normal progress. v0.1 returns an explicit out-of-range boundary and
requires a separate future operator reset contract.
