# Closed-book contract: fenced committed-prefix message queue

## Reading boundary

This contract was written from the case title alone, before reading the fixed `system-design-notes` chapter. “Distributed message
queue” names a problem family, not one settled product contract. Queue versus retained log semantics, push versus pull, ordering,
delivery guarantees, replication, transactions, consumer groups, retention, tiered storage, scale, and latency are hypotheses for
this lab—not facts about Kafka, Pulsar, RabbitMQ, SQS, or any other system. Later research must record confirmations, conflicts,
omissions, and changes instead of silently rewriting this baseline.

## Users and core behavior

### Producer

1. An authenticated synthetic producer selects a topic and canonical partition key, then publishes a bounded immutable record under
   a stable request identity.
2. A successful response names the partition, offset, leader epoch, record digest, and durability boundary actually reached.
3. Retrying the same producer identity, epoch, sequence, and record returns the original receipt. Reusing that identity for changed
   bytes conflicts; a stale producer epoch is fenced.
4. “Accepted by the leader” and “committed on a quorum” are separate outcomes. Neither proves a consumer fetched or processed the
   record or that a downstream side effect happened.

### Consumer-group member

1. A synthetic member joins a named group, receives a generation and bounded partition assignment, and fetches only committed
   records from its assigned partitions.
2. Records within one partition are returned by monotonically increasing offset. There is no default total order across partitions.
3. A member may commit an offset only under its current group generation and assignment. Revoked or superseded members are fenced.
4. An offset commit records a queue checkpoint, not proof that arbitrary application logic or an external side effect succeeded.
   At-least-once delivery permits replay after a crash between side effect and checkpoint.

### Broker operator

1. An operator can create a bounded topic contract, elect/fence a leader, observe replica progress, advance only a quorum-backed
   committed prefix, apply retention, and inspect non-payload health receipts.
2. A recovering or stale replica cannot expose divergent uncommitted suffixes as committed or regain leadership without a newer
   epoch/term and reconciliation.
3. Ordinary logs and metrics exclude record bodies, keys, headers, auth material, request IDs/digests, producer/group/member IDs,
   and fetched results.

## Non-goals for v0.1

- real tenants, identities, business events, credentials, customer payloads, production traces, or proprietary broker data;
- protocol compatibility with Kafka, AMQP, MQTT, Pulsar, SQS, NATS, or any vendor;
- broker transactions, atomic writes across partitions, consume-transform-produce exactly-once processing, or atomic external
  database side effects;
- schema registry, stream processing, joins, windows, connectors, change-data capture, dead-letter automation, or workflow engines;
- global ordering, priority queues, per-message delay, arbitrary filtering, request/reply, broadcast presence, or push delivery;
- dynamic partition splitting, multi-cluster mirroring, geo-replication, tiered/object storage, disaster recovery, backup/restore,
  production deployment, SLA, or capacity;
- proving disk-controller power-loss behavior, arbitrary Byzantine faults, all network schedules, application handler success,
  downstream idempotency, human receipt, business outcome, or external acceptance.

## Hypothetical scale envelope

These numbers select failure modes; they are not sourced requirements:

- 50 million synthetic records/day is about 579/s average before peak and replication; a 30× peak gives roughly 17,400/s;
- 2 KiB average encoded record size is about 100 GB/day before indexes, replication, protocol overhead, compression, and history;
- replication factor 3 makes raw retained bytes about 300 GB/day before those additions;
- seven-day hot retention would be about 2.1 TB at that fixture, not a measured capacity;
- one hot partition at 20% of traffic is a deliberate skew case; aggregate throughput cannot hide that bottleneck;
- record/key/header bytes, batch bytes/count, partitions/topic, in-flight requests, retry count, lag, fetch bytes, poll interval,
  group members, retention age/bytes, and recovery work all require hard limits.

The runnable lab will use small synthetic records and a bounded partition/replica fixture, report exact raw counts, and never
extrapolate them to a production broker.

## Candidate state model

Partition leadership and replication:

```text
(partition, leader epoch) -> leader + replica set
append -> leader-local suffix -> replicated acknowledgements -> committed high watermark
                        \-> truncated/reconciled if never committed

committed log is an immutable contiguous prefix [0, high watermark)
```

Producer session:

```text
producer identity -> epoch -> next expected sequence per partition
same sequence + same digest -> original receipt
same sequence + changed digest -> conflict
older epoch -> fenced
gap/future sequence -> explicit out-of-order result
```

Consumer group:

```text
empty -> preparing generation -> stable assignment -> rebalancing -> stable next generation
member fetch/offset commit requires (group, generation, member, assigned partition)
```

The exact consensus protocol, storage engine, wire protocol, batching, checksum, compression, retry policy, retention model, and
vertical slice remain undecided until source and primary specifications are reviewed.

## Core invariants

1. **Canonical record and bounds.** Topic, partition, key, headers, timestamp source, payload bytes, encoding, producer epoch,
   sequence, batch count/bytes, fetch limit, offset, and timeouts use one canonical domain. Unknown fields, unsafe integers,
   malformed encodings, oversized inputs, and ambiguous partition choices fail before allocation or persistence.
2. **Stable immutable publish intent.** `(producer identity, producer epoch, partition, sequence)` binds the canonical record digest
   and result. Exact replay returns one receipt; changed intent conflicts; future gaps fail; an older epoch cannot append.
3. **Epoch-fenced leadership.** Every append and replica acknowledgement binds a current partition leader epoch. Once a newer epoch
   is durable, an older leader cannot advance the committed prefix or acknowledge new success.
4. **Committed prefix only.** Consumer-visible records form a gap-free immutable prefix. A high watermark advances only when every
   earlier offset in the prefix satisfies the selected quorum durability rule; leader-local or minority suffixes remain invisible.
5. **No divergent committed histories.** Two successful leadership epochs cannot commit different records at the same partition
   offset. Recovery reconciles or truncates only uncommitted suffixes and never rewrites a committed record identity.
6. **Acknowledgement says exactly what survived.** A publish response distinguishes validation, leader-local append, quorum commit,
   and response write. Response loss can be replayed by stable intent; no response implies consumer delivery or external effect.
7. **Partition order, not global order.** One committed partition is read by ascending offset. Cross-partition merge order is
   unspecified unless an explicit higher-level contract is added; wall-clock timestamps cannot manufacture a total order.
8. **Generation-fenced ownership.** Fetch and offset commit require the current group generation, live member identity, and current
   partition assignment. A revoked member cannot commit over a newer owner even if its handler finishes late.
9. **Monotonic bounded checkpoint.** A committed next offset cannot move backward by accident, exceed the committed high watermark,
   or silently skip beyond the member's allowed processed boundary. Deliberate rewind/reset is a separate audited operator event.
10. **At-least-once is explicit.** Crashing after fetch or downstream work but before checkpoint may replay a record. Crashing after
    checkpoint but before a non-atomic downstream effect may lose that effect. Exactly-once application outcome is excluded unless
    the same transaction covers both queue progress and the effect.
11. **Retention cannot masquerade as lag.** Retention/tombstone decisions are monotonic and versioned. If a requested or committed
    offset is below the log start, fetch returns an explicit out-of-range/data-loss boundary; it never jumps forward as normal
    progress.
12. **Backpressure is observable.** Storage full, replication lag, too many in-flight bytes, slow consumers, oversized fetches, and
    bounded work return explicit reject/throttle/stale outcomes. The system does not accept unbounded memory or silently drop a
    committed record.
13. **Checksums are identity, not truth.** Stored/replicated bytes are length- and digest-checked before use. Matching digests prove
    technical byte identity, not semantic validity, authorization, legal origin, handler completion, or business correctness.
14. **Payload privacy in evidence.** Record keys/bodies/headers, auth data, producer/group/member identities, request keys/digests,
    offsets tied to a tenant, and fetched results do not enter ordinary logs or metric labels.
15. **Evidence separation.** `request_validated`, `leader_appended`, `quorum_committed`, `broker_responded`, `consumer_fetched`,
    `handler_returned`, `offset_committed`, and `downstream_effect_observed` are distinct. None implies human receipt, production
    deployment, SLA, or external acceptance.

## Initial API/event sketch

Authenticated producer routes:

- `POST /v1/topics/{topic}/records` with stable producer ID/epoch/sequence, partition key or explicit partition, canonical headers,
  bounded payload, required acknowledgement level, and request deadline;
- `GET /v1/publish-receipts/{id}` for immutable non-payload append/commit status after response loss.

Authenticated consumer-group routes:

- `POST /v1/groups/{group}/members` to join and receive member ID, generation, assignment, and lease/session limits;
- `POST /v1/groups/{group}/fetch` with member, generation, assigned partition, next offset, byte/count cap, and wait bound;
- `POST /v1/groups/{group}/offsets` with member, generation, partition, next offset, and stable checkpoint key;
- `DELETE /v1/groups/{group}/members/{member}` to leave and trigger a new generation.

Authenticated operator routes:

- `POST /v1/topics` with immutable topic/partition/replication/retention limits;
- `POST /v1/partitions/{partition}/leadership` with strong expected epoch and replica evidence;
- `GET /v1/partitions/{partition}` for leader epoch, log start/end, committed high watermark, replica lag, and health counts without
  payloads;
- `POST /v1/groups/{group}/offset-resets` as an explicit audited operation separate from ordinary commits.

Exact fields, auth, broker/storage/consensus choice, producer-session lifecycle, rebalancing protocol, retention, error vocabulary,
and acknowledgement levels remain hypotheses until primary sources are reviewed.

## Failure matrix

| Failure window | Required result |
|---|---|
| producer loses response after quorum commit | identical identity/digest returns the original partition/offset/epoch receipt; changed bytes conflict |
| producer retries after only leader-local append | selected acknowledgement contract decides whether it may be reported; it cannot be mislabeled quorum-durable |
| old leader continues after a newer epoch | stale epoch appends/acks cannot advance committed state or return success |
| leader dies with minority-only suffix | recovery truncates/reconciles that suffix before exposing committed reads |
| follower acknowledges bytes with failed digest | acknowledgement is rejected; high watermark cannot cross the corrupt/missing offset |
| concurrent producers target one partition | committed offsets form one contiguous deterministic order; each stable intent has one immutable result |
| member handles a record while rebalance revokes it | old generation checkpoint is fenced; replay to the new owner is allowed and observable |
| member crashes after fetch before checkpoint | record is fetched again from the committed offset; delivery count is not called exactly once |
| downstream side effect succeeds before checkpoint, then member crashes | replay is expected; downstream deduplication/transaction is an external requirement |
| checkpoint request response is lost | exact stable retry returns the original result; changed or backward intent conflicts |
| retention passes a group's checkpoint | explicit offset-out-of-range/data-loss receipt; no silent jump to current log start |
| disk/in-flight/replica lag limit is reached | bounded throttle/reject/degraded result; no silent committed-record drop |
| consumer asks beyond committed high watermark | empty/bounded wait response, never exposure of uncommitted suffix |
| process dies after response bytes start | broker response-write evidence stays distinct from producer/consumer receipt and downstream outcome |

## Required executable evidence before v0.1 completion

1. A clean-room README, source comparison, requirements, architecture, API/events, operations, threat model, and at least one ADR.
2. Exact record/topic/partition/session/generation/offset/batch validation plus deterministic identity and error-vocabulary tests.
3. Generated/property tests comparing committed-prefix, producer-sequence, leader-epoch, group-generation, checkpoint, retention, and
   replay behavior with a small direct state-machine oracle across randomized interleavings.
4. Real persistence or real multi-process tests for quorum/commit authority, concurrent append, stale leader/member fencing,
   response-loss replay, replica recovery/truncation, retention gaps, and durable restart.
5. A true-process crash/restart smoke covering publish commit response loss, minority suffix recovery, rebalance during handling,
   checkpoint response loss, and visible replay without an exactly-once claim.
6. A bounded benchmark with exact partition/record/batch/replica/consumer fixture, runtime/service versions, exclusions, and raw
   observations—without production extrapolation or cross-broker comparison.
7. Node 22/24/26 public CI with pinned actions/services, dependency audit, zero skipped infrastructure tests, and exact commit/run
   receipts.
8. Log/evidence-vocabulary scans that reject synthetic private values and any claim of application side-effect atomicity, human
   receipt, production deployment, SLA, or external acceptance without matching evidence.

## Initial design choices to challenge after source review

- Does the chapter define a retained partition log, destructive work queue, pub/sub fanout, or blur those contracts together?
- What exact event makes a producer acknowledgement durable, and how are leader epochs or terms fenced after failover?
- Is “replication factor” treated as a number of copies or tied to a committed-prefix/quorum rule with recovery semantics?
- Are idempotent producer identity, epoch, sequence, changed-intent conflict, and out-of-order retry behavior defined?
- Does it distinguish append order, commit order, delivery order, handler completion, offset commit, and downstream effects?
- How are consumer-group assignment generation, revoked-member fencing, slow members, lag, and retention gaps handled?
- Which throughput, latency, record size, retention, partition-count, and availability figures are cited, reproducible, or assumed?
- Which claims would require disk, controller, network-partition, application, or external evidence that this lab must abstain from?

Implementation remains pending until this baseline is committed, the fixed source is inspected, primary specifications are
verified, and the smallest executable invariant is selected.
