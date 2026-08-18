# Research log

## Evidence boundary

The secondary chapter is fixed at repository commit `9d8388721e7231442763ad37398b8d82224aa68f`, chapter tree
`ee67d0162dc340a3fc0b2508a0e2adfcb3aaa7b9`, and `README.md` blob
`9e26cf82770fcc19a8ca24e51c8b54decff861d1`. The tree has no detected license, so this repository contains only independent
analysis and implementation. It does not copy the chapter's prose, diagrams, images, examples, benchmark figures, data, or code.

The chapter is a Kafka-like architecture snapshot, not a timeless contract for every queue and not current Kafka documentation.
Its product choices, performance statements, durability language, ZooKeeper roles, group protocol, ISR configuration, and
delivery claims remain secondary unless corrected by a primary source or verified by this lab.

Controlled public-source acquisition produced verified evidence bundles for Apache Kafka 4.3 design, protocol, producer/topic
configuration, KRaft, the current consumer rebalance protocol, PostgreSQL 17 transaction isolation/locking, and the USENIX page
for the Raft paper. A controlled fetch of the extended Raft PDF did not acquire usable artifacts; that failed bundle is retained
only as a diagnostic and is not used as paper-content evidence.

## Closed-book comparison

| Question | Closed-book decision | Fixed chapter | Result for v0.1 |
|---|---|---|---|
| queue contract | choose retained partition-log semantics explicitly; do not mix destructive work queue and replayable stream | distinguishes traditional point-to-point from streaming, then chooses retention/replay/order | implement one retained synthetic partition and one consumer-group checkpoint; no destructive per-record ack queue |
| order | only within one partition; partition mapping/version changes are separate contracts | requirements say messages stay ordered, later design limits order to a partition and suggests `hash(key) % partitions` | expose one partition in v0.1; reject global-order and repartitioning claims |
| publish retry | producer identity, epoch, sequence, immutable batch digest, exact replay, changed-intent conflict | retries are used for at-least-once; no producer ID/epoch/sequence contract | bind `(producer, epoch, base sequence)` to one canonical batch and original result |
| commit visibility | immutable contiguous prefix; leader-local/minority bytes remain hidden | “enough confirmations,” later “all ISR,” acknowledgements, and committed offset appear without one precise state transition | PostgreSQL owns one next committed offset; two verified local replica receipts are required before it advances |
| leadership | every writer/receipt/finalizer carries a current epoch; stale epoch is fenced | leader election is delegated to ZooKeeper but no term/epoch reaches data-path operations | PostgreSQL serializes explicit epoch rotation; stale workers may leave ignored bytes but cannot record/commit them |
| consumer takeover | one current generation/member assignment; revoked member cannot checkpoint | heartbeat/rebalance flow assigns partitions but late processing and offset commit are not generation-fenced | one-partition group generation fences fetch/checkpoint; takeover deliberately permits replay |
| retention | monotonic log-start revision and explicit out-of-range/data-loss result | segments/history are truncated after time, but lagging groups and reset behavior are not defined | retained metadata records the cut; reads below it fail rather than silently jumping |
| evidence | split leader write, replica readback, commit, response, fetch, handler claim, checkpoint, external effect | arrows and delivery-semantic labels can be read as end-to-end outcomes | executable vocabulary stops at broker-side receipts; downstream and human outcomes remain unproved |

## What the chapter contributes

- A replayable retained log and a destructive point-to-point queue have different ownership, deletion, and consumption state. The
  chapter explicitly chooses the retained-log family instead of hiding that trade-off.
- Topic partitions provide a practical ordering and parallelism domain. One member per partition in a classic consumer group gives
  a small checkpoint—the next offset—while independent groups can replay the same retained bytes.
- Append-oriented storage, immutable old segments, shared producer/broker/consumer framing, batching, compression, page cache, and
  pull/long-poll flows are coherent performance directions when their exact runtime and durability boundaries are measured.
- Client-side leader discovery avoids a permanent routing hop and enables per-partition batching, but it makes metadata epoch,
  stale-route errors, retry ordering, and producer identity part of correctness.
- Replicas, ISR, acknowledgement level, minimum replicas, leader election, and consumer visibility are related but not
  interchangeable. The design correctly surfaces the availability/durability trade-off even though it does not close it.
- Rebalancing, lag, retention, delayed delivery, filtering, retry topics, and archive storage are distinct mechanisms with
  independent state and failure contracts, not free options on a generic queue box.

The runnable slice narrows those directions to the missing join: an old leader, duplicate producer request, or revoked consumer
must not turn partially durable bytes into a new committed prefix or move the group checkpoint past work owned by a newer
generation.

## Defects and missing contracts in the fixed chapter

1. **The top-level ordering requirement is broader than the design.** Partitioning only preserves order inside a partition. No
   global merge, key-order domain, tie rule, partition-map version, or reconfiguration behavior is defined.
2. **`hash(key) % numPartitions` is not stable under partition-count change.** Adding a partition can remap the same key and let new
   records overtake old records in another partition; the later scalability section does not reconcile that with ordering.
3. **The delivery-semantics menu has no common boundary.** Producer durability, broker log duplicates, consumer redelivery,
   handler execution, offset persistence, and external effects are different problems. “Configurable exactly once” is not one
   switch.
4. **Producer retry identity is absent.** No producer ID, epoch, sequence, canonical batch digest, duplicate window, gap rule,
   changed-intent conflict, or response-loss receipt prevents a retry from appending a second record.
5. **Stale metadata has no epoch.** Embedding routing in the producer is useful, but a cached leader route needs leader-epoch and
   retriable/fatal error semantics. Otherwise an old leader can keep accepting after a control-plane change.
6. **Leadership is not fenced on the data path.** ZooKeeper election is named, but append, follower acknowledgement, commit, fetch,
   and recovery do not carry a term/epoch that rejects superseded actors.
7. **The commit rule changes wording.** “Enough confirmations,” “all ISR,” `ACK=all`, replication factor, and minimum ISR are used
   without one atomic definition of the current ISR set, required acknowledgement count, or consumer high watermark.
8. **`ACK=all` is easy to misread.** It means all current in-sync replicas in Kafka, not automatically every assigned replica.
   Shrinking ISR changes the participating set; `min.insync.replicas` determines when an all-ack write must fail.
9. **An ISR is not automatically a majority quorum.** Safe failover also needs a controller-authorized electable set and rules for
   unclean election. Merely counting stored copies does not prove the new leader contains every committed entry.
10. **Replica progress lacks byte identity.** Offsets alone do not establish that two replicas stored identical frame/batch bytes.
    Checksum algorithm/scope, torn write, corrupt frame, segment recovery, truncation, and repair are absent.
11. **Page-cache write is not disk-power-loss durability.** Sequential append and OS caching can improve throughput, but the
    chapter does not define flush/fsync, controller caches, storage ordering, acknowledgement timing, or recovery validation.
12. **The log is called a WAL without naming protected state.** In this design the retained log is the primary data, not
    necessarily a write-ahead record for a separately updated data structure.
13. **Database rejection is too broad.** “Databases do not support read- and write-heavy systems” has no engine, schema, access
    pattern, durability setting, fixture, or measurement. It cannot justify architecture by itself.
14. **Performance statements are not reproducible.** HDD throughput, sufficient speed, batch-size curves, memory savings, and
    connection counts lack hardware, filesystem, protocol, compression, TLS, message distribution, run method, and raw results.
15. **Consumer-group takeover has no generation fence.** A timed-out member can finish late and commit an offset after another
    member owns the partition, potentially skipping replay needed by the new owner.
16. **Join/rebalance requests have no stable identity.** Lost responses or repeated heartbeats can create ambiguous assignments;
    the chapter records only the happy-path plan, not an immutable generation receipt.
17. **“Last consumed offset” is ambiguous.** A durable checkpoint should say whether it names the last handled record or the next
    record to fetch. Crash reasoning changes by one at that boundary.
18. **Handler completion is taken on trust.** Committing after processing supports at-least-once replay, but the broker cannot infer
    that an application or external database completed correctly just because the client says so.
19. **Retention has no lag-loss protocol.** A group below the new log start needs an explicit out-of-range/data-loss outcome and an
    authorized reset policy; silently starting at earliest/latest changes semantics.
20. **Backpressure and storage exhaustion are missing.** Producer buffers, broker disk, unreplicated bytes, consumer lag, fetch
    bytes, retries, quotas, slow groups, and retention pressure all need hard limits and observable reject/throttle behavior.
21. **ZooKeeper is presented as essential and owns offsets/metadata.** That is a version-specific snapshot, not a general queue
    requirement and not the current Kafka control plane.
22. **The rebalance protocol is version-specific.** The chapter describes a client leader and global synchronization flow; current
    Kafka also has a server-side, fully incremental consumer protocol. Neither should be copied without naming version and scope.
23. **Replica redistribution is not a membership protocol.** Temporarily adding/removing copies needs configuration identity,
    overlap/election safety, catch-up boundary, cancellation, failure recovery, and operator receipts.
24. **Partition deletion and decommission are underspecified.** Producers, old consumers, retained offsets, group assignments,
    metadata caches, archive, tombstones, and physical deletion need a versioned order.
25. **Filtering and delayed delivery add new semantics.** Tag authorization, encrypted payloads, untrusted expressions, time source,
    schedule identity, retry, cancellation, ordering, retention, and clock recovery are not closed by a temporary topic.
26. **Security and privacy are absent.** Authentication, topic/group ACLs, tenant quotas, encryption, message/schema validation,
    secrets, payload minimization, log redaction, retention/erasure, abuse, and audit are not designed.
27. **Availability is asserted without a failure model.** Broker crash, process restart, disk loss, rack/DC loss, controller
    quorum, network partition, correlated replicas, recovery time, RPO/RTO, and unclean election have different results.
28. **A broker response is treated too close to delivery.** There is no evidence of producer receipt, consumer fetch, handler
    completion, downstream atomicity, human receipt, business outcome, deployment, SLA, or external acceptance.

## Primary-source corrections

### Current Kafka does not make ZooKeeper essential

Apache Kafka 4.3's official [KRaft documentation](https://kafka.apache.org/43/operations/kraft/) assigns Kafka servers controller,
broker, or combined roles and says ZooKeeper-to-KRaft migration required a bridge release whose last version was 3.9. The fixed
chapter's “ZooKeeper is essential” and ZooKeeper-owned consumer offsets/metadata are therefore historical implementation choices,
not a universal design invariant.

v0.1 does not imitate either ZooKeeper or KRaft. PostgreSQL is an explicitly external single authority for epoch, intent, commit,
and group state. This makes the lab's bottleneck and failure domain visible instead of presenting a hand-built controller as
consensus.

### Kafka ISR acknowledgement is not a generic majority proof

Kafka 4.3's official [design](https://kafka.apache.org/43/design/design/) distinguishes majority quorums from Kafka's dynamic ISR
approach and states that committed records are only exposed to consumers. The current
[topic configuration](https://kafka.apache.org/43/configuration/topic-configs/) says `acks=all` requires every current ISR member,
while `min.insync.replicas` rejects the write if that set is too small; “all” does not mean all assigned replicas.

The lab instead fixes three local replica directories and requires two independently read-back matching batch objects before its
PostgreSQL high watermark advances. PostgreSQL supplies the serialization and epoch fence. That is a tested lab rule, not Kafka's
ISR algorithm, not Raft, and not proof that two objects on one host survive a host/disk failure.

### Idempotence needs producer identity, epoch, and sequence

Kafka 4.3 [design documentation](https://kafka.apache.org/43/design/design/) attributes retry deduplication to broker-assigned
producer IDs and per-message sequence numbers. Its [producer configuration](https://kafka.apache.org/43/configuration/producer-configs/)
also binds idempotence to retries, `acks=all`, and bounded in-flight requests. The current
[wire protocol](https://kafka.apache.org/43/design/protocol/) carries producer epochs and explicitly rejects mismatches.

v0.1 uses a smaller non-compatible analogue: `(producer ID, producer epoch, base sequence)` binds the canonical batch digest and
record count. Exact retry recovers one stored result; changed bytes conflict; stale epoch and future sequence are fenced. It does
not claim Kafka protocol compatibility, an equivalent deduplication window, or cross-partition transactions.

### Group ownership is a versioned protocol, not a friendly callback

Kafka 4.3's [consumer rebalance documentation](https://kafka.apache.org/43/operations/consumer-rebalance-protocol/) says the newer
consumer protocol is server-side and fully incremental, while classic groups remain supported. The official wire protocol carries
group generation/member identity for classic group operations and defines fenced-instance errors.

The lab deliberately uses a simpler one-partition generation model. Each takeover increments an immutable generation and names
one member. A fetch receipt and checkpoint carry that generation; a revoked member's new checkpoint fails. This proves the lab's
fencing rule only, not a complete Kafka classic/new rebalance implementation.

### Exactly-once stops at the transaction boundary

Kafka 4.3's [delivery-semantics section](https://kafka.apache.org/43/design/design/) separates publication durability from consumer
processing. It explains that consuming and producing Kafka records can atomically include offsets in one Kafka transaction, while
external destinations require cooperation or storing the offset with the output.

v0.1 therefore promises at-least-once fetch around checkpoint failure. It has no broker transaction and no external output. A
checkpoint proves only that the queue stored a next offset under the current assignment; it cannot prove a handler or downstream
side effect happened exactly once.

### A majority replicated log needs election safety too

The verified USENIX record for Ongaro and Ousterhout's 2014 paper,
[*In Search of an Understandable Consensus Algorithm*](https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro),
identifies Raft as a consensus algorithm that separates leader election, log replication, and safety and uses overlapping
majorities for membership change.

The lab does not implement those election, log-matching, leader-completeness, timing, membership, or network-partition rules. Its
leader epoch is a PostgreSQL row under explicit transaction locking. “Two replica objects exist” is never labeled Raft or a
standalone consensus quorum.

### PostgreSQL supplies primitives; the application defines the queue contract

PostgreSQL 17 [transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html) documents statement versus fixed
snapshots and Serializable retries. Its [explicit-locking documentation](https://www.postgresql.org/docs/17/explicit-locking.html)
defines row locks and their transaction lifetime.

The lab locks one partition row while reserving/finalizing the next batch, rotating leader epoch, and advancing retention, and
locks one group row while changing generation/checkpoint. Immutable file writes occur outside those short transactions; a second
transaction records a replica receipt only if the expected epoch still matches. This is an intentional single-partition
correctness bottleneck, not evidence for global queue throughput or PostgreSQL failover.

## Decisions after comparison

- PostgreSQL 17 owns the single synthetic partition's leader/epoch, active append intent, next offset, committed high watermark,
  producer epoch/sequence, immutable batch metadata, per-epoch replica receipts, group generation/assignment, fetch/checkpoint
  receipts, monotonic log start, and operator revisions.
- Each append is one bounded canonical batch. PostgreSQL temporarily stages exact bytes so a broker process can recover an intent
  even if the producer disappears. Once committed, staging bytes are cleared and reads come from a digest-verified replica object.
- Three local replica directories store identical immutable batch objects by SHA-256. A write uses a same-directory temporary file,
  file sync, no-replace hard link, directory sync, and final readback. An existing same-digest object is reused; changed bytes at
  the same identity conflict.
- A replica worker receives an expected leader epoch, verifies it before the write, and records a receipt afterward only if the
  epoch is still current. A stale worker may leave an unreferenced same-digest object, but cannot count it or advance commit.
- Two current-epoch verified replica receipts are required to finalize the active intent. Finalization rechecks objects, inserts
  immutable batch metadata, clears staged bytes, advances one contiguous high watermark, and stores the producer result in one
  transaction.
- Only one uncommitted intent may exist for the partition. This makes recovery and gap freedom explicit but serializes producers;
  batching amortizes some cost. Pipelined multi-batch replication is rejected for v0.1.
- `(producer ID, epoch, base sequence)` is the publish identity. Exact replay returns its original phase/result; changed digest or
  count conflicts; stale epoch and future/gapped sequence fail. Epoch rotation is an explicit operator action and does not erase
  committed receipts.
- One consumer group has one owner for the sole partition. A stable join intent creates an immutable generation receipt; a newer
  owner revokes the old generation. Fetch freezes a committed upper bound. Checkpoint is monotonic, cannot exceed what that
  generation fetched, and is fenced after takeover.
- Checkpoint response loss is replayable by stable key, but the receipt says only `offset_committed`. Rebalance after a fetch and
  before checkpoint intentionally replays records to the new member.
- Retention advances a monotonic `log_start_offset` under an expected revision. Reads/checkpoints below it return an explicit
  out-of-range/data-loss boundary. Object garbage collection is separate and never changes retained offset identity.
- Ordinary logs expose low-cardinality event/phase/result, counts, latency, and failure class. They exclude topic/producer/group/
  member/request identities, keys/bodies/headers, digests, offsets tied to a principal, object paths, auth values, and result bodies.

## Remaining unknowns

- A real consensus/control plane, controller quorum, election safety, log matching, network partitions, dynamic membership,
  unclean election, rack awareness, independent hosts/disks, power loss, remote replication, and disaster recovery.
- Segmented append logs, sparse/time indexes, torn-frame recovery, zero-copy/sendfile, page-cache tuning, compression, checksums,
  batch protocol compatibility, TLS, storage encryption, and remote/tiered storage.
- Multiple partitions, key routing, hot keys, partition-map migration, global ordering, producer pipelining, transactions,
  compaction, schemas, tombstones, and atomic partition increase/decrease.
- Classic/new/share consumer protocols, incremental assignment, leases/heartbeats, static membership, cooperative revoke, many
  partitions, long polling, pause/resume, slow consumers, retry/dead-letter flows, and backfill isolation.
- Multi-tenant authentication/authorization, ACLs, quotas, privacy classification, retention/erasure, audit, malware/content
  validation, incident response, and regulatory requirements.
- Production capacity, cost, saturation, SLOs, failover time, backup/restore, deploy/rollback, client/downstream outcomes, and
  external acceptance.
