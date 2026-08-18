# Verification evidence

## Current status

The implementation has passed the complete local gate on 2026-08-19 with Node 26.7.0 and PostgreSQL 17.11. Public CI is not yet a
fact. This document will name the exact implementation commit and GitHub Actions run only after all Node 22/24/26 jobs complete.

Local observations:

- 17 pure tests passed; zero failed, skipped, cancelled, or todo;
- 12 real PostgreSQL tests passed; zero failed, skipped, cancelled, or todo;
- the true-process smoke passed three separate SIGKILL windows and ended with high watermark 3, two committed batches, two
  completed fetch receipts, and one checkpoint receipt;
- the bounded benchmark committed and fetched 512 records in 32 batches, with 96 replica receipts and four fetches;
- npm install reported zero vulnerabilities before the full audit gate;
- no deployment, producer/client receipt, application handler, downstream side effect, independent-host durability, SLA, or
  external acceptance was tested.

## Retained local failure

The first true-process attempt failed before exercising a crash window because `PORT=0` correctly asked the OS for a free port,
but the readiness event reported the requested value `0` instead of the socket's assigned port. The client attempted port zero and
received `EADDRNOTAVAIL`. `src/main.js` now reads `server.address().port`; the exact smoke then passed. This is evidence that the
process boundary caught behavior the in-memory HTTP tests did not cover.

The failed shell invocation continued to the benchmark because that ad-hoc wrapper lacked fail-fast mode; it is not counted as a
complete gate. Subsequent verification wrappers use fail-fast behavior, and CI uses one `npm run check:ci` chain whose nonzero
subcommand stops the job.

## Gate map

| Gate | What it can prove | What it cannot prove |
|---|---|---|
| `npm run lint` | required files, syntax, links, fixed-source identities, workflow pins, schema/code contracts, portable-data policy | runtime semantics or database/filesystem behavior |
| `npm test` | strict canonical contracts, replica immutability/corruption, HTTP auth/errors, direct model invariants | PostgreSQL transactions, process restart, power-loss durability |
| `npm audit --audit-level=high` | registry audit reported no known high-severity issue for the exact lock at that time | absence of vulnerabilities or supply-chain compromise |
| `npm run test:infra` | real PostgreSQL 17 locks, receipts, races, fencing, retention, and real local object behavior | PostgreSQL failover, independent hosts, arbitrary crash schedules |
| `npm run smoke:infra` | API-process SIGKILL recovery and response-loss replay against PostgreSQL/local objects | host/disk/controller failure, client receipt, downstream effect |
| `npm run benchmark:infra` | raw bounded observation for one exact generated fixture/runtime | production capacity, latency SLO, broker comparison, horizontal scale |
| public Node 22/24/26 CI | reproducibility on three runtimes and pinned PostgreSQL container for one commit | deployment, availability, production behavior, external acceptance |

## Pure test design

- canonical header ordering and byte-stable publish identity;
- changed record bytes change identity;
- stored-batch rejection for non-canonical or digest-mismatched bytes;
- unknown fields, duplicate headers, control characters, unsafe integers, fetch/retention bounds;
- immutable replica write/replay and concurrent same-byte writer convergence;
- corruption fallback and invalid replica/digest rejection before filesystem access;
- public health versus authenticated state, leader header requirement, stable group key, generic unexpected failure;
- direct committed-prefix/epoch/generation takeover examples;
- 200 generated fixtures × 120 transitions with invariant checks after every attempted operation.

The model oracle is independent of PostgreSQL queries but shares the written requirements. It is a broad state-transition check,
not a proof over all possible executions.

## Real PostgreSQL test design

1. Confirm PostgreSQL major version 17 and the exact empty partition state.
2. Leave one replica receipt invisible, then exact-retry onto a second replica and conflict changed intent.
3. Rotate leader after an old-epoch receipt and require two new-epoch receipts.
4. Block a different producer behind the head and converge six concurrent exact publishers to one batch.
5. Fence stale producer epoch while preserving replay of its old committed identity.
6. Return partition-ordered committed records and exact fetch replay.
7. Rebalance after fetch, fence old checkpoint, and visibly replay to the new owner.
8. Explicitly revoke an accepted old-generation fetch receipt without advancing its cursor.
9. Replay a checkpoint after response uncertainty and conflict changed intent.
10. Expose retention lag as `offset_out_of_range`; initialize a new group at log start.
11. Fall back from one corrupt committed copy; fail closed after every receipt-backed copy is corrupt.
12. Scan ordinary service logs for actor, key, header, value, and digest sentinels.

## True-process smoke design

1. Create a producer through the HTTP process.
2. Start with the first-replica crash hook; SIGKILL after one receipt.
3. Confirm one `replicating` intent, one receipt, and high watermark 0.
4. Restart and exact-retry; complete the same head as offsets `[0,2)`.
5. Start with the publish-commit crash hook; commit `[2,3)` and SIGKILL before response.
6. Confirm high watermark 3/two batches, restart, and require an exact committed replay.
7. Member A fetches offsets 0–2; member B joins; member A's checkpoint is fenced; member B fetches 0–2 again.
8. Start with the checkpoint-commit crash hook; commit offset 3 and SIGKILL before response.
9. Restart and require exact checkpoint replay with no second receipt.
10. Scan all child logs for token, actor IDs, record material, and 64-hex digests.

SIGKILL shows process failure at those explicit hooks. It does not simulate kernel, database, filesystem, host, or power failure.

## Bounded benchmark fixture

The fixture is fixed at one partition, three local replica directories, 32 sequential batches, 16 records per batch, 1,024-byte
ASCII values, 512 total records, and one consumer generation. Every publish writes/reads all three objects; consumption uses four
bounded fetches followed by one checkpoint.

The complete local Node 26.7.0/PostgreSQL 17.11 chain observed:

| Metric | Local observation |
|---|---:|
| publish records/s | 422.789 |
| publish batch p50 / p95 / max | 33.684 / 62.229 / 81.586 ms |
| fetch records/s | 25,871.326 |
| fetch p50 / p95 / max | 4.466 / 6.620 / 6.620 ms |

This is one raw run, not a stable estimate or runtime ranking. It excludes network latency, independent hosts/disks, broker
failover/consensus, compression, segment indexes, multi-partition balance, production extrapolation, and SLA.

## Public receipt placeholder

No public receipt exists yet. When CI is green, this section must record:

- exact implementation commit;
- exact GitHub Actions run URL and per-runtime job result;
- pure/infrastructure counts and smoke summary;
- per-runtime raw benchmark observations;
- any retained red run and the precise defect it exposed;
- the final documentation-only run separately from the implementation evidence.
