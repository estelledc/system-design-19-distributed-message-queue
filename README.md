# Fenced Commit-Log Message Queue Lab

This clean-room system-design practice starts with one question: when leaders, replicas, producers, and consumer-group members can
all restart or race, what exact durable boundary lets a queue acknowledge an append and later advance consumption without
inventing “exactly once” delivery?

The prompt title is “Distributed Message Queue.” This repository does not copy a broker, source chapter, protocol, diagram,
benchmark, trace, or proprietary behavior. The initial contract was frozen from the title alone before reading the fixed secondary
chapter.

## Current phase

The bounded implementation at `968e8b4896ace6130e4ac25c34da0b7817c79c82` passed the complete public Node 22/24/26 matrix in
[GitHub Actions run 32194803417](https://github.com/estelledc/system-design-19-distributed-message-queue/actions/runs/32194803417).
The selected slice is one PostgreSQL-fenced synthetic partition whose immutable batch bytes must be read back from two of three
local replica directories before the committed high watermark advances. Stable producer epoch/sequence and consumer-group
generation/checkpoint receipts cover response loss and stale actors.

This is deliberately not a Kafka-compatible broker, Raft implementation, cross-host quorum, or disk-power-loss proof. The
benchmark is one bounded CI observation, not throughput capacity. No production deployment, SLA, downstream exactly-once outcome,
or external acceptance is claimed.

## What is executable

- strict canonical batch identity and bounded request contracts;
- one durable active head and contiguous committed prefix;
- current-leader-epoch two-of-three local object readback before commit;
- producer epoch/sequence fencing and exact response-loss replay;
- group generation fencing, stable fetch intervals, monotonic checkpoints, and visible at-least-once replay;
- explicit retention gaps and corrupt/missing committed-copy failure;
- true child-process SIGKILL recovery at three named failure windows;
- a deterministic state-machine oracle and bounded PostgreSQL/filesystem benchmark.

```text
publish -> durable head -> replica readback receipts -> PostgreSQL finalize -> committed prefix
                                                        |
join/fetch -> frozen committed range -> generation cursor -> checkpoint
```

PostgreSQL chooses the legal history. The replica directories hold committed bytes. They share one host/filesystem and therefore
do not establish distributed quorum durability.

## Read first

- [Closed-book contract](docs/closed-book-contract.md)
- [Fixed-source and primary-source comparison](docs/research-log.md)
- [Requirements and acceptance gates](docs/requirements.md)
- [Architecture and failure model](docs/architecture.md)
- [HTTP API and error vocabulary](docs/api.md)
- [Operations and recovery playbooks](docs/operations.md)
- [Threat model](docs/threat-model.md)
- [Verification evidence](docs/verification.md)
- [ADR 0001: PostgreSQL-fenced quorum batches and generation checkpoints](docs/adr/0001-postgres-fenced-quorum-batches-and-generation-checkpoints.md)
- [Security policy](SECURITY.md)

## Run locally

```sh
npm ci --ignore-scripts
docker compose up -d postgres
mkdir -p replica-data
export DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue
export MQ_REPLICA_ROOT="$PWD/replica-data"
export MQ_API_TOKEN=synthetic-local-token-00000001
npm start
```

The server binds loopback only. The database and records must be disposable synthetic fixtures. For the exact verification commands
and destructive-test warning, read [operations](docs/operations.md).

## Verify

```sh
npm run check
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue npm run test:infra
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue npm run smoke:infra
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/message_queue npm run benchmark:infra
```

Infrastructure gates never skip when PostgreSQL is absent; they fail. The current local evidence and its exclusions are recorded in
[verification](docs/verification.md).

## Evidence boundary

An accepted API response, a leader-local append, a quorum commit, a fetched record, a handler return, a committed offset, and a
downstream effect are different facts. The eventual lab must name and test each boundary it actually reaches. It must abstain from
claims about arbitrary network partitions, disk power-loss durability, cross-region availability, application side effects,
human receipt, production capacity, SLA, or external acceptance unless matching evidence exists.

## License

MIT. Third-party study material and broker data are not included.
