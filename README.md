# Fenced Commit-Log Message Queue Lab

This clean-room system-design practice starts with one question: when leaders, replicas, producers, and consumer-group members can
all restart or race, what exact durable boundary lets a queue acknowledge an append and later advance consumption without
inventing “exactly once” delivery?

The prompt title is “Distributed Message Queue.” This repository does not copy a broker, source chapter, protocol, diagram,
benchmark, trace, or proprietary behavior. The initial contract was frozen from the title alone before reading the fixed secondary
chapter.

## Current phase

Source comparison and the first design decision are complete; implementation is still pending. The selected slice is one
PostgreSQL-fenced synthetic partition whose immutable batch bytes must be read back from two of three local replica directories
before the committed high watermark advances. Stable producer epoch/sequence and consumer-group generation/checkpoint receipts
cover response loss and stale actors.

This is deliberately not a Kafka-compatible broker, Raft implementation, cross-host quorum, or disk-power-loss proof. No
throughput result, durability result, public CI, production deployment, or external acceptance is claimed yet.

## Read first

- [Closed-book contract](docs/closed-book-contract.md)
- [Fixed-source and primary-source comparison](docs/research-log.md)
- [ADR 0001: PostgreSQL-fenced quorum batches and generation checkpoints](docs/adr/0001-postgres-fenced-quorum-batches-and-generation-checkpoints.md)
- [Security policy](SECURITY.md)

## Evidence boundary

An accepted API response, a leader-local append, a quorum commit, a fetched record, a handler return, a committed offset, and a
downstream effect are different facts. The eventual lab must name and test each boundary it actually reaches. It must abstain from
claims about arbitrary network partitions, disk power-loss durability, cross-region availability, application side effects,
human receipt, production capacity, SLA, or external acceptance unless matching evidence exists.

## License

MIT. Third-party study material and broker data are not included.
