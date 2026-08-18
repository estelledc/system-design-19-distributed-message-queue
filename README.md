# Fenced Commit-Log Message Queue Lab

This clean-room system-design practice starts with one question: when leaders, replicas, producers, and consumer-group members can
all restart or race, what exact durable boundary lets a queue acknowledge an append and later advance consumption without
inventing “exactly once” delivery?

The prompt title is “Distributed Message Queue.” This repository does not copy a broker, source chapter, protocol, diagram,
benchmark, trace, or proprietary behavior. The initial contract was frozen from the title alone before reading the fixed secondary
chapter.

## Current phase

Closed-book contract only. No implementation, throughput result, durability result, public CI, production deployment, or external
acceptance is claimed yet.

The first runnable slice will be chosen only after comparing the fixed chapter with primary specifications and papers. Candidate
focus is one replicated partition with stable producer intent, committed-prefix reads, leader fencing, and consumer-group generation
fencing—not a feature-complete broker.

## Read first

- [Closed-book contract](docs/closed-book-contract.md)
- [Security policy](SECURITY.md)

## Evidence boundary

An accepted API response, a leader-local append, a quorum commit, a fetched record, a handler return, a committed offset, and a
downstream effect are different facts. The eventual lab must name and test each boundary it actually reaches. It must abstain from
claims about arbitrary network partitions, disk power-loss durability, cross-region availability, application side effects,
human receipt, production capacity, SLA, or external acceptance unless matching evidence exists.

## License

MIT. Third-party study material and broker data are not included.
