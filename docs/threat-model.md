# Threat model

## Scope and trust boundaries

The protected assets are synthetic record confidentiality, committed-byte integrity, partition/producer/group progress, stable
retry receipts, and evidence accuracy. The boundaries are:

1. local HTTP caller to loopback API;
2. API process to PostgreSQL;
3. API process to three local replica directories;
4. repository and lockfile to package registry, container registry, and CI actions.

The model assumes the local OS user, PostgreSQL administrator, repository maintainer, and CI runner are trusted. It does not defend
against a compromised host, malicious database administrator, arbitrary native dependency compromise, or Byzantine replicas.

## Threats, controls, and residual risk

| Threat | Implemented control | Residual risk / non-claim |
|---|---|---|
| Unauthenticated state mutation/read | exact bearer comparison; `/v1` auth; loopback bind | no TLS, token rotation, scopes, tenants, rate limiting, or remote deployment contract |
| Token/payload leakage through errors or logs | generic 500 body; ordinary-log allowlist; sentinel scans; no-store responses | process memory, PostgreSQL, filesystem, shell history, and explicit debug access remain sensitive |
| Changed retry overwrites prior intent | canonical digest and stable identity receipt conflict | SHA-256 collision/compromise excluded |
| Stale producer writes after takeover | producer epoch and exact sequence fencing | no producer lease, automatic epoch negotiation, or multi-partition transaction |
| Stale leader advances committed state | expected leader epoch on reserve, receipt, and finalize; current-epoch receipt count | PostgreSQL is single authority; no consensus or network-partition liveness proof |
| Duplicate or forged replica count | allowed replica ID set; unique `(intent, replica, epoch)`; digest/length match; readback before commit | directories share one host; an OS/DB administrator can forge state |
| Torn, replaced, or corrupt object | exclusive temp write, file sync, hard-link no-replace, directory sync, digest readback; fetch fallback | power-loss guarantees depend on OS/filesystem/device; repair and anti-rollback absent |
| Read uncommitted bytes | fetch queries committed batch metadata only and caps at frozen high watermark | database compromise or manual table edits bypass protocol |
| Old member checkpoints after rebalance | current member/generation/assignment locks and immutable generation receipts | already-returned bytes and external side effects cannot be revoked |
| Silent data loss after retention | monotonic revisioned log start and explicit out-of-range result | physical GC, legal holds, backup, and authorized reset absent |
| Path traversal through IDs/digests | fixed replica allowlist and strict lowercase 64-hex digest before path construction | replica root itself is operator-controlled |
| Resource exhaustion | request, record, batch, header, fetch, and DB statement bounds; one active head | no rate limits, quotas, admission queue, disk-watermark rejection, or slow-client controls |
| SQL injection | parameterized values; lock mode is private fixed code path | raw administrator access remains trusted |
| Dependency/action/container substitution | exact npm lock, one pinned runtime dependency, full action commit pins, container digest | registry/account compromise, malicious pinned artifact, and image provenance verification remain possible |
| Misleading evidence claim | named phase receipts, gate map, explicit exclusions, no exactly-once/production language | humans can still quote a receipt outside its documented boundary |

## Abuse cases worth testing

- Reuse a publish identity with different bytes.
- Race several exact publishers and verify one batch/offset interval.
- Rotate leader after one receipt and prove the old receipt does not count.
- Corrupt one committed object, then all receipt-backed copies.
- Join a newer member between fetch and checkpoint.
- Reuse a checkpoint key with a different next offset.
- Advance retention beyond a lagging group and require explicit failure.
- Send unknown fields, oversized values, unsafe integers, control characters, invalid path segments, and wrong auth.
- Crash after first replica receipt, publish commit, and checkpoint commit.
- Scan logs for synthetic secret/identity/payload/digest sentinels.

These cases are covered by the current local suite; exact public receipts belong in [verification](verification.md).

## Privacy stance

Only synthetic records and identities belong in this repository or its tests. Do not attach real credentials, customer data,
internal broker metadata, proprietary traces, or production endpoints to issues or CI artifacts. A digest, object hash, or passing
decoder proves byte identity only; it does not prove origin, consent, truth, or permissible use.

## Security response

When a public remote exists, report sensitive repository vulnerabilities through a GitHub security advisory. Public issues are
appropriate only for non-sensitive correctness defects. This lab is not a deployed broker and has no incident-response or data-
processing commitment.
