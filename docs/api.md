# HTTP API

## Transport and identity

- The server listens on `127.0.0.1` only.
- `/health` is unauthenticated. Every `/v1` route requires `Authorization: Bearer <MQ_API_TOKEN>`.
- Mutating operator and group requests require an `Idempotency-Key` of 8–128 restricted characters.
- Publish identity is carried in the body as producer ID, epoch, and base sequence; it does not use the header key.
- Publish also requires `X-Leader-Epoch`.
- Every response is JSON with `Cache-Control: no-store`.

Examples below use synthetic values. They show shape, not a public endpoint or credential.

## Health and state

### `GET /health`

Returns `200` and `{"status":"ok"}`. It proves only that the HTTP process can answer, not that PostgreSQL or replica objects are
healthy.

### `GET /v1/state`

Returns the sole partition's leader ID/epoch, `nextOffset`, committed `highWatermark`, `logStartOffset`, active head ID or `null`,
operator revision, and counts of replicating intents, committed batches, and groups.

## Producer lifecycle

### `POST /v1/producers`

```json
{"producerId":"producer-a"}
```

Returns `201` when created or `200` when already present:

```json
{"producerId":"producer-a","epoch":0,"nextSequence":0,"created":true}
```

### `POST /v1/producers/{producerId}/epochs`

Requires `Idempotency-Key`.

```json
{"expectedEpoch":0}
```

The result increments the epoch and resets its next sequence to zero. A producer cannot rotate while it owns the active head.
Stale expected epoch returns `fenced`; an exact key retry returns `replayed: true`.

## Publish

### `POST /v1/records`

Requires `X-Leader-Epoch`.

```json
{
  "producerId":"producer-a",
  "producerEpoch":0,
  "baseSequence":0,
  "records":[
    {
      "key":"order-001",
      "headers":[{"name":"kind","value":"created"}],
      "value":"synthetic-payload"
    }
  ]
}
```

New commit returns `201`; committed replay returns `200`:

```json
{
  "phase":"batch_committed",
  "topic":"events",
  "partition":0,
  "producerId":"producer-a",
  "producerEpoch":0,
  "baseSequence":0,
  "baseOffset":0,
  "nextOffset":1,
  "recordCount":1,
  "batchDigest":"<sha256>",
  "leaderEpoch":1,
  "replicaCount":3,
  "replayed":false
}
```

`replicaCount` is the number of distinct current-epoch receipt-backed objects re-read before commit. It is not an independent-host
durability count. A failed request with `insufficient_replicas` may have left a durable resumable intent and one object; retry the
exact body and epoch rather than changing identity.

## Leadership

### `POST /v1/leadership`

Requires `Idempotency-Key`.

```json
{"expectedEpoch":1,"newLeaderId":"replica-b"}
```

Returns the new leader/epoch, new partition revision, and any still-active intent ID. Existing old-epoch replica receipts do not
count toward later commit.

## Consumer group

### `POST /v1/groups/{groupId}/members`

Requires `Idempotency-Key`.

```json
{"memberId":"member-a"}
```

Creates a new generation and returns its member, checkpoint, and previous generation. Every distinct successful join revokes the
old generation; there is no heartbeat/session protocol in v0.1.

### `GET /v1/groups/{groupId}`

Returns current generation/member, durable checkpoint, generation fetch cursor, and assignment status.

### `POST /v1/groups/{groupId}/fetch`

Requires `Idempotency-Key`.

```json
{
  "memberId":"member-a",
  "generation":1,
  "offset":0,
  "maxRecords":128,
  "maxBytes":1048576
}
```

Returns the requested offset, frozen committed upper bound, next offset, ordered records with offsets, and replay flag. Offset must
equal the current generation cursor. An exact key retry reconstructs the same interval; changed body under that key conflicts.

An empty result is legal when offset equals the frozen high watermark. A stale generation returns `fenced`; a retained-away offset
returns `offset_out_of_range`; no readable committed copy returns `replica_corrupt_or_missing`.

### `POST /v1/groups/{groupId}/checkpoints`

Requires `Idempotency-Key`.

```json
{"memberId":"member-a","generation":1,"nextOffset":1}
```

Returns previous and next durable offsets. The next offset may equal the current checkpoint, but cannot move backward or pass this
generation's fetched cursor/high watermark. This receipt says nothing about external side effects.

## Retention

### `POST /v1/retention`

Requires `Idempotency-Key`.

```json
{"expectedRevision":0,"newLogStart":1}
```

The boundary may only move forward within the committed range. Success increments the partition revision. Existing objects remain
on disk; this is a logical visibility boundary, not physical deletion.

## Error vocabulary

| HTTP | Code | Meaning |
|---:|---|---|
| 400 | `invalid_request` | shape, bound, canonical identity, path, or required header failed |
| 401 | `unauthorized` | bearer token absent or different |
| 404 | `not_found` | producer, group, receipt, or route target is absent |
| 409 | `conflict` | a stable identity is bound to changed intent or a monotonic range is invalid |
| 409 | `fenced` | leader, producer, or group generation is stale; `details.current` gives the authority value |
| 409 | `out_of_order` | producer sequence or fetch cursor is not the exact next value |
| 409 | `offset_out_of_range` | requested progress is outside retained committed bounds |
| 412 | `precondition_failed` | operator partition revision changed |
| 503 | `partition_busy` | another unfinished head blocks this append/epoch operation |
| 503 | `insufficient_replicas` | fewer than two current-epoch objects passed readback |
| 503 | `replica_corrupt_or_missing` | no receipt-backed committed object can be verified |
| 500 | `internal_error` | unexpected failure; response and ordinary log omit exception text |

Clients should branch on `error`, not English `message`. Retrying is safe only with the exact stable identity and intent. A new key
is a new request, not a recovery token.
