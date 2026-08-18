CREATE TABLE IF NOT EXISTS mq_partitions (
  topic text NOT NULL,
  partition_id integer NOT NULL CHECK (partition_id = 0),
  leader_id text NOT NULL CHECK (leader_id IN ('replica-a', 'replica-b', 'replica-c')),
  leader_epoch bigint NOT NULL CHECK (leader_epoch >= 1),
  next_offset bigint NOT NULL CHECK (next_offset >= 0 AND next_offset <= 9007199254740991),
  high_watermark bigint NOT NULL CHECK (high_watermark >= 0 AND high_watermark <= 9007199254740991),
  log_start_offset bigint NOT NULL CHECK (log_start_offset >= 0 AND log_start_offset <= 9007199254740991),
  active_intent_id uuid,
  state_revision bigint NOT NULL CHECK (state_revision >= 0 AND state_revision <= 9007199254740991),
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (topic, partition_id),
  CHECK (log_start_offset <= high_watermark AND high_watermark <= next_offset),
  CHECK ((active_intent_id IS NULL AND next_offset = high_watermark) OR active_intent_id IS NOT NULL)
);

INSERT INTO mq_partitions (
  topic, partition_id, leader_id, leader_epoch,
  next_offset, high_watermark, log_start_offset, state_revision, updated_at
)
VALUES ('events', 0, 'replica-a', 1, 0, 0, 0, 0, clock_timestamp())
ON CONFLICT (topic, partition_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS mq_producers (
  producer_id text PRIMARY KEY CHECK (producer_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
  epoch bigint NOT NULL CHECK (epoch >= 0 AND epoch <= 9007199254740991),
  next_sequence bigint NOT NULL CHECK (next_sequence >= 0 AND next_sequence <= 9007199254740991),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS mq_publish_intents (
  intent_id uuid PRIMARY KEY,
  topic text NOT NULL,
  partition_id integer NOT NULL,
  producer_id text NOT NULL REFERENCES mq_producers(producer_id),
  producer_epoch bigint NOT NULL CHECK (producer_epoch >= 0),
  base_sequence bigint NOT NULL CHECK (base_sequence >= 0),
  base_offset bigint NOT NULL CHECK (base_offset >= 0),
  record_count integer NOT NULL CHECK (record_count BETWEEN 1 AND 64),
  batch_digest text NOT NULL CHECK (batch_digest ~ '^[0-9a-f]{64}$'),
  batch_bytes integer NOT NULL CHECK (batch_bytes BETWEEN 1 AND 262144),
  staged_bytes bytea,
  status text NOT NULL CHECK (status IN ('replicating', 'committed')),
  started_leader_epoch bigint NOT NULL CHECK (started_leader_epoch >= 1),
  committed_leader_epoch bigint,
  result_json jsonb,
  created_at timestamptz NOT NULL,
  committed_at timestamptz,
  UNIQUE (producer_id, producer_epoch, base_sequence),
  UNIQUE (topic, partition_id, base_offset),
  FOREIGN KEY (topic, partition_id) REFERENCES mq_partitions(topic, partition_id),
  CHECK (
    (status = 'replicating' AND staged_bytes IS NOT NULL AND octet_length(staged_bytes) = batch_bytes
      AND committed_leader_epoch IS NULL AND result_json IS NULL AND committed_at IS NULL)
    OR
    (status = 'committed' AND staged_bytes IS NULL
      AND committed_leader_epoch IS NOT NULL AND result_json IS NOT NULL AND committed_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS mq_replica_receipts (
  intent_id uuid NOT NULL REFERENCES mq_publish_intents(intent_id) ON DELETE CASCADE,
  replica_id text NOT NULL CHECK (replica_id IN ('replica-a', 'replica-b', 'replica-c')),
  leader_epoch bigint NOT NULL CHECK (leader_epoch >= 1),
  batch_digest text NOT NULL CHECK (batch_digest ~ '^[0-9a-f]{64}$'),
  batch_bytes integer NOT NULL CHECK (batch_bytes BETWEEN 1 AND 262144),
  verified_at timestamptz NOT NULL,
  PRIMARY KEY (intent_id, replica_id, leader_epoch)
);

CREATE TABLE IF NOT EXISTS mq_batches (
  topic text NOT NULL,
  partition_id integer NOT NULL,
  base_offset bigint NOT NULL CHECK (base_offset >= 0),
  next_offset bigint NOT NULL CHECK (next_offset > base_offset),
  record_count integer NOT NULL CHECK (record_count BETWEEN 1 AND 64),
  batch_digest text NOT NULL CHECK (batch_digest ~ '^[0-9a-f]{64}$'),
  batch_bytes integer NOT NULL CHECK (batch_bytes BETWEEN 1 AND 262144),
  intent_id uuid NOT NULL UNIQUE REFERENCES mq_publish_intents(intent_id),
  committed_leader_epoch bigint NOT NULL CHECK (committed_leader_epoch >= 1),
  committed_at timestamptz NOT NULL,
  PRIMARY KEY (topic, partition_id, base_offset),
  FOREIGN KEY (topic, partition_id) REFERENCES mq_partitions(topic, partition_id),
  CHECK (next_offset = base_offset + record_count)
);

CREATE TABLE IF NOT EXISTS mq_mutation_receipts (
  principal text NOT NULL CHECK (principal ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
  request_key text NOT NULL CHECK (request_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$'),
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  operation text NOT NULL CHECK (operation IN ('bump_producer_epoch', 'rotate_leader', 'advance_retention')),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (principal, request_key)
);

CREATE TABLE IF NOT EXISTS mq_groups (
  group_id text PRIMARY KEY CHECK (group_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
  current_generation bigint NOT NULL CHECK (current_generation >= 0 AND current_generation <= 9007199254740991),
  current_member_id text,
  checkpoint_offset bigint NOT NULL CHECK (checkpoint_offset >= 0 AND checkpoint_offset <= 9007199254740991),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  CHECK ((current_generation = 0 AND current_member_id IS NULL) OR (current_generation > 0 AND current_member_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS mq_group_assignments (
  group_id text NOT NULL REFERENCES mq_groups(group_id) ON DELETE CASCADE,
  generation bigint NOT NULL CHECK (generation > 0),
  member_id text NOT NULL CHECK (member_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
  status text NOT NULL CHECK (status IN ('current', 'revoked')),
  fetch_cursor bigint NOT NULL CHECK (fetch_cursor >= 0 AND fetch_cursor <= 9007199254740991),
  assigned_at timestamptz NOT NULL,
  revoked_at timestamptz,
  PRIMARY KEY (group_id, generation),
  CHECK ((status = 'current' AND revoked_at IS NULL) OR (status = 'revoked' AND revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS mq_one_current_assignment
ON mq_group_assignments(group_id) WHERE status = 'current';

CREATE TABLE IF NOT EXISTS mq_join_receipts (
  group_id text NOT NULL REFERENCES mq_groups(group_id) ON DELETE CASCADE,
  request_key text NOT NULL CHECK (request_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$'),
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  member_id text NOT NULL,
  generation bigint NOT NULL CHECK (generation > 0),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (group_id, request_key),
  FOREIGN KEY (group_id, generation) REFERENCES mq_group_assignments(group_id, generation)
);

CREATE TABLE IF NOT EXISTS mq_fetch_receipts (
  fetch_id uuid PRIMARY KEY,
  group_id text NOT NULL REFERENCES mq_groups(group_id) ON DELETE CASCADE,
  request_key text NOT NULL CHECK (request_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$'),
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  member_id text NOT NULL,
  generation bigint NOT NULL CHECK (generation > 0),
  requested_offset bigint NOT NULL CHECK (requested_offset >= 0),
  frozen_log_start bigint NOT NULL CHECK (frozen_log_start >= 0),
  frozen_high_watermark bigint NOT NULL CHECK (frozen_high_watermark >= frozen_log_start),
  max_records integer NOT NULL CHECK (max_records BETWEEN 1 AND 128),
  max_bytes integer NOT NULL CHECK (max_bytes BETWEEN 8192 AND 1048576),
  status text NOT NULL CHECK (status IN ('accepted', 'complete', 'revoked')),
  returned_next_offset bigint,
  result_count integer,
  created_at timestamptz NOT NULL,
  completed_at timestamptz,
  UNIQUE (group_id, request_key),
  FOREIGN KEY (group_id, generation) REFERENCES mq_group_assignments(group_id, generation),
  CHECK (
    (status = 'accepted' AND returned_next_offset IS NULL AND result_count IS NULL AND completed_at IS NULL)
    OR
    (status = 'complete' AND returned_next_offset IS NOT NULL AND result_count IS NOT NULL AND completed_at IS NOT NULL)
    OR
    (status = 'revoked' AND returned_next_offset IS NULL AND result_count IS NULL AND completed_at IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS mq_checkpoint_receipts (
  group_id text NOT NULL REFERENCES mq_groups(group_id) ON DELETE CASCADE,
  request_key text NOT NULL CHECK (request_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$'),
  intent_digest text NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
  member_id text NOT NULL,
  generation bigint NOT NULL CHECK (generation > 0),
  next_offset bigint NOT NULL CHECK (next_offset >= 0),
  result_json jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (group_id, request_key),
  FOREIGN KEY (group_id, generation) REFERENCES mq_group_assignments(group_id, generation)
);

CREATE INDEX IF NOT EXISTS mq_batches_range
ON mq_batches(topic, partition_id, next_offset, base_offset);

CREATE INDEX IF NOT EXISTS mq_replica_receipts_digest
ON mq_replica_receipts(batch_digest, replica_id);
