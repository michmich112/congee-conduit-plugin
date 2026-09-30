package index

const schemaSQL = `
CREATE TABLE IF NOT EXISTS listings (
  coord TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  kind INTEGER NOT NULL,
  pubkey TEXT NOT NULL,
  d_tag TEXT NOT NULL,
  stall_id TEXT,
  status TEXT NOT NULL,
  inactive_reason TEXT,
  title TEXT,
  body TEXT,
  text_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS listing_geo (
  coord TEXT PRIMARY KEY,
  geohash TEXT,
  lat REAL,
  lon REAL
);
CREATE TABLE IF NOT EXISTS listing_embeddings (
  coord TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  dim INTEGER NOT NULL,
  vector BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS index_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reconcile_jobs (
 job_key TEXT PRIMARY KEY, coord TEXT NOT NULL, kind INTEGER NOT NULL, author TEXT NOT NULL,
 version BIGINT NOT NULL, attempts INTEGER NOT NULL, next_attempt BIGINT NOT NULL,
 pending_since BIGINT NOT NULL, last_error TEXT NOT NULL, cursor TEXT NOT NULL, phase TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reconcile_jobs_due ON reconcile_jobs(next_attempt,pending_since);
CREATE INDEX IF NOT EXISTS reconcile_jobs_coord ON reconcile_jobs(coord);
CREATE INDEX IF NOT EXISTS reconcile_jobs_merchant ON reconcile_jobs(author,kind) WHERE coord='';
CREATE TABLE IF NOT EXISTS reconcile_stage (
 job_key TEXT NOT NULL, version BIGINT NOT NULL, coord TEXT NOT NULL, listing TEXT NOT NULL,
 PRIMARY KEY(job_key,version,coord)
);
CREATE INDEX IF NOT EXISTS listings_status_kind ON listings(status, kind);
CREATE INDEX IF NOT EXISTS listings_pubkey ON listings(pubkey);
CREATE INDEX IF NOT EXISTS listings_event_id ON listings(event_id);
CREATE INDEX IF NOT EXISTS listings_reconcile_target ON listings((CASE WHEN kind IN (30017,30018) THEN CAST(kind AS TEXT) || ':' || pubkey || ':*' ELSE coord END));
CREATE INDEX IF NOT EXISTS listing_geo_geohash ON listing_geo(geohash);
`

const pgSchemaSQL = `
CREATE TABLE IF NOT EXISTS listings (
  coord TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  kind INTEGER NOT NULL,
  pubkey TEXT NOT NULL,
  d_tag TEXT NOT NULL,
  stall_id TEXT,
  status TEXT NOT NULL,
  inactive_reason TEXT,
  title TEXT,
  body TEXT,
  text_hash TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS listing_geo (
  coord TEXT PRIMARY KEY,
  geohash TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS listing_embeddings (
  coord TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  dim INTEGER NOT NULL,
  vector BYTEA NOT NULL
);
CREATE TABLE IF NOT EXISTS index_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reconcile_jobs (
 job_key TEXT PRIMARY KEY, coord TEXT NOT NULL, kind INTEGER NOT NULL, author TEXT NOT NULL,
 version BIGINT NOT NULL, attempts INTEGER NOT NULL, next_attempt BIGINT NOT NULL,
 pending_since BIGINT NOT NULL, last_error TEXT NOT NULL, cursor TEXT NOT NULL, phase TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reconcile_jobs_due ON reconcile_jobs(next_attempt,pending_since);
CREATE INDEX IF NOT EXISTS reconcile_jobs_coord ON reconcile_jobs(coord);
CREATE INDEX IF NOT EXISTS reconcile_jobs_merchant ON reconcile_jobs(author,kind) WHERE coord='';
CREATE TABLE IF NOT EXISTS reconcile_stage (
 job_key TEXT NOT NULL, version BIGINT NOT NULL, coord TEXT NOT NULL, listing TEXT NOT NULL,
 PRIMARY KEY(job_key,version,coord)
);
CREATE INDEX IF NOT EXISTS listings_status_kind ON listings(status, kind);
CREATE INDEX IF NOT EXISTS listings_pubkey ON listings(pubkey);
CREATE INDEX IF NOT EXISTS listings_event_id ON listings(event_id);
CREATE INDEX IF NOT EXISTS listings_reconcile_target ON listings((CASE WHEN kind IN (30017,30018) THEN CAST(kind AS TEXT) || ':' || pubkey || ':*' ELSE coord END));
CREATE INDEX IF NOT EXISTS listing_geo_geohash ON listing_geo(geohash);
`
