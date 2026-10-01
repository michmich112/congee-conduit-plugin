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
CREATE INDEX IF NOT EXISTS listings_status_kind ON listings(status, kind);
CREATE INDEX IF NOT EXISTS listings_pubkey ON listings(pubkey);
CREATE INDEX IF NOT EXISTS listings_event_id ON listings(event_id);
CREATE INDEX IF NOT EXISTS listing_geo_geohash ON listing_geo(geohash);
CREATE VIRTUAL TABLE IF NOT EXISTS listings_fts USING fts5(
  title, body, content='listings', content_rowid='rowid', tokenize='unicode61'
);
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
CREATE INDEX IF NOT EXISTS listings_status_kind ON listings(status, kind);
CREATE INDEX IF NOT EXISTS listings_pubkey ON listings(pubkey);
CREATE INDEX IF NOT EXISTS listings_event_id ON listings(event_id);
CREATE INDEX IF NOT EXISTS listing_geo_geohash ON listing_geo(geohash);
CREATE INDEX IF NOT EXISTS listings_search_document ON listings USING GIN ((
  setweight(to_tsvector('simple', COALESCE(title, '')), 'A') ||
  setweight(to_tsvector('simple', COALESCE(body, '')), 'B')
));
`
