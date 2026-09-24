# Conduit

Marketplace index plugin for [Congee](https://github.com/michmich112/congee). Indexes NIP-15 and NIP-99 listings already stored on the relay (geo + vector rank). It never writes events to the relay.

Depends only on [`github.com/michmich112/congee/sdk/plugin`](https://github.com/michmich112/congee/tree/main/sdk/plugin):

```bash
go get github.com/michmich112/congee/sdk/plugin@v0.1.0
```

A sibling Congee checkout is not required. For local ABI iteration, `cp go.work.example go.work` (do not commit `go.work`).

Build with `CGO_ENABLED=1` (Turso/libSQL + onnxruntime).

## Layout

- `listing/` — parse NIP-15 / NIP-99 / kind 5 using roles from `kinds.json`
- `kinds.json` — kind numbers, NIPs, names, and roles (stall, product, listing, community, …)
- `embed/` — on-device MiniLM (onnxruntime) unless `CONDUIT_EMBEDDER=fake`
- `index/` — Turso default, Postgres optional, ANN cache, Search
- `handler/` — gRPC Handler, intercept decisions, watermark backfill
- `web/` — Svelte 5 static UI compiled to `ui/`

## Product search

For a supported single-filter NIP-50 product request, the plugin retrieves
semantic matches across its active vector index and text matches from the
listing title/description index. It unions those candidates, ranks relevance
first, then applies a small 75-day half-life freshness preference and
same-publisher duplicate/exposure controls. `search_candidate_cap` bounds each
query-relevant retrieval branch; it is not a newest-products window. Products
without vectors can still appear through text matching. If query embedding is
unavailable, only text matches are returned. Unsupported compound filters pass
through to Congee's native NIP-50 search.
The original subscription filter stays intact, so an initial search response
does not turn into a stream of unrelated new product events.
Congee currently does not match live events against NIP-50 search text, so
this change covers stored results at REQ time rather than live search updates.

The default candidate cap is 1,000 per retrieval branch. The status endpoint
reports cumulative search counts, failures, searches above 200 ms, and
semantic versus lexical fallback use. These counters reset when the plugin
restarts.
Ranking uses deterministic ties within a query snapshot. NIP-50 does not
define a relevance cursor, so `since`/`until` windows cannot guarantee stable
deep pagination when listings or scores change.

This index reflects the relay's stored current product revisions and deletions.
Complete backfill across events sharing a timestamp, replacement/deletion
consistency, and NIP-77 synchronization are separate prerequisites for relying
on it as a complete product corpus. Use the search benchmark with representative
queries, corpus size, and concurrent load before raising production limits;
the Congee intercept deadline is 200 ms for this plugin.

## Install

GitHub Releases ship per-platform tarballs (`plugin.json` + `bin/conduit-plugin` + `ui/`) for `linux_amd64`, `linux_arm64`, and `darwin_arm64`. (go-libsql does not vendor a `darwin_amd64` C library, so Intel macOS is not released.) In the Congee admin UI, **Plugins → Install** with the tarball URL and the **archive** SHA-256 from the release notes.

Raise `plugins.intercept_timeout_ms` to at least 200 (250 in Congee `config.example.json`).

On-device ranking downloads MiniLM ONNX, `tokenizer.json`, and onnxruntime into `$CONGEE_PLUGIN_DATA_DIR` (`plugins/conduit/data/`) via `--hook=install` / `--hook=launch`. Re-install (admin **Update**) keeps `data/` and runs `--hook=update` to apply index schema migrations without re-downloading models. Uninstall runs `--hook=uninstall` to delete those blobs; `wipe_data` also drops the index. Set `CONDUIT_EMBEDDER=fake` for tests only.

Vector width is `embed_dim` (default 384). Indexes can use a verified OpenAI-compatible embeddings URL that returns that many floats; after Test + Save, MiniLM is not loaded.
