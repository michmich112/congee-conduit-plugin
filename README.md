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
- `handler/` — gRPC Handler, intercept decisions, canonical index reconciliation
- `web/` — Svelte 5 static UI compiled to `ui/`

## Install

GitHub Releases ship per-platform tarballs (`plugin.json` + `bin/conduit-plugin` + `ui/`) for `linux_amd64`, `linux_arm64`, and `darwin_arm64`. (go-libsql does not vendor a `darwin_amd64` C library, so Intel macOS is not released.) In the Congee admin UI, **Plugins → Install** with the tarball URL and the **archive** SHA-256 from the release notes.

Raise `plugins.intercept_timeout_ms` to at least 200 (250 in Congee `config.example.json`).

On-device ranking downloads MiniLM ONNX, `tokenizer.json`, and onnxruntime into `$CONGEE_PLUGIN_DATA_DIR` (`plugins/conduit/data/`) via `--hook=install` / `--hook=launch`. Re-install (admin **Update**) keeps `data/` and runs `--hook=update` to apply index schema migrations without re-downloading models. Uninstall runs `--hook=uninstall` to delete those blobs; `wipe_data` also drops the index. Set `CONDUIT_EMBEDDER=fake` for tests only.

Vector width is `embed_dim` (default 384). Indexes can use a verified OpenAI-compatible embeddings URL that returns that many floats; after Test + Save, MiniLM is not loaded.

## Index reconciliation

Stored-event callbacks invalidate coordinates; the plugin reads Congee's current event for each coordinate before changing the index. Startup and **Rebuild** reconcile persisted rows, then discover current events. A one-minute pass checks up to 100 persisted coordinates and the 100 most recent canonical events to repair missed callbacks. Source-read or index-write errors keep the plugin unready until a full retry succeeds.

Congee's stored-event queue is nonblocking, so an index update can lag a relay write until its callback or reconciliation pass. The current host event query only pages by timestamp: when a page is full, events sharing its oldest second may be skipped, and status reports a partial pass. A complete, bounded sweep needs Congee to expose a cursor over canonical events ordered by `(created_at, id)`; a stable snapshot/watermark would also fence concurrent writes. New coordinates older than the recent-event window may remain absent until a complete sweep is available. The plugin relies on Congee's storage and deletion enforcement for canonical NIP-01/NIP-09 state.
