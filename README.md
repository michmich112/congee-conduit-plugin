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

Stored-event callbacks durably invalidate coordinates and return without querying Congee or computing embeddings. Dirty coordinates are hidden from ranked search until a reconciliation job succeeds. A failed source read or embedding retries only that job, with persisted backoff; unrelated search remains available. Status reports pending jobs, their oldest age, and failed jobs. If the index database cannot record invalidations, the plugin fails open through the host.

One cancelable scheduler performs discovery, stored-coordinate checks, and pending retries. Each source page contains at most 200 events; stored targets are enumerated in batches of 100; at most four jobs run together with 30-second deadlines. Product/stall legacy events are scanned once per merchant and kind, with durable cursors and staged winners. Application uses batches of ten listings and pruning uses batches of 100 coordinates. Interrupted scans never prove absence or prune rows. There is no 1,001-event merchant ceiling.

Congee's additive cursor RPC orders retained events by `created_at DESC, id ASC`, so a full timestamp boundary is traversed without skipping same-second events. Discovery covers the retained source, including older coordinates whose callbacks were missed. Completed discovery and stored-target sweeps restart after one minute; job retries continue between sweeps. A sweep is not a pinned snapshot: events inserted before its cursor are discovered by a later sweep. This provides convergence, not complete historical export or immediate visibility.

Embeddings run before short database transactions. Job versions and coordinate versions reject obsolete results, and SQL listings, geo rows, and embeddings commit together. ANN candidates are checked against the current SQL event identity. Non-legacy winners are read again after embedding before their dirty marker is cleared. Rebuild, settings replacement, and shutdown cancel and wait for the previous scheduler before closing its store.

This requires the cursor-capable Congee host and SDK from the [Congee support branch](https://github.com/ericfj2140/congee/tree/fix/plugin-cursor-paging). Older hosts report an explicit paging compatibility error and do not complete discovery. Until upstream publishes the updated nested SDK module, `go.mod` pins the support commit in the fork; replace that pin with the official SDK version after release. The plugin relies on Congee's retained canonical NIP-01 state and deletion enforcement.

## License

The Conduit plugin is licensed under the [MIT License](LICENSE).
