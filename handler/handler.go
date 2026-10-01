package handler

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/index"
	"github.com/michmich112/conduit-plugin/listing"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

// Version is the plugin release, injected at link time:
//
//	go build -ldflags "-X github.com/michmich112/conduit-plugin/handler.Version=0.1.0"
var Version = "0.1.0-dev"

// Handler implements sdk.Handler for Conduit.
type Handler struct {
	dataDir  string
	embedSel embed.Selection

	mu              sync.RWMutex
	settings        Settings
	store           index.Store
	ready           bool
	storeOpen       bool
	embedWarm       bool
	backfillStarted bool
	backfillState   string
	lastErr         string
	host            sdk.Host

	backfillGen     atomic.Int64
	backfillScanned atomic.Int64
	backfillIndexed atomic.Int64
	interceptN      atomic.Int64
	passthroughN    atomic.Int64
	respondN        atomic.Int64
	reshapeN        atomic.Int64
}

func New(dataDir string, sel embed.Selection) *Handler {
	return &Handler{
		dataDir:       dataDir,
		embedSel:      sel,
		settings:      defaultSettings(),
		backfillState: "idle",
	}
}

func (h *Handler) SetHost(host sdk.Host) { h.host = host }

func (h *Handler) Handshake(ctx context.Context, settings json.RawMessage) (*sdk.HandshakeResult, error) {
	if err := h.apply(ctx, settings, true); err != nil {
		return nil, err
	}
	h.startBackfill(context.WithoutCancel(ctx))
	h.mu.RLock()
	st := h.settings
	defer h.mu.RUnlock()
	return &sdk.HandshakeResult{
		PluginID:            "conduit",
		Name:                "Conduit",
		Version:             Version,
		Capabilities:        []string{sdk.CapIntercept, sdk.CapEventsRead, sdk.CapIndexOwn, sdk.CapAdminUI},
		Subscriptions:       subscriptionsFor(st),
		InterceptDeadlineMs: 200,
	}, nil
}

func (h *Handler) Health(ctx context.Context) (bool, string, error) {
	_ = ctx
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.ready {
		return true, "ready", nil
	}
	return false, "not ready: " + h.lastErr + " backfill=" + h.backfillState, nil
}

func (h *Handler) Observe(ctx context.Context, msg sdk.ObserveMessage) error {
	_ = ctx
	_ = msg
	return nil
}

func (h *Handler) OnStoredEvent(ctx context.Context, ev sdk.Event, stored bool) error {
	if !stored {
		return nil
	}
	h.mu.RLock()
	st := h.settings
	store := h.store
	h.mu.RUnlock()
	if store == nil {
		return nil
	}
	l, ok := listing.FromEvent(listing.Event{
		ID: ev.ID, PubKey: ev.PubKey, CreatedAt: ev.CreatedAt, Kind: ev.Kind, Tags: ev.Tags, Content: ev.Content,
	}, st.IndexDrafts)
	if !ok {
		return nil
	}
	return store.Upsert(ctx, l)
}

func (h *Handler) InterceptREQ(ctx context.Context, req sdk.Req) (*sdk.InterceptResult, error) {
	h.interceptN.Add(1)
	h.mu.RLock()
	st := h.settings
	ready := h.ready
	store := h.store
	h.mu.RUnlock()
	d := decide(req, st, ready)
	if d.kind == decPassthrough {
		h.passthroughN.Add(1)
		return &sdk.InterceptResult{Action: sdk.InterceptPassthrough}, nil
	}
	if d.kind == decReshape {
		h.reshapeN.Add(1)
		return &sdk.InterceptResult{Action: sdk.InterceptReshapeREQ, ReshapeFilters: d.filters}, nil
	}
	if store == nil {
		h.passthroughN.Add(1)
		return &sdk.InterceptResult{Action: sdk.InterceptPassthrough}, nil
	}
	f := sdk.Filter{}
	if len(req.Filters) > 0 {
		f = req.Filters[0]
	}
	// NIP-01 limit 0 asks for no stored events; clampLimit treats zero as a
	// default, so answer the empty snapshot before entering the index.
	if f.Limit != nil && *f.Limit == 0 {
		h.respondN.Add(1)
		return &sdk.InterceptResult{Action: sdk.InterceptRespond}, nil
	}
	q := index.Query{
		Search:             f.Search,
		Kinds:              kindsForSearch(f, st),
		Authors:            f.Authors,
		Since:              f.Since,
		Until:              f.Until,
		GeoPrefixes:        geoPrefixes(f),
		Limit:              mergeLimit(f, st.MaxResults),
		GeoMinPrefixLen:    st.GeoMinPrefixLen,
		SearchCandidateCap: st.SearchCandidateCap,
		ActiveOnly:         st.ActiveFilter,
		VectorEnabled:      st.VectorEnabled && h.embedSel.VectorRanking(),
		GeoEnabled:         st.GeoEnabled,
	}
	if d.kind == decRespondGeo {
		q.Search = ""
		q.VectorEnabled = false
	}
	ids, err := store.Search(ctx, q)
	if err != nil {
		h.passthroughN.Add(1)
		h.log(ctx, "warn", "intercept search failed", map[string]string{"error": err.Error()})
		return &sdk.InterceptResult{Action: sdk.InterceptPassthrough}, nil
	}
	h.respondN.Add(1)
	return &sdk.InterceptResult{
		Action:   sdk.InterceptRespond,
		EventIDs: ids,
	}, nil
}

func (h *Handler) ApplySettings(ctx context.Context, settings json.RawMessage) ([]sdk.TrafficSubscription, error) {
	if err := h.apply(ctx, settings, false); err != nil {
		return nil, err
	}
	h.mu.RLock()
	st := h.settings
	h.mu.RUnlock()
	return subscriptionsFor(st), nil
}

func (h *Handler) AdminAction(ctx context.Context, name string, payload json.RawMessage) (json.RawMessage, error) {
	switch name {
	case "rebuild":
		h.mu.Lock()
		store := h.store
		h.backfillStarted = false
		h.backfillState = "running"
		h.mu.Unlock()
		h.backfillScanned.Store(0)
		h.backfillIndexed.Store(0)
		gen := h.backfillGen.Add(1)
		if store != nil {
			_ = store.SetMeta(ctx, metaWatermark, "")
		}
		h.startBackfill(context.WithoutCancel(ctx))
		return json.Marshal(map[string]any{"ok": true, "generation": gen})
	case "test_store":
		h.mu.RLock()
		store := h.store
		h.mu.RUnlock()
		if store == nil {
			return json.Marshal(map[string]any{"ok": false, "error": "store not open"})
		}
		if err := store.Ping(ctx); err != nil {
			return json.Marshal(map[string]any{"ok": false, "error": err.Error()})
		}
		return json.Marshal(map[string]any{"ok": true})
	case "test_embed":
		return h.testEmbed(ctx, payload)
	case "ensure_assets":
		return h.ensureAssets(ctx, payload)
	case "list_listings":
		return h.listListings(ctx, payload)
	case "list_embeddings":
		return h.listEmbeddings(ctx, payload)
	case "get_event":
		return h.getEvent(ctx, payload)
	default:
		_ = payload
		return json.Marshal(map[string]any{"ok": false, "error": "unknown action"})
	}
}

func (h *Handler) Status(ctx context.Context) (*sdk.Status, error) {
	h.mu.RLock()
	st := h.settings
	ready := h.ready
	store := h.store
	bf := h.backfillState
	sel := h.embedSel
	h.mu.RUnlock()
	stats := index.Stats{Backend: st.IndexBackend}
	if store != nil {
		if s, err := store.Stats(ctx); err == nil {
			stats = s
		}
	}
	body, _ := json.Marshal(map[string]any{
		"backend":                 stats.Backend,
		"active":                  stats.Active,
		"inactive":                stats.Inactive,
		"embeddings":              stats.Embeddings,
		"embedding_mismatch":      stats.EmbeddingMismatch,
		"search_total":            stats.SearchTotal,
		"search_errors":           stats.SearchErrors,
		"search_over_200ms":       stats.SearchOver200ms,
		"search_semantic":         stats.SearchSemantic,
		"search_lexical_fallback": stats.SearchLexicalFallback,
		"backfill":                bf,
		"backfill_generation":     h.backfillGen.Load(),
		"backfill_scanned":        h.backfillScanned.Load(),
		"backfill_indexed":        h.backfillIndexed.Load(),
		"intercept_n":             h.interceptN.Load(),
		"passthrough_n":           h.passthroughN.Load(),
		"respond_n":               h.respondN.Load(),
		"reshape_n":               h.reshapeN.Load(),
		"settings":                st.redacted(),
		"assets":                  embed.InspectAssets(h.dataDir),
		"embedder": map[string]any{
			"model_id":       sel.ModelID,
			"source":         sel.Source,
			"error":          sel.Error,
			"warning":        sel.Warning,
			"dim":            sel.Dim,
			"required_dim":   st.EmbedDim,
			"default_dim":    embed.DefaultDim,
			"provider":       st.EmbedProvider,
			"http_verified":  sel.Source == embed.SourceHTTP,
			"vector_ranking": sel.VectorRanking() && st.VectorEnabled,
		},
	})
	return &sdk.Status{Ready: ready, JSON: body}, nil
}

func (h *Handler) apply(ctx context.Context, raw json.RawMessage, opening bool) error {
	st, err := parseSettings(raw)
	if err != nil {
		return err
	}
	h.mu.RLock()
	old := h.settings
	h.mu.RUnlock()
	sec, _ := loadSecretsFile(h.dataDir)
	if st.PostgresPassword != "" {
		sec.PostgresPassword = st.PostgresPassword
		st.PostgresPassword = ""
	}
	if st.EmbedHTTPAPIKey != "" {
		sec.EmbedHTTPAPIKey = st.EmbedHTTPAPIKey
		st.EmbedHTTPAPIKey = ""
	}
	_ = saveSecretsFile(h.dataDir, sec)
	h.embedSel = embed.SelectWith(embed.SelectOpts{
		ModelPath:        embed.DefaultModelPath(h.dataDir),
		Provider:         st.EmbedProvider,
		HTTPURL:          st.EmbedHTTPURL,
		HTTPModel:        st.EmbedHTTPModel,
		HTTPKey:          sec.EmbedHTTPAPIKey,
		SavedFingerprint: sec.EmbedHTTPFingerprint,
		Dim:              st.EmbedDim,
	})
	e := h.embedSel.Embedder
	if e != nil {
		if err := embed.Warm(ctx, e); err != nil {
			h.mu.Lock()
			h.lastErr = err.Error()
			h.embedWarm = false
			h.ready = false
			h.mu.Unlock()
			return err
		}
	}
	store, err := h.openStore(ctx, st, sec.PostgresPassword, e)
	if err != nil {
		h.mu.Lock()
		h.lastErr = err.Error()
		h.storeOpen = false
		h.ready = false
		h.mu.Unlock()
		return err
	}
	h.mu.Lock()
	if h.store != nil {
		_ = h.store.Close()
	}
	h.store = store
	h.settings = st
	h.storeOpen = true
	h.embedWarm = e != nil
	h.ready = true
	h.lastErr = ""
	h.mu.Unlock()
	keep := listing.DefaultStallKinds()
	keep = append(keep, listing.DefaultProductKinds()...)
	keep = append(keep, listing.DefaultDraftKinds()...)
	keep = append(keep, listing.DefaultDeletionKinds()...)
	if err := store.PurgeKindsNotIn(ctx, keep); err != nil {
		h.log(ctx, "warn", "purge non-marketplace kinds", map[string]string{"error": err.Error()})
	}
	reembed := !opening && (old.EmbedDim != st.EmbedDim ||
		old.EmbedProvider != st.EmbedProvider ||
		old.EmbedHTTPURL != st.EmbedHTTPURL ||
		old.EmbedHTTPModel != st.EmbedHTTPModel ||
		old.EmbedModelURL != st.EmbedModelURL)
	if reembed {
		_ = store.SetMeta(ctx, metaWatermark, "")
		h.mu.Lock()
		h.backfillStarted = false
		h.mu.Unlock()
		h.startBackfill(context.WithoutCancel(ctx))
	}
	if e == nil {
		h.log(ctx, "warn", "embedder unavailable", map[string]string{
			"error":   h.embedSel.Error,
			"warning": h.embedSel.Warning,
		})
	} else if h.embedSel.Warning != "" {
		h.log(ctx, "info", "embedder selected", map[string]string{
			"source":  h.embedSel.Source,
			"model":   h.embedSel.ModelID,
			"warning": h.embedSel.Warning,
		})
	}
	_ = opening
	return nil
}

func (h *Handler) openStore(ctx context.Context, st Settings, password string, e embed.Embedder) (index.Store, error) {
	if st.IndexBackend == "postgres" {
		return index.OpenPostgres(ctx, postgresDSN(st, password), e)
	}
	path := filepath.Join(h.dataDir, "conduit-index.db")
	return index.OpenTurso(ctx, path, e)
}

type listPayload struct {
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
	Status string `json:"status"`
}

func (h *Handler) listListings(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	h.mu.RLock()
	store := h.store
	h.mu.RUnlock()
	if store == nil {
		return json.Marshal(map[string]any{"ok": false, "error": "store not open"})
	}
	var p listPayload
	_ = json.Unmarshal(payload, &p)
	page, err := store.ListListings(ctx, index.ListQuery{Limit: p.Limit, Offset: p.Offset, Status: p.Status})
	if err != nil {
		return json.Marshal(map[string]any{"ok": false, "error": err.Error()})
	}
	return json.Marshal(map[string]any{"ok": true, "items": page.Items, "total": page.Total})
}

func (h *Handler) listEmbeddings(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	h.mu.RLock()
	store := h.store
	h.mu.RUnlock()
	if store == nil {
		return json.Marshal(map[string]any{"ok": false, "error": "store not open"})
	}
	var p listPayload
	_ = json.Unmarshal(payload, &p)
	page, err := store.ListEmbeddings(ctx, index.ListQuery{Limit: p.Limit, Offset: p.Offset})
	if err != nil {
		return json.Marshal(map[string]any{"ok": false, "error": err.Error()})
	}
	return json.Marshal(map[string]any{"ok": true, "items": page.Items, "total": page.Total})
}

func (h *Handler) getEvent(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		ID    string `json:"id"`
		Coord string `json:"coord"`
	}
	_ = json.Unmarshal(payload, &p)
	id := strings.ToLower(strings.TrimSpace(p.ID))
	if id == "" && strings.TrimSpace(p.Coord) != "" {
		h.mu.RLock()
		store := h.store
		h.mu.RUnlock()
		if store != nil {
			if l, ok, err := store.Get(ctx, p.Coord); err == nil && ok {
				id = strings.ToLower(strings.TrimSpace(l.EventID))
			}
		}
	}
	if id == "" {
		return json.Marshal(map[string]any{
			"ok": false, "missing": true,
			"error": "This index row has no event id. The listing may have been removed from the index.",
		})
	}
	if len(id) != 64 {
		return json.Marshal(map[string]any{"ok": false, "error": "invalid event id"})
	}
	if h.host == nil {
		return json.Marshal(map[string]any{"ok": false, "error": "host unavailable"})
	}
	evs, err := h.host.GetEventsByIDs(ctx, []string{id})
	if err != nil {
		return json.Marshal(map[string]any{"ok": false, "error": err.Error()})
	}
	if len(evs) == 0 {
		return json.Marshal(map[string]any{"ok": false, "missing": true, "id": id, "error": "not in relay event store"})
	}
	return json.Marshal(map[string]any{"ok": true, "event": evs[0]})
}

func (h *Handler) testEmbed(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		URL    string `json:"url"`
		Model  string `json:"model"`
		APIKey string `json:"api_key"`
		Dim    int    `json:"dim"`
	}
	_ = json.Unmarshal(payload, &p)
	h.mu.RLock()
	st := h.settings
	h.mu.RUnlock()
	url := strings.TrimSpace(p.URL)
	if url == "" {
		url = st.EmbedHTTPURL
	}
	model := strings.TrimSpace(p.Model)
	if model == "" {
		model = st.EmbedHTTPModel
	}
	dim := p.Dim
	if dim <= 0 {
		dim = st.EmbedDim
	}
	sec, err := loadSecretsFile(h.dataDir)
	if err != nil {
		return json.Marshal(map[string]any{"ok": false, "error": err.Error()})
	}
	key := p.APIKey
	if strings.TrimSpace(key) == "" {
		key = sec.EmbedHTTPAPIKey
	}
	if url == "" {
		return json.Marshal(map[string]any{"ok": false, "error": "url required"})
	}
	e := embed.NewHTTP(url, model, key, dim)
	if err := embed.Probe(ctx, e); err != nil {
		return json.Marshal(map[string]any{
			"ok":           false,
			"error":        err.Error(),
			"required_dim": dim,
		})
	}
	sec.EmbedHTTPFingerprint = embed.HTTPFingerprint(url, model, key, dim)
	if strings.TrimSpace(p.APIKey) != "" {
		sec.EmbedHTTPAPIKey = p.APIKey
	}
	if err := saveSecretsFile(h.dataDir, sec); err != nil {
		return json.Marshal(map[string]any{"ok": false, "error": err.Error()})
	}
	return json.Marshal(map[string]any{
		"ok":           true,
		"dim":          e.Dim(),
		"required_dim": e.Dim(),
		"model_id":     e.ModelID(),
	})
}

func (h *Handler) ensureAssets(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Force      bool   `json:"force"`
		ModelURL   string `json:"model_url"`
		RuntimeURL string `json:"runtime_url"`
	}
	_ = json.Unmarshal(payload, &p)
	h.mu.RLock()
	st := h.settings
	h.mu.RUnlock()
	modelURL := strings.TrimSpace(p.ModelURL)
	if modelURL == "" {
		modelURL = st.EmbedModelURL
	}
	runtimeURL := strings.TrimSpace(p.RuntimeURL)
	if runtimeURL == "" {
		runtimeURL = st.EmbedRuntimeURL
	}
	status, err := embed.EnsureAssets(ctx, embed.AssetOpts{
		DataDir:    h.dataDir,
		ModelURL:   modelURL,
		RuntimeURL: runtimeURL,
		Force:      p.Force,
	})
	if err != nil {
		return json.Marshal(map[string]any{"ok": false, "error": err.Error(), "assets": status})
	}
	return json.Marshal(map[string]any{"ok": true, "assets": status})
}

// RunLifecycleHook is --hook=install / --hook=launch: download assets, never panic.
func RunLifecycleHook(ctx context.Context, dataDir, settingsJSON string) error {
	st, err := parseSettings(json.RawMessage(settingsJSON))
	if err != nil {
		st = defaultSettings()
	}
	_, err = embed.EnsureAssets(ctx, embed.AssetOpts{
		DataDir:    dataDir,
		ModelURL:   st.EmbedModelURL,
		RuntimeURL: st.EmbedRuntimeURL,
	})
	return err
}

// RunUpdateHook is --hook=update: apply index schema migrations without re-downloading MiniLM.
func RunUpdateHook(ctx context.Context, dataDir, settingsJSON string) error {
	st, err := parseSettings(json.RawMessage(settingsJSON))
	if err != nil {
		st = defaultSettings()
	}
	sec, _ := loadSecretsFile(dataDir)
	var store index.Store
	if st.IndexBackend == "postgres" {
		store, err = index.OpenPostgres(ctx, postgresDSN(st, sec.PostgresPassword), embed.Fake{})
	} else {
		store, err = index.OpenTurso(ctx, filepath.Join(dataDir, "conduit-index.db"), embed.Fake{})
	}
	if err != nil {
		return err
	}
	return store.Close()
}

// RunUninstallHook is --hook=uninstall: delete downloaded MiniLM / ORT blobs only.
func RunUninstallHook(dataDir string) error {
	return embed.RemoveDownloadedAssets(dataDir)
}

func (h *Handler) log(ctx context.Context, level, msg string, fields map[string]string) {
	if h.host == nil {
		return
	}
	_ = h.host.Log(ctx, level, msg, fields)
}
