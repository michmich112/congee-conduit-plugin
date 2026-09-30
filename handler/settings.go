package handler

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/kinds"
	"github.com/michmich112/conduit-plugin/listing"
	"github.com/michmich112/conduit-plugin/nip85"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

// Settings is Conduit plugin configuration (JSON in host item.settings).
type Settings struct {
	IndexBackend               string `json:"index_backend"`
	PostgresURL                string `json:"postgres_url"`
	PostgresUser               string `json:"postgres_user"`
	PostgresPassword           string `json:"postgres_password,omitempty"`
	ProductKinds               []int  `json:"product_kinds"`
	StallKinds                 []int  `json:"stall_kinds"`
	DraftKinds                 []int  `json:"draft_kinds"`
	DeletionKinds              []int  `json:"deletion_kinds"`
	IndexDrafts                bool   `json:"index_drafts"`
	GeoEnabled                 bool   `json:"geo_enabled"`
	VectorEnabled              bool   `json:"vector_enabled"`
	EmbedProvider              string `json:"embed_provider"`
	EmbedHTTPURL               string `json:"embed_http_url"`
	EmbedHTTPModel             string `json:"embed_http_model"`
	EmbedHTTPAPIKey            string `json:"embed_http_api_key,omitempty"`
	EmbedDim                   int    `json:"embed_dim"`
	EmbedModelURL              string `json:"embed_model_url"`
	EmbedRuntimeURL            string `json:"embed_runtime_url"`
	ActiveFilter               bool   `json:"active_filter"`
	RankAllProductReqs         bool   `json:"rank_all_product_reqs"`
	InjectProductKindsOnSearch bool   `json:"inject_product_kinds_on_search"`
	MaxResults                 int    `json:"max_results"`
	GeoMinPrefixLen            int    `json:"geo_min_prefix_len"`
	SearchCandidateCap         int    `json:"search_candidate_cap"`
	NIP85ProviderPubkey        string `json:"nip85_provider_pubkey"`
	NIP85MaxAgeDays            int    `json:"nip85_max_age_days"`
}

const defaultNIP85Provider = "78ed0837eba0ba244384195ce41d2a21575476a8e99e43f02d6e9729860e29e6"

func defaultSettings() Settings {
	return Settings{
		IndexBackend:               "turso",
		ProductKinds:               listing.DefaultProductKinds(),
		StallKinds:                 listing.DefaultStallKinds(),
		DraftKinds:                 listing.DefaultDraftKinds(),
		DeletionKinds:              listing.DefaultDeletionKinds(),
		GeoEnabled:                 true,
		VectorEnabled:              true,
		EmbedProvider:              "on_device",
		EmbedDim:                   embed.DefaultDim,
		ActiveFilter:               true,
		MaxResults:                 0,
		GeoMinPrefixLen:            2,
		SearchCandidateCap:         1000,
		NIP85ProviderPubkey:        defaultNIP85Provider,
		NIP85MaxAgeDays:            14,
		InjectProductKindsOnSearch: false,
	}
}

func parseSettings(raw json.RawMessage) (Settings, error) {
	s := defaultSettings()
	if len(raw) == 0 || string(raw) == "null" {
		return s, nil
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("settings: %w", err)
	}
	s.IndexBackend = strings.ToLower(strings.TrimSpace(s.IndexBackend))
	if s.IndexBackend == "" {
		s.IndexBackend = "turso"
	}
	if s.IndexBackend != "turso" && s.IndexBackend != "postgres" {
		return s, fmt.Errorf("settings: index_backend must be turso or postgres")
	}
	s.EmbedProvider = strings.ToLower(strings.TrimSpace(s.EmbedProvider))
	if s.EmbedProvider == "" {
		s.EmbedProvider = "on_device"
	}
	if s.EmbedProvider != "on_device" && s.EmbedProvider != "http" {
		return s, fmt.Errorf("settings: embed_provider must be on_device or http")
	}
	s.EmbedHTTPURL = strings.TrimSpace(s.EmbedHTTPURL)
	s.EmbedHTTPModel = strings.TrimSpace(s.EmbedHTTPModel)
	s.EmbedModelURL = strings.TrimSpace(s.EmbedModelURL)
	s.EmbedRuntimeURL = strings.TrimSpace(s.EmbedRuntimeURL)
	if s.EmbedDim <= 0 {
		s.EmbedDim = embed.DefaultDim
	}
	if s.EmbedDim < embed.MinDim || s.EmbedDim > embed.MaxDim {
		return s, fmt.Errorf("settings: embed_dim must be between %d and %d", embed.MinDim, embed.MaxDim)
	}
	if s.MaxResults < 0 {
		s.MaxResults = 0
	}
	if s.GeoMinPrefixLen <= 0 {
		s.GeoMinPrefixLen = 2
	}
	if s.SearchCandidateCap <= 0 {
		s.SearchCandidateCap = 1000
	}
	s.NIP85ProviderPubkey = strings.ToLower(strings.TrimSpace(s.NIP85ProviderPubkey))
	if s.NIP85ProviderPubkey != "" && !nip85.ValidPubkey(s.NIP85ProviderPubkey) {
		return s, fmt.Errorf("settings: nip85_provider_pubkey must be a 64-character hex pubkey or empty")
	}
	if s.NIP85MaxAgeDays <= 0 {
		s.NIP85MaxAgeDays = 14
	}
	if len(s.ProductKinds) == 0 {
		s.ProductKinds = listing.DefaultProductKinds()
	} else {
		s.ProductKinds = keepKindsWithAnyRole(s.ProductKinds, kinds.RoleProduct, kinds.RoleListing)
		if len(s.ProductKinds) == 0 {
			s.ProductKinds = listing.DefaultProductKinds()
		}
	}
	if len(s.StallKinds) == 0 {
		s.StallKinds = listing.DefaultStallKinds()
	} else {
		s.StallKinds = keepKindsWithAnyRole(s.StallKinds, kinds.RoleStall)
		if len(s.StallKinds) == 0 {
			s.StallKinds = listing.DefaultStallKinds()
		}
	}
	if len(s.DraftKinds) == 0 {
		s.DraftKinds = listing.DefaultDraftKinds()
	} else {
		s.DraftKinds = keepKindsWithAnyRole(s.DraftKinds, kinds.RoleListingDraft)
		if len(s.DraftKinds) == 0 {
			s.DraftKinds = listing.DefaultDraftKinds()
		}
	}
	if len(s.DeletionKinds) == 0 {
		s.DeletionKinds = listing.DefaultDeletionKinds()
	} else {
		s.DeletionKinds = keepKindsWithAnyRole(s.DeletionKinds, kinds.RoleDeletion)
		if len(s.DeletionKinds) == 0 {
			s.DeletionKinds = listing.DefaultDeletionKinds()
		}
	}
	s.InjectProductKindsOnSearch = false
	return s, nil
}

func (s Settings) redacted() Settings {
	c := s
	c.PostgresPassword = ""
	c.EmbedHTTPAPIKey = ""
	return c
}

func (s Settings) allIndexKinds() []int {
	out := append([]int{}, s.ProductKinds...)
	out = append(out, s.StallKinds...)
	if s.IndexDrafts {
		out = append(out, s.DraftKinds...)
	}
	out = append(out, s.DeletionKinds...)
	return uniqueInts(out)
}

func uniqueInts(in []int) []int {
	seen := map[int]struct{}{}
	var out []int
	for _, n := range in {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

func keepKindsWithAnyRole(in []int, roles ...string) []int {
	var out []int
	seen := map[int]struct{}{}
	for _, n := range in {
		if _, ok := seen[n]; ok {
			continue
		}
		keep := false
		for _, role := range roles {
			if kinds.HasRole(n, role) {
				keep = true
				break
			}
		}
		if !keep {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

func subscriptionsFor(s Settings) []sdk.TrafficSubscription {
	storedKinds := s.allIndexKinds()
	if s.NIP85ProviderPubkey != "" {
		storedKinds = append(storedKinds, nip85.KindUserAssertion)
	}
	return []sdk.TrafficSubscription{
		{Kinds: storedKinds, OnStoredEvent: true},
		{
			MessageTypes: []string{"REQ"},
			Kinds:        s.ProductKinds,
			InterceptREQ: true,
		},
	}
}

type secretsFile struct {
	PostgresPassword     string `json:"postgres_password,omitempty"`
	EmbedHTTPAPIKey      string `json:"embed_http_api_key,omitempty"`
	EmbedHTTPFingerprint string `json:"embed_http_fingerprint,omitempty"`
}

func loadSecretsFile(dataDir string) (secretsFile, error) {
	var s secretsFile
	p := filepath.Join(dataDir, "secrets.json")
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, err
	}
	return s, nil
}

func saveSecretsFile(dataDir string, s secretsFile) error {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	p := filepath.Join(dataDir, "secrets.json")
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(p, b, 0o600)
}

func loadSecrets(dataDir string) (password string, err error) {
	s, err := loadSecretsFile(dataDir)
	if err != nil {
		return "", err
	}
	return s.PostgresPassword, nil
}

func saveSecrets(dataDir, password string) error {
	s, err := loadSecretsFile(dataDir)
	if err != nil {
		return err
	}
	s.PostgresPassword = password
	return saveSecretsFile(dataDir, s)
}

func postgresDSN(s Settings, password string) string {
	if strings.TrimSpace(s.PostgresURL) != "" {
		u := s.PostgresURL
		if password != "" && !strings.Contains(u, "password=") {
			if strings.Contains(u, "?") {
				u += "&password=" + password
			} else if strings.Contains(u, "@") {
				return u
			} else {
				u += "?password=" + password
			}
		}
		return u
	}
	user := s.PostgresUser
	if user == "" {
		user = "postgres"
	}
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:5432/conduit?sslmode=disable", user, password)
}
