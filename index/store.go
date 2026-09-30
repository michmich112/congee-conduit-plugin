package index

import (
	"context"
	"math"
	"time"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/listing"
)

// Query is the search planner input.
type Query struct {
	Search             string
	Kinds              []int
	Authors            []string
	Since              *int64
	Until              *int64
	GeoPrefixes        []string
	Limit              int
	GeoMinPrefixLen    int
	SearchCandidateCap int
	ActiveOnly         bool
	VectorEnabled      bool
	GeoEnabled         bool
}

// Stats is Overview UI counts.
type Stats struct {
	Active            int64  `json:"active"`
	Inactive          int64  `json:"inactive"`
	Embeddings        int64  `json:"embeddings"`
	EmbeddingMismatch int64  `json:"embedding_mismatch"`
	Backend           string `json:"backend"`
}

// ListQuery pages through stored listings or embeddings.
type ListQuery struct {
	Limit  int
	Offset int
	Status string
}

// ListingListItem is a listing row for the admin UI.
type ListingListItem struct {
	Coord          string `json:"coord"`
	EventID        string `json:"event_id"`
	Kind           int    `json:"kind"`
	PubKey         string `json:"pubkey"`
	DTag           string `json:"d_tag"`
	StallID        string `json:"stall_id"`
	Status         string `json:"status"`
	InactiveReason string `json:"inactive_reason,omitempty"`
	Title          string `json:"title"`
	CreatedAt      int64  `json:"created_at"`
	Geohash        string `json:"geohash,omitempty"`
	HasEmbedding   bool   `json:"has_embedding"`
}

// EmbeddingListItem is an embedding row for the admin UI.
type EmbeddingListItem struct {
	Coord   string `json:"coord"`
	EventID string `json:"event_id"`
	Model   string `json:"model"`
	Dim     int    `json:"dim"`
	Title   string `json:"title"`
	Status  string `json:"status"`
	Kind    int    `json:"kind"`
	DTag    string `json:"d_tag"`
	PubKey  string `json:"pubkey"`
}

// ListingPage is a paged listing list.
type ListingPage struct {
	Items []ListingListItem `json:"items"`
	Total int64             `json:"total"`
}

// EmbeddingPage is a paged embedding list.
type EmbeddingPage struct {
	Items []EmbeddingListItem `json:"items"`
	Total int64               `json:"total"`
}

// Store is the only persistence API.
type Store interface {
	Upsert(ctx context.Context, l listing.Listing) error
	ReplaceCanonical(ctx context.Context, l listing.Listing) error
	MarkInactive(ctx context.Context, pubkey string, eventIDs, coords []string) error
	DeleteCoord(ctx context.Context, coord string) error
	CoordsForEventIDs(ctx context.Context, pubkey string, eventIDs []string) ([]string, error)
	CoordsAfter(ctx context.Context, after string, limit int) ([]string, error)
	Get(ctx context.Context, coord string) (listing.Listing, bool, error)
	Search(ctx context.Context, q Query) ([]string, error)
	ListListings(ctx context.Context, q ListQuery) (ListingPage, error)
	ListEmbeddings(ctx context.Context, q ListQuery) (EmbeddingPage, error)
	Meta(ctx context.Context, key string) (string, error)
	SetMeta(ctx context.Context, key, value string) error
	Stats(ctx context.Context) (Stats, error)
	Ping(ctx context.Context) error
	Close() error
	PurgeKindsNotIn(ctx context.Context, keep []int) error
}

func clampLimit(n, max int) int {
	if n <= 0 {
		return 20
	}
	if max > 0 && n > max {
		return max
	}
	return n
}

func nowUnix() int64 { return time.Now().Unix() }

func floatsToBytes(v []float32) []byte {
	b := make([]byte, len(v)*4)
	for i, x := range v {
		u := math.Float32bits(x)
		b[i*4] = byte(u)
		b[i*4+1] = byte(u >> 8)
		b[i*4+2] = byte(u >> 16)
		b[i*4+3] = byte(u >> 24)
	}
	return b
}

func bytesToFloats(b []byte) []float32 {
	n := len(b) / 4
	v := make([]float32, n)
	for i := 0; i < n; i++ {
		u := uint32(b[i*4]) | uint32(b[i*4+1])<<8 | uint32(b[i*4+2])<<16 | uint32(b[i*4+3])<<24
		v[i] = math.Float32frombits(u)
	}
	return v
}

func ensureEmbedder(e embed.Embedder) embed.Embedder {
	return e
}
