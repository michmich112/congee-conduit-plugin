package handler

import (
	"testing"

	sdk "github.com/michmich112/congee/sdk/plugin"
)

func TestDecidePassthroughNotReady(t *testing.T) {
	st := defaultSettings()
	d := decide(reqSearch(), st, false)
	if d.kind != decPassthrough {
		t.Fatalf("%v", d.kind)
	}
}

func TestDecideSearchRespond(t *testing.T) {
	st := defaultSettings()
	d := decide(reqSearch(), st, true)
	if d.kind != decRespondSearch {
		t.Fatalf("%v", d.kind)
	}
	d = decide(sdk.Req{Filters: []sdk.Filter{{Kinds: []int{30018}, Search: "bike"}}}, st, true)
	if d.kind != decRespondSearch {
		t.Fatalf("nip-15 product: %v", d.kind)
	}
}

func TestDecideInjectKinds(t *testing.T) {
	st := defaultSettings()
	st.InjectProductKindsOnSearch = true
	d := decide(sdk.Req{Filters: []sdk.Filter{{Search: "bike"}}}, st, true)
	if d.kind != decReshape || len(d.filters) != 1 || len(d.filters[0].Kinds) == 0 {
		t.Fatalf("%+v", d)
	}
}

func TestDecideGeo(t *testing.T) {
	st := defaultSettings()
	d := decide(sdk.Req{Filters: []sdk.Filter{{Kinds: []int{30402}, Tags: map[string][]string{"g": {"9q8"}}}}}, st, true)
	if d.kind != decRespondGeo {
		t.Fatalf("%v", d.kind)
	}
}

func TestDecideGeoWithoutProductKindsPassthrough(t *testing.T) {
	st := defaultSettings()
	cases := []sdk.Req{
		{Filters: []sdk.Filter{{Tags: map[string][]string{"g": {"9q8"}}}}},
		{Filters: []sdk.Filter{{Kinds: []int{1}, Tags: map[string][]string{"g": {"9q8"}}}}},
		{Filters: []sdk.Filter{{Kinds: []int{30017}, Tags: map[string][]string{"g": {"9q8"}}}}},
	}
	for i, req := range cases {
		d := decide(req, st, true)
		if d.kind != decPassthrough {
			t.Fatalf("case %d: %v", i, d.kind)
		}
	}
}

func TestDecideRankAll(t *testing.T) {
	st := defaultSettings()
	st.RankAllProductReqs = true
	d := decide(sdk.Req{Filters: []sdk.Filter{{Kinds: []int{30402}}}}, st, true)
	if d.kind != decRespondRankAll {
		t.Fatalf("%v", d.kind)
	}
}

func TestDecideEmptySearchPassthrough(t *testing.T) {
	st := defaultSettings()
	d := decide(sdk.Req{Filters: []sdk.Filter{{Search: "bike"}}}, st, true)
	if d.kind != decPassthrough {
		t.Fatalf("%v", d.kind)
	}
}

func TestMergeLimitUnlimitedHonorsREQ(t *testing.T) {
	n := 3
	got := mergeLimit(sdk.Filter{Limit: &n}, 0)
	if got != 3 {
		t.Fatalf("%d", got)
	}
	got = mergeLimit(sdk.Filter{}, 0)
	if got != 500 {
		t.Fatalf("%d", got)
	}
	got = mergeLimit(sdk.Filter{Limit: &n}, 50)
	if got != 3 {
		t.Fatalf("%d", got)
	}
}

func TestDecideKind1Passthrough(t *testing.T) {
	st := defaultSettings()
	d := decide(sdk.Req{Filters: []sdk.Filter{{Kinds: []int{1}, Search: "hi"}}}, st, true)
	if d.kind != decPassthrough {
		t.Fatalf("%v", d.kind)
	}
}

func TestDecideLeavesUnsupportedNIP01FiltersToRelay(t *testing.T) {
	st := defaultSettings()
	cases := []sdk.Req{
		{Filters: []sdk.Filter{{Kinds: []int{30402}, Search: "bike"}, {Kinds: []int{30402}, Search: "helmet"}}},
		{Filters: []sdk.Filter{{Kinds: []int{30402}, Search: "bike", IDs: []string{"id"}}}},
		{Filters: []sdk.Filter{{Kinds: []int{30402}, Search: "bike", Tags: map[string][]string{"t": {"cycling"}}}}},
		{Filters: []sdk.Filter{{Kinds: []int{30402, 1}, Search: "bike"}}},
	}
	for i, req := range cases {
		if got := decide(req, st, true).kind; got != decPassthrough {
			t.Fatalf("case %d: %v", i, got)
		}
	}
}

func reqSearch() sdk.Req {
	return sdk.Req{Filters: []sdk.Filter{{Kinds: []int{30402}, Search: "bike"}}}
}
