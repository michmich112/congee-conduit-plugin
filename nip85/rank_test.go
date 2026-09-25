package nip85

import (
	"strings"
	"testing"

	sdk "github.com/michmich112/congee/sdk/plugin"
)

func TestParseUserRank(t *testing.T) {
	ev := sdk.Event{
		ID: strings.Repeat("c", 64), PubKey: strings.Repeat("a", 64),
		CreatedAt: 100, Kind: KindUserAssertion,
		Tags: [][]string{{"d", strings.Repeat("b", 64)}, {"rank", "85"}, {"context", "all"}},
	}
	got, ok := ParseUserRank(ev)
	if !ok || got.Rank != 85 || got.Target != strings.Repeat("b", 64) || got.Provider != ev.PubKey {
		t.Fatalf("valid assertion: %+v %t", got, ok)
	}
	for _, rank := range []string{"0", "100"} {
		ev.Tags[1][1] = rank
		if _, ok := ParseUserRank(ev); !ok {
			t.Fatalf("valid boundary rank %s", rank)
		}
	}
	for _, rank := range []string{"-1", "101", "NaN"} {
		ev.Tags[1][1] = rank
		if _, ok := ParseUserRank(ev); ok {
			t.Fatalf("accepted invalid rank %s", rank)
		}
	}
	ev.Tags[1][1] = "85"
	for _, tags := range [][][]string{
		{{"d", "short"}, {"rank", "85"}},
		{{"d", strings.Repeat("b", 64)}, {"rank", "85"}, {"rank", "90"}},
		{{"d", strings.Repeat("b", 64)}, {"rank", "85"}, {"d", strings.Repeat("d", 64)}},
	} {
		ev.Tags = tags
		if _, ok := ParseUserRank(ev); ok {
			t.Fatalf("accepted ambiguous assertion %v", tags)
		}
	}
}
