package handler

import (
	"strings"

	sdk "github.com/michmich112/congee/sdk/plugin"
)

type decisionKind int

const (
	decPassthrough decisionKind = iota
	decReshape
	decRespondSearch
	decRespondGeo
	decRespondRankAll
)

type decision struct {
	kind    decisionKind
	filters []sdk.Filter
}

func decide(req sdk.Req, st Settings, ready bool) decision {
	// The index answers one filter at a time. Let Congee handle OR filters and
	// constraints that the index cannot enforce rather than returning a partial
	// or over-broad result set.
	if !ready || len(req.Filters) != 1 {
		return decision{kind: decPassthrough}
	}
	product := toSet(st.ProductKinds)
	stall := toSet(st.StallKinds)

	for _, f := range req.Filters {
		if len(f.IDs) != 0 {
			return decision{kind: decPassthrough}
		}
		for name := range f.Tags {
			if !st.GeoEnabled || strings.TrimPrefix(name, "#") != "g" {
				return decision{kind: decPassthrough}
			}
		}
		hasSearch := strings.TrimSpace(f.Search) != ""
		hasGeo := false
		if st.GeoEnabled {
			for name, vals := range f.Tags {
				n := strings.TrimPrefix(name, "#")
				if n == "g" && len(vals) > 0 {
					hasGeo = true
					break
				}
			}
		}
		kindsEmpty := len(f.Kinds) == 0
		hitsProduct := false
		for _, k := range f.Kinds {
			if product[k] {
				hitsProduct = true
			}
			if !product[k] && !stall[k] {
				return decision{kind: decPassthrough}
			}
		}

		if hasSearch && kindsEmpty && st.InjectProductKindsOnSearch {
			nf := cloneFilter(f)
			nf.Kinds = append([]int{}, st.ProductKinds...)
			return decision{kind: decReshape, filters: []sdk.Filter{nf}}
		}
		if hasSearch && hitsProduct {
			return decision{kind: decRespondSearch}
		}
		if hasGeo && !hasSearch && hitsProduct {
			return decision{kind: decRespondGeo}
		}
		if st.RankAllProductReqs && hitsProduct {
			if hasSearch {
				return decision{kind: decRespondSearch}
			}
			return decision{kind: decRespondRankAll}
		}
	}
	return decision{kind: decPassthrough}
}

func cloneFilter(f sdk.Filter) sdk.Filter {
	out := f
	if f.IDs != nil {
		out.IDs = append([]string(nil), f.IDs...)
	}
	if f.Authors != nil {
		out.Authors = append([]string(nil), f.Authors...)
	}
	if f.Kinds != nil {
		out.Kinds = append([]int(nil), f.Kinds...)
	}
	if f.Tags != nil {
		out.Tags = map[string][]string{}
		for k, v := range f.Tags {
			out.Tags[k] = append([]string(nil), v...)
		}
	}
	return out
}

func geoPrefixes(f sdk.Filter) []string {
	var out []string
	for name, vals := range f.Tags {
		n := strings.TrimPrefix(name, "#")
		if n != "g" {
			continue
		}
		out = append(out, vals...)
	}
	return out
}

func toSet(in []int) map[int]bool {
	m := map[int]bool{}
	for _, n := range in {
		m[n] = true
	}
	return m
}

func kindsForSearch(f sdk.Filter, st Settings) []int {
	if len(f.Kinds) == 0 {
		return st.ProductKinds
	}
	var out []int
	prod := toSet(st.ProductKinds)
	stall := toSet(st.StallKinds)
	for _, k := range f.Kinds {
		if prod[k] || stall[k] {
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		return st.ProductKinds
	}
	return out
}

func mergeLimit(f sdk.Filter, max int) int {
	const fallback = 500
	reqLimit := 0
	if f.Limit != nil && *f.Limit > 0 {
		reqLimit = *f.Limit
	}
	if max <= 0 {
		if reqLimit > 0 {
			return reqLimit
		}
		return fallback
	}
	if reqLimit > 0 && reqLimit < max {
		return reqLimit
	}
	return max
}
