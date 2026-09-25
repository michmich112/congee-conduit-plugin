package nip85

import (
	"encoding/hex"
	"strconv"
	"strings"

	sdk "github.com/michmich112/congee/sdk/plugin"
)

const KindUserAssertion = 30382

// UserRank is one provider's signed assertion about a user pubkey. The host
// verifies event signatures before delivering stored events to plugins.
type UserRank struct {
	Provider  string
	Target    string
	EventID   string
	CreatedAt int64
	Rank      int
}

func ValidPubkey(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

// ParseUserRank rejects ambiguous or out-of-range tags. It never treats an
// absent assertion as rank zero; absence is neutral in search.
func ParseUserRank(ev sdk.Event) (UserRank, bool) {
	if ev.Kind != KindUserAssertion || !ValidPubkey(ev.PubKey) || !ValidPubkey(ev.ID) || ev.CreatedAt <= 0 {
		return UserRank{}, false
	}
	var target string
	rank := -1
	for _, tag := range ev.Tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "d":
			if target != "" || !ValidPubkey(tag[1]) {
				return UserRank{}, false
			}
			target = tag[1]
		case "rank":
			if rank >= 0 {
				return UserRank{}, false
			}
			value, err := strconv.Atoi(tag[1])
			if err != nil || value < 0 || value > 100 {
				return UserRank{}, false
			}
			rank = value
		}
	}
	if target == "" || rank < 0 {
		return UserRank{}, false
	}
	return UserRank{Provider: ev.PubKey, Target: target, EventID: ev.ID, CreatedAt: ev.CreatedAt, Rank: rank}, true
}
