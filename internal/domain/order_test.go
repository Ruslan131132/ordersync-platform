package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewFreshOrderHashIgnoresFormatting(t *testing.T) {
	t.Parallel()
	a, err := NewFreshOrder("ERP-1", MarketplaceWBLike, json.RawMessage(`{"sku":"A","qty":1}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewFreshOrder("ERP-1", MarketplaceWBLike, json.RawMessage("{\n  \"sku\": \"A\",\n  \"qty\": 1\n}"))
	if err != nil {
		t.Fatal(err)
	}
	if a.ContentHash != b.ContentHash {
		t.Fatalf("hash differs for equivalent payloads: %s vs %s", a.ContentHash, b.ContentHash)
	}
}

func TestNewFreshOrderValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		id      string
		mp      Marketplace
		payload string
	}{
		{"empty id", "", MarketplaceWBLike, `{}`},
		{"long id", strings.Repeat("x", 129), MarketplaceWBLike, `{}`},
		{"unknown marketplace", "ERP-1", "amazon", `{}`},
		{"invalid json", "ERP-1", MarketplaceOzonLike, `{`},
		{"not an object", "ERP-1", MarketplaceOzonLike, `[1,2]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewFreshOrder(tc.id, tc.mp, json.RawMessage(tc.payload)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
