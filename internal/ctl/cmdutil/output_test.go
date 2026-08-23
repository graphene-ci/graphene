package cmdutil

import (
	"encoding/base64"
	"reflect"
	"testing"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestDecodeEmbedded(t *testing.T) {
	in := map[string]any{
		"resource": map[string]any{
			// spec carries JSON — decodes into the object.
			"spec": b64(`{"size":10}`),
			// state carries JSON with a NESTED embedded field —
			// decode recurses into what it just decoded.
			"state": b64(`{"manifest":"` + b64(`{"kinds":["docker"]}`) + `"}`),
			// not JSON — stays as the original string.
			"payload": b64("plain text"),
			// unknown key — untouched even though it is base64 JSON.
			"digest": b64(`{"x":1}`),
		},
		"items": []any{
			map[string]any{"params": b64(`[1,2]`)},
		},
	}
	want := map[string]any{
		"resource": map[string]any{
			"spec":    map[string]any{"size": float64(10)},
			"state":   map[string]any{"manifest": map[string]any{"kinds": []any{"docker"}}},
			"payload": b64("plain text"),
			"digest":  b64(`{"x":1}`),
		},
		"items": []any{
			map[string]any{"params": []any{float64(1), float64(2)}},
		},
	}
	if got := decodeEmbedded(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("decodeEmbedded:\n got %#v\nwant %#v", got, want)
	}
}
