package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsResponseBoundaries(t *testing.T) {
	const limit = 8 << 20
	for _, tc := range []struct {
		name, body, wantError string
		status                int
	}{
		{"complete", `{"status":"success","data":{"result":[]}}`, "", http.StatusOK},
		{"exact limit", `{"padding":"` + strings.Repeat("x", limit-14) + `"}`, "", http.StatusOK},
		{"oversized", `{"padding":"` + strings.Repeat("x", limit) + `"}`, "exceeds 8 MiB", http.StatusOK},
		{"invalid json", `{"data":`, "invalid JSON", http.StatusOK},
		{"backend error", "unavailable", "503 Service Unavailable", http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/query_range" {
					t.Errorf("unexpected path: %s", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			p := &PromQL{Base: server.URL, Client: server.Client()}
			for _, raw := range []bool{false, true} {
				var result []byte
				var err error
				if raw {
					result, err = p.RawMetrics(context.Background(), "up", time.Unix(100, 0), time.Unix(200, 0))
				} else {
					result, err = p.Series(context.Background(), Selector{Namespace: "test", Attribute: "graphene.run", Value: "run-1"}, time.Unix(100, 0), time.Unix(200, 0))
				}
				if tc.wantError != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantError) || result != nil {
						t.Fatalf("raw=%v: got %d bytes, error %v; want %q and no partial data", raw, len(result), err, tc.wantError)
					}
				} else if err != nil || string(result) != tc.body {
					t.Fatalf("raw=%v: got %d bytes, error %v; want complete response (%d bytes)", raw, len(result), err, len(tc.body))
				}
			}
		})
	}
}
