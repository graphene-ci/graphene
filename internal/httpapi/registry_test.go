package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegistryUploadLocation(t *testing.T) {
	for _, location := range []string{"absolute", "relative", "external"} {
		t.Run(location, func(t *testing.T) {
			var upstreamURL string
			path := "/v2/ns/image/blobs/uploads/uuid?_state=a%2Fb"
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					t.Error("door credential forwarded upstream")
				}
				switch location {
				case "absolute":
					w.Header().Set("Location", upstreamURL+path)
				case "relative":
					w.Header().Set("Location", path)
				case "external":
					w.Header().Set("Location", "https://storage.example/signed")
				}
				w.WriteHeader(http.StatusAccepted)
			}))
			defer upstream.Close()
			upstreamURL = upstream.URL
			proxy, err := registryProxy(upstreamURL)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "https://public.example/v2/ns/image/blobs/uploads/", nil)
			req.Header.Set("Authorization", "Bearer test-token")
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, req)
			want := path
			if location == "external" {
				want = "https://storage.example/signed"
			}
			if response.Code != http.StatusAccepted || response.Header().Get("Location") != want {
				t.Fatalf("status=%d location=%q", response.Code, response.Header().Get("Location"))
			}
		})
	}
}
