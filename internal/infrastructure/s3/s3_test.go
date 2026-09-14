package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestPutSmallArtifactsUsesBoundedMemory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/artifacts/tenant/blob" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(400)
			return
		}
		var bodyReader io.Reader = r.Body
		if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
			bodyReader = httputil.NewChunkedReader(r.Body)
		}
		body, err := io.ReadAll(bodyReader)
		if err != nil || string(body) != "artifact" {
			t.Errorf("uploaded %q: %v", body, err)
		}
		w.Header().Set("ETag", `"test-etag"`)
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds: credentials.NewStaticV4("test-access", "test-secret", ""), Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{client: client, bucket: "artifacts"}
	for _, kind := range []string{"memory-offset", "file-offset", "stream", "empty"} {
		t.Run(kind, func(t *testing.T) {
			var reader io.Reader
			switch kind {
			case "memory-offset":
				r := strings.NewReader("prefixartifact")
				_, _ = r.Seek(6, io.SeekStart)
				reader = r
			case "file-offset":
				f, err := os.CreateTemp(t.TempDir(), "artifact")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = f.Close() }()
				if _, err = f.WriteString("prefixartifact"); err != nil {
					t.Fatal(err)
				}
				if _, err = f.Seek(6, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				reader = f
			case "stream":
				reader = struct{ io.Reader }{strings.NewReader("artifact")}
			case "empty":
				body, size, cleanup, err := uploadBody(strings.NewReader(""))
				if err != nil {
					t.Fatal(err)
				}
				defer cleanup()
				data, err := io.ReadAll(body)
				if err != nil || len(data) != 0 || size != 0 {
					t.Fatalf("empty: %d %q %v", size, data, err)
				}
				return
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			size, err := store.Put(context.Background(), "tenant", "blob", reader)
			runtime.ReadMemStats(&after)
			if err != nil || size != 8 {
				t.Fatalf("put: %d %v", size, err)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("allocated %d bytes", allocated)
			if allocated > 64<<20 {
				t.Fatalf("tiny artifact allocated %d bytes", allocated)
			}
		})
	}
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("source failed") }

func TestUploadBodyRemovesSpool(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	body, size, cleanup, err := uploadBody(struct{ io.Reader }{strings.NewReader("artifact")})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "artifact" || size != 8 {
		t.Fatalf("body: %q %d %v", data, size, err)
	}
	cleanup()
	if _, _, _, err := uploadBody(brokenReader{}); err == nil {
		t.Fatal("read error lost")
	}
	files, err := filepath.Glob(filepath.Join(dir, "graphene-s3-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("spool leak: %v %v", files, err)
	}
}
