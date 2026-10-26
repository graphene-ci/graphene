package services

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gopherex/xlog"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

// A backend that stops answering must not hold the exporter's call: the
// payload is queued and the export answered at once; what the queue
// cannot take is dropped and counted.
func TestOTLPForwardDoesNotWaitForTheBackend(t *testing.T) {
	release := make(chan struct{})
	received := make(chan struct{}, forwardQueue+forwardSenders)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- struct{}{}
		<-release
	}))
	defer backend.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := &OTLP{Logs: backend.URL, Log: xlog.New(xlog.NopCore{})}
	o.Start(ctx)

	total := forwardQueue + forwardSenders + 10
	start := time.Now()
	for range total {
		o.ForwardLogs(ctx, "default", &collogspb.ExportLogsServiceRequest{})
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("exports waited for a stuck backend: %s", took)
	}
	// Every sender holds one payload at the backend, the queue is full,
	// and the rest is shed.
	for range forwardSenders {
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("senders never reached the backend")
		}
	}
	if got := o.Dropped(); got < 10 {
		t.Fatalf("dropped %d, want at least 10", got)
	}
}
