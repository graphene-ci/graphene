package services

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gopherex/xlog"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
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

// The queue is bounded by bytes as well as by count: large payloads are
// shed before they hold more than the budget.
func TestOTLPQueueIsBoundedByBytes(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer backend.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := &OTLP{Logs: backend.URL, Log: xlog.New(xlog.NopCore{})}
	o.Start(ctx)

	big := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: strings.Repeat("x", 1<<20)}},
		}}}},
	}}}
	const sent = forwardBudget>>20 + 64
	for range sent {
		o.ForwardLogs(ctx, "default", big)
	}
	if q := o.queued.Load(); q > forwardBudget {
		t.Fatalf("queue holds %d bytes, budget %d", q, forwardBudget)
	}
	if o.Dropped() < 64-forwardSenders {
		t.Fatalf("dropped %d of %d payloads past the budget", o.Dropped(), sent)
	}
}
