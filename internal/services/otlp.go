package services

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopherex/xlog"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/telemetry"
)

// OTLP is the telemetry half of the door: workers and agents export
// traces, logs, and metrics with the STANDARD OTLP gRPC protocol to
// the same address they already dial, authenticated by the same token.
// The server stamps the caller's namespace onto every resource (a
// client cannot lie about it) and forwards each signal to its backend
// over OTLP/HTTP — the backends speak OTLP themselves, so the server
// IS the collector. A signal without a backend is accepted and
// dropped: telemetry must never fail a run.
type OTLP struct {
	coltracepb.UnimplementedTraceServiceServer
	collogspb.UnimplementedLogsServiceServer
	colmetricspb.UnimplementedMetricsServiceServer

	// Hub fans every accepted signal out to live follow streams; the
	// forward to storage and the live fan-out are independent halves —
	// a dead backend does not kill live tails, a slow tail does not
	// block the forward.
	Hub *telemetry.Hub

	// Per-signal OTLP/HTTP ingest URLs (e.g. the Victoria stack:
	// http://victoriatraces:10428/insert/opentelemetry/v1/traces).
	// Empty drops that signal.
	Traces  string
	Logs    string
	Metrics string
	Log     *xlog.Logger

	client  *http.Client
	queue   chan shipment
	dropped atomic.Int64

	mu        sync.Mutex
	warned    map[string]bool
	droppedAt time.Time
}

// The forward to storage runs BEHIND the door, not inside the caller's
// RPC: an export is answered once its payload is queued, and senders
// drain the queue. A backend that slows down then costs queued
// payloads — counted and said — instead of holding every exporter's
// call until its deadline, which is what turned one slow backend into
// "DeadlineExceeded" on every agent at once.
const (
	// forwardQueue bounds payloads waiting for a sender.
	forwardQueue = 4096
	// forwardSenders posts to the backends in parallel.
	forwardSenders = 16
	// forwardTimeout bounds one post.
	forwardTimeout = 10 * time.Second
)

// shipment is one payload on its way to a backend.
type shipment struct {
	signal, url string
	raw         []byte
}

// Start opens the queue and its senders; they live until ctx ends.
// Without Start the forward is synchronous — the shape tests use.
func (o *OTLP) Start(ctx context.Context) {
	o.queue = make(chan shipment, forwardQueue)
	for range forwardSenders {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case sh := <-o.queue:
					o.post(ctx, sh)
				}
			}
		}()
	}
}

// Dropped is the number of payloads shed because the queue was full.
func (o *OTLP) Dropped() int64 { return o.dropped.Load() }

// forward hands one OTLP protobuf payload to its backend.
func (o *OTLP) forward(ctx context.Context, signal, url string, msg proto.Message) {
	if url == "" {
		o.warnOnce(signal)
		return
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		o.Log.Error("telemetry marshal", xlog.String("signal", signal), xlog.Err(err))
		return
	}
	sh := shipment{signal: signal, url: url, raw: raw}
	if o.queue == nil {
		o.post(ctx, sh)
		return
	}
	select {
	case o.queue <- sh:
	default:
		o.dropped.Add(1)
		o.warnDropped()
	}
}

// post sends one payload; the caller's cancellation does not cut it.
func (o *OTLP) post(ctx context.Context, sh shipment) {
	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), forwardTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, sh.url, bytes.NewReader(sh.raw))
	if err != nil {
		o.Log.Error("telemetry request", xlog.String("signal", sh.signal), xlog.Err(err))
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := o.httpClient().Do(req)
	if err != nil {
		o.Log.Warn("telemetry backend unreachable", xlog.String("signal", sh.signal), xlog.Err(err))
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		o.Log.Warn("telemetry backend refused",
			xlog.String("signal", sh.signal),
			xlog.String("status", resp.Status),
			xlog.String("body", string(body)))
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

// warnDropped says the queue sheds payloads, at most once a minute.
func (o *OTLP) warnDropped() {
	o.mu.Lock()
	now := time.Now()
	say := now.Sub(o.droppedAt) >= time.Minute
	if say {
		o.droppedAt = now
	}
	o.mu.Unlock()
	if say {
		o.Log.Warn("telemetry backends fall behind — payloads dropped",
			xlog.Int64("dropped_total", o.dropped.Load()))
	}
}

func (o *OTLP) httpClient() *http.Client {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.client == nil {
		// Every sender keeps its connection: the default keeps two idle
		// per host, and the rest dial anew on every post.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConnsPerHost = forwardSenders
		o.client = &http.Client{Timeout: 15 * time.Second, Transport: transport}
	}
	return o.client
}

func (o *OTLP) warnOnce(signal string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.warned == nil {
		o.warned = map[string]bool{}
	}
	if !o.warned[signal] {
		o.warned[signal] = true
		o.Log.Warn("no backend configured — signal is accepted and dropped", xlog.String("signal", signal))
	}
}

// ForwardLogs is the in-process door for the server's own tailers
// (managed run containers): same namespace stamping, same backend —
// no gRPC round-trip and no token, the caller IS the server.
func (o *OTLP) ForwardLogs(ctx context.Context, namespace string, req *collogspb.ExportLogsServiceRequest) {
	for _, rl := range req.GetResourceLogs() {
		stampNamespace(rl.GetResource(), namespace)
	}
	o.forward(ctx, "logs", o.Logs, req)
	o.publish(telemetry.RouteLogs(namespace, req))
}

// publish hands envelopes to the live hub, if one is wired.
func (o *OTLP) publish(envs []telemetry.Envelope) {
	if o.Hub == nil {
		return
	}
	for _, env := range envs {
		o.Hub.Publish(env)
	}
}

// Export forwards traces.
func (o *OTLP) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	namespace, err := scope(ctx, auth.RoleRun, auth.RoleAgent, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	for _, rs := range req.GetResourceSpans() {
		stampNamespace(rs.GetResource(), namespace)
	}
	o.forward(ctx, "traces", o.Traces, req)
	o.publish(telemetry.RouteSpans(namespace, req))
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// otlpLogs carries the logs half (the Export method name collides
// across the three generated services).
type otlpLogs struct{ *OTLP }

// OTLPLogs exposes the logs service.
func (o *OTLP) OTLPLogs() collogspb.LogsServiceServer { return otlpLogs{o} }

func (o otlpLogs) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	namespace, err := scope(ctx, auth.RoleRun, auth.RoleAgent, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	for _, rl := range req.GetResourceLogs() {
		stampNamespace(rl.GetResource(), namespace)
	}
	o.forward(ctx, "logs", o.Logs, req)
	o.publish(telemetry.RouteLogs(namespace, req))
	return &collogspb.ExportLogsServiceResponse{}, nil
}

type otlpMetrics struct{ *OTLP }

// OTLPMetrics exposes the metrics service.
func (o *OTLP) OTLPMetrics() colmetricspb.MetricsServiceServer { return otlpMetrics{o} }

func (o otlpMetrics) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	namespace, err := scope(ctx, auth.RoleRun, auth.RoleAgent, auth.RoleAdmin)
	if err != nil {
		return nil, err
	}
	for _, rm := range req.GetResourceMetrics() {
		stampNamespace(rm.GetResource(), namespace)
	}
	o.forward(ctx, "metrics", o.Metrics, req)
	o.publish(telemetry.RouteMetrics(namespace, req))
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// Close is kept for the composition root's symmetry.
func (o *OTLP) Close() {}

// stampNamespace upserts graphene.namespace on the resource — the
// caller's token decides, never the payload.
func stampNamespace(res *resourcepb.Resource, namespace string) {
	if res == nil {
		return
	}
	for _, kv := range res.Attributes {
		if kv.GetKey() == "graphene.namespace" {
			kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: namespace}}
			return
		}
	}
	res.Attributes = append(res.Attributes, &commonpb.KeyValue{
		Key:   "graphene.namespace",
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: namespace}},
	})
}
