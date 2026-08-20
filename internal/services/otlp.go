package services

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gopherex/xlog"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/graphene-ci/graphene/internal/auth"
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

	// Per-signal OTLP/HTTP ingest URLs (e.g. the Victoria stack:
	// http://victoriatraces:10428/insert/opentelemetry/v1/traces).
	// Empty drops that signal.
	Traces  string
	Logs    string
	Metrics string
	Log     *xlog.Logger

	client *http.Client

	mu     sync.Mutex
	warned map[string]bool
}

// forward posts one OTLP protobuf payload to a backend.
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
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		o.Log.Error("telemetry request", xlog.String("signal", signal), xlog.Err(err))
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := o.httpClient().Do(req)
	if err != nil {
		o.Log.Warn("telemetry backend unreachable", xlog.String("signal", signal), xlog.Err(err))
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		o.Log.Warn("telemetry backend refused",
			xlog.String("signal", signal),
			xlog.String("status", resp.Status),
			xlog.String("body", string(body)))
	}
}

func (o *OTLP) httpClient() *http.Client {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.client == nil {
		o.client = &http.Client{Timeout: 15 * time.Second}
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
