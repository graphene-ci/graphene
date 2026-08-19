package services

import (
	"context"
	"sync"

	"github.com/gopherex/xlog"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/graphene-ci/graphene/internal/auth"
)

// OTLP is the telemetry half of the door: workers and agents export
// traces, logs, and metrics with the STANDARD OTLP gRPC protocol to
// the same address they already dial, authenticated by the same token.
// The server stamps the caller's namespace onto every resource (a
// client cannot lie about it) and forwards to the collector. No
// collector configured — accepted and dropped: telemetry must never
// fail a run.
type OTLP struct {
	coltracepb.UnimplementedTraceServiceServer
	collogspb.UnimplementedLogsServiceServer
	colmetricspb.UnimplementedMetricsServiceServer

	// Endpoint is the collector's OTLP gRPC address; empty drops.
	Endpoint string
	Log      *xlog.Logger

	mu      sync.Mutex
	conn    *grpc.ClientConn
	trace   coltracepb.TraceServiceClient
	logs    collogspb.LogsServiceClient
	metrics colmetricspb.MetricsServiceClient
	warned  bool
}

func (o *OTLP) clients() (coltracepb.TraceServiceClient, collogspb.LogsServiceClient, colmetricspb.MetricsServiceClient, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Endpoint == "" {
		if !o.warned {
			o.warned = true
			o.Log.Warn("no otel collector configured — telemetry is accepted and dropped")
		}
		return nil, nil, nil, false
	}
	if o.conn == nil {
		conn, err := grpc.NewClient(o.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			o.Log.Error("otel collector dial", xlog.Err(err))
			return nil, nil, nil, false
		}
		o.conn = conn
		o.trace = coltracepb.NewTraceServiceClient(conn)
		o.logs = collogspb.NewLogsServiceClient(conn)
		o.metrics = colmetricspb.NewMetricsServiceClient(conn)
	}
	return o.trace, o.logs, o.metrics, true
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
	trace, _, _, ok := o.clients()
	if !ok {
		return &coltracepb.ExportTraceServiceResponse{}, nil
	}
	return trace.Export(ctx, req)
}

// exportLogs forwards logs (method name collides across the three
// generated services, so the log and metric halves live on aliases).
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
	_, logs, _, ok := o.clients()
	if !ok {
		return &collogspb.ExportLogsServiceResponse{}, nil
	}
	return logs.Export(ctx, req)
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
	_, _, metrics, ok := o.clients()
	if !ok {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}
	return metrics.Export(ctx, req)
}

// Close releases the collector connection.
func (o *OTLP) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.conn != nil {
		_ = o.conn.Close()
		o.conn = nil
	}
}

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
