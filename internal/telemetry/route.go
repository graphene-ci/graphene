package telemetry

// The knife between the collector and the hub: an OTLP export batch
// carries records of MANY subjects (the correlation attributes live on
// individual log records, data points and spans), while a follow
// stream wants exactly one. Routing cuts a batch into envelopes of one
// subject each — WITHOUT re-describing the payload: what comes out is
// still the standard OTLP message, records of one subject, resource
// attributes intact.

import (
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// routingKeys are the correlation attributes an envelope is matched
// by — the same names every reader queries.
var routingKeys = []string{"graphene.entity", "graphene.run", "graphene.agent"}

// keysOf extracts the routing attributes from OTLP key-value lists,
// later lists overriding earlier (record over resource).
func keysOf(lists ...[]*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, list := range lists {
		for _, kv := range list {
			for _, want := range routingKeys {
				if kv.GetKey() == want {
					out[want] = kv.GetValue().GetStringValue()
				}
			}
		}
	}
	return out
}

// subjectOf renders a grouping key.
func subjectOf(keys map[string]string) string {
	return keys["graphene.entity"] + "\x00" + keys["graphene.run"] + "\x00" + keys["graphene.agent"]
}

// RouteLogs cuts a log export into per-subject envelopes.
func RouteLogs(namespace string, req *collogspb.ExportLogsServiceRequest) []Envelope {
	var out []Envelope
	for _, rl := range req.GetResourceLogs() {
		resKeys := rl.GetResource().GetAttributes()
		for _, sl := range rl.GetScopeLogs() {
			groups := map[string][]*logspb.LogRecord{}
			keys := map[string]map[string]string{}
			for _, rec := range sl.GetLogRecords() {
				k := keysOf(resKeys, rec.GetAttributes())
				sig := subjectOf(k)
				groups[sig] = append(groups[sig], rec)
				keys[sig] = k
			}
			for sig, recs := range groups {
				out = append(out, Envelope{
					Type: "log", Namespace: namespace, Keys: keys[sig],
					Payload: &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
						Resource:  rl.GetResource(),
						ScopeLogs: []*logspb.ScopeLogs{{Scope: sl.GetScope(), LogRecords: recs}},
					}}},
				})
			}
		}
	}
	return out
}

// RouteSpans cuts a trace export into per-subject envelopes.
func RouteSpans(namespace string, req *coltracepb.ExportTraceServiceRequest) []Envelope {
	var out []Envelope
	for _, rs := range req.GetResourceSpans() {
		resKeys := rs.GetResource().GetAttributes()
		for _, ss := range rs.GetScopeSpans() {
			groups := map[string][]*tracepb.Span{}
			keys := map[string]map[string]string{}
			for _, span := range ss.GetSpans() {
				k := keysOf(resKeys, span.GetAttributes())
				sig := subjectOf(k)
				groups[sig] = append(groups[sig], span)
				keys[sig] = k
			}
			for sig, spans := range groups {
				out = append(out, Envelope{
					Type: "span", Namespace: namespace, Keys: keys[sig],
					Payload: &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
						Resource:   rs.GetResource(),
						ScopeSpans: []*tracepb.ScopeSpans{{Scope: ss.GetScope(), Spans: spans}},
					}}},
				})
			}
		}
	}
	return out
}

// RouteMetrics cuts a metric export into per-subject envelopes. Number
// points (our Count and Measure produce nothing else) are cut point by
// point; the exotic shapes (histogram, summary) travel whole under the
// subject of their first point — cutting a bucket set in half would
// lie harder than a coarse address does.
func RouteMetrics(namespace string, req *colmetricspb.ExportMetricsServiceRequest) []Envelope {
	var out []Envelope
	for _, rm := range req.GetResourceMetrics() {
		resKeys := rm.GetResource().GetAttributes()
		for _, sm := range rm.GetScopeMetrics() {
			groups := map[string][]*metricspb.Metric{}
			keys := map[string]map[string]string{}
			add := func(k map[string]string, m *metricspb.Metric) {
				sig := subjectOf(k)
				groups[sig] = append(groups[sig], m)
				keys[sig] = k
			}
			for _, m := range sm.GetMetrics() {
				switch data := m.GetData().(type) {
				case *metricspb.Metric_Sum:
					for sig, pts := range splitNumber(resKeys, data.Sum.GetDataPoints()) {
						clone := &metricspb.Metric{Name: m.GetName(), Description: m.GetDescription(), Unit: m.GetUnit(),
							Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
								AggregationTemporality: data.Sum.GetAggregationTemporality(),
								IsMonotonic:            data.Sum.GetIsMonotonic(),
								DataPoints:             pts.points,
							}}}
						add(pts.keys, clone)
						_ = sig
					}
				case *metricspb.Metric_Gauge:
					for sig, pts := range splitNumber(resKeys, data.Gauge.GetDataPoints()) {
						clone := &metricspb.Metric{Name: m.GetName(), Description: m.GetDescription(), Unit: m.GetUnit(),
							Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: pts.points}}}
						add(pts.keys, clone)
						_ = sig
					}
				case *metricspb.Metric_Histogram:
					k := keysOf(resKeys)
					if dps := data.Histogram.GetDataPoints(); len(dps) > 0 {
						k = keysOf(resKeys, dps[0].GetAttributes())
					}
					add(k, m)
				default:
					add(keysOf(resKeys), m)
				}
			}
			for sig, metrics := range groups {
				out = append(out, Envelope{
					Type: "metric", Namespace: namespace, Keys: keys[sig],
					Payload: &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
						Resource:     rm.GetResource(),
						ScopeMetrics: []*metricspb.ScopeMetrics{{Scope: sm.GetScope(), Metrics: metrics}},
					}}},
				})
			}
		}
	}
	return out
}

type numberGroup struct {
	keys   map[string]string
	points []*metricspb.NumberDataPoint
}

// splitNumber groups number points by subject.
func splitNumber(resKeys []*commonpb.KeyValue, points []*metricspb.NumberDataPoint) map[string]numberGroup {
	out := map[string]numberGroup{}
	for _, p := range points {
		k := keysOf(resKeys, p.GetAttributes())
		sig := subjectOf(k)
		g := out[sig]
		g.keys = k
		g.points = append(g.points, p)
		out[sig] = g
	}
	return out
}
