package telemetry

// Logs are the one signal we render into our own wire message even
// when live: a log line converts from OTLP without loss (time,
// severity, body, attributes — that is the whole record), and one
// format for history and live keeps every log consumer trivial.
// Metrics and traces do NOT get this treatment: their OTLP shapes
// (buckets, exemplars, links) would lose in translation, so they
// travel as raw OTLP chunks.

import (
	"fmt"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// LogRecordsFrom renders a routed log envelope's records.
func LogRecordsFrom(env Envelope) []LogRecord {
	req, ok := env.Payload.(*collogspb.ExportLogsServiceRequest)
	if !ok {
		return nil
	}
	var out []LogRecord
	for _, rl := range req.GetResourceLogs() {
		resAttrs := rl.GetResource().GetAttributes()
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				ts := rec.GetTimeUnixNano()
				if ts == 0 {
					ts = rec.GetObservedTimeUnixNano()
				}
				attrs := map[string]string{}
				for _, kv := range resAttrs {
					attrs[kv.GetKey()] = renderValue(kv.GetValue())
				}
				for _, kv := range rec.GetAttributes() {
					attrs[kv.GetKey()] = renderValue(kv.GetValue())
				}
				out = append(out, LogRecord{
					Time:       time.Unix(0, int64(ts)), //nolint:gosec // otel nanos
					Severity:   rec.GetSeverityText(),
					Body:       renderValue(rec.GetBody()),
					Attributes: attrs,
				})
			}
		}
	}
	return out
}

// renderValue renders an OTLP AnyValue the way a log reader wants it.
func renderValue(v *commonpb.AnyValue) string {
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", val.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", val.DoubleValue)
	case nil:
		return ""
	default:
		return v.String()
	}
}
