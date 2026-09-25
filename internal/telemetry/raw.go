package telemetry

// The RAW half of the observe surface: a query in the backend's own
// language, passed through the door over the whole store — an
// administrator's, because no per-record scope can be derived from an
// arbitrary query. RawLogs and RawMetrics live next to their scoped
// siblings in logsql.go and promql.go; the Jaeger one is here.

import (
	"context"
	"encoding/json"
)

// RawTraces runs one Jaeger search as given.
func (j *Jaeger) RawTraces(ctx context.Context, params string) (json.RawMessage, error) {
	return j.get(ctx, "/api/traces?"+params)
}
