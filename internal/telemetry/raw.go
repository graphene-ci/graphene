package telemetry

// The RAW half of the observe surface: a query in the backend's own
// language, passed through the door. A resource's dimensions are a
// FILTERED view of the same stores; the raw view is the whole store —
// admin-only by the door, because no per-record scope can be derived
// from an arbitrary query.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RawLogs runs one LogsQL query as given.
func (l *LogsQL) RawLogs(ctx context.Context, query string, limit int) ([]LogRecord, error) {
	form := url.Values{
		"query": {query},
		"limit": {strconv.Itoa(limit)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(l.Base, "/")+"/select/logsql/query",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := l.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("logs backend: %s: %s", resp.Status, raw)
	}
	return parseLogsQLStream(resp.Body)
}

// RawMetrics runs one PromQL range query as given.
func (p *PromQL) RawMetrics(ctx context.Context, query string, start, end time.Time) (json.RawMessage, error) {
	step := max(int(end.Sub(start).Seconds())/200, 15)
	q := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {strconv.Itoa(step) + "s"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(p.Base, "/")+"/api/v1/query_range?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics backend: %s: %s", resp.Status, truncate(raw, 512))
	}
	return raw, nil
}

// RawTraces runs one Jaeger search with the given query-string
// parameters ("service=x&tags={...}&limit=20").
func (j *Jaeger) RawTraces(ctx context.Context, params string) (json.RawMessage, error) {
	return j.get(ctx, "/api/traces?"+params)
}
