package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Jaeger reads traces through the standard Jaeger query API
// (VictoriaTraces, Jaeger, Grafana Tempo).
type Jaeger struct {
	// Base is the Jaeger API base URL, up to but not including
	// /api (http://victoriatraces:10428/select/jaeger).
	Base   string
	Client *http.Client
}

// Search returns standard Jaeger JSON: the traces of the record. Params
// are the caller's own search parameters (service, operation, tags,
// minDuration, ...) evaluated inside the record's scope: the scope's tags
// win over the caller's, the namespace is a post-filter, and a service the
// caller names is the only one asked. Correlation attributes live in the
// RESOURCE (Jaeger: process tags), and tag search implementations differ
// on whether process tags participate — so the namespace filter runs
// here, over the standard response shape, and works everywhere.
func (j *Jaeger) Search(ctx context.Context, sel Selector, params string, limit int) (json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	caller, err := url.ParseQuery(params)
	if err != nil {
		return nil, &ClientError{Msg: "trace query: " + err.Error()}
	}
	callerTags := map[string]string{}
	if raw := caller.Get("tags"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &callerTags); err != nil {
			return nil, &ClientError{Msg: "trace query: tags must be a JSON object: " + err.Error()}
		}
	}
	services, err := j.services(ctx)
	if err != nil {
		return nil, err
	}
	if want := caller.Get("service"); want != "" {
		services = []string{want}
	}
	// The SUBJECT filters server-side: the correlation attribute lives on
	// the spans, and the backend indexes span tags.
	axes := []map[string]string{{sel.Attribute: sel.Value}}
	if sel.AltAttribute != "" {
		axes = append(axes, map[string]string{sel.AltAttribute: sel.AltValue})
	}
	merged := []json.RawMessage{}
	seen := map[string]bool{}
	for _, service := range services {
		for _, axis := range axes {
			tags := map[string]string{}
			for k, v := range callerTags {
				if isCorrelationKey(k) {
					continue // the scope owns the correlation tags
				}
				tags[k] = v
			}
			for k, v := range axis {
				tags[k] = v
			}
			encoded, err := json.Marshal(tags)
			if err != nil {
				return nil, err
			}
			q := url.Values{}
			for k, vs := range caller {
				if k == "service" || k == "tags" || k == "limit" || k == "start" {
					continue
				}
				q[k] = vs
			}
			q.Set("service", service)
			q.Set("tags", string(encoded))
			q.Set("limit", strconv.Itoa(limit))
			if !sel.Since.IsZero() {
				// Jaeger's bounds are microseconds since the epoch; the
				// record's birth is the floor of any start the caller gave.
				start := sel.Since.UnixMicro()
				if s, err := strconv.ParseInt(caller.Get("start"), 10, 64); err == nil && s > start {
					start = s
				}
				q.Set("start", strconv.FormatInt(start, 10))
			} else if s := caller.Get("start"); s != "" {
				q.Set("start", s)
			}
			raw, err := j.get(ctx, "/api/traces?"+q.Encode())
			if err != nil {
				return nil, err
			}
			var page struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(raw, &page); err != nil {
				return nil, err
			}
			for _, trace := range page.Data {
				key := traceKey(trace)
				if seen[key] {
					continue
				}
				if traceInNamespace(trace, sel.Namespace) && len(merged) < limit {
					seen[key] = true
					merged = append(merged, trace)
				}
			}
		}
	}
	out, err := json.Marshal(map[string]any{"data": merged})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// isCorrelationKey names the tags the door stamps and the scope is made
// of; a caller's own value for one is dropped, not merged.
func isCorrelationKey(key string) bool {
	switch key {
	case "graphene.namespace", "graphene.run", "graphene.agent", "graphene.entity":
		return true
	}
	return false
}

// traceKey identifies a trace for axis-merge dedup.
func traceKey(trace json.RawMessage) string {
	var t struct {
		TraceID string `json:"traceID"`
	}
	_ = json.Unmarshal(trace, &t)
	return t.TraceID
}

// traceInNamespace reports whether any process of the trace carries
// the namespace stamp.
func traceInNamespace(trace json.RawMessage, namespace string) bool {
	if namespace == "" {
		return true
	}
	var t struct {
		Processes map[string]struct {
			Tags []struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			} `json:"tags"`
		} `json:"processes"`
	}
	if json.Unmarshal(trace, &t) != nil {
		return false
	}
	for _, proc := range t.Processes {
		for _, tag := range proc.Tags {
			if tag.Key == "graphene.namespace" && fmt.Sprint(tag.Value) == namespace {
				return true
			}
		}
	}
	return false
}

func (j *Jaeger) services(ctx context.Context) ([]string, error) {
	raw, err := j.get(ctx, "/api/services")
	if err != nil {
		return nil, err
	}
	var out struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (j *Jaeger) get(ctx context.Context, path string) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(j.Base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := j.Client.Do(req)
	if err != nil {
		return nil, err
	}
	raw, err := readBackend("traces", resp, 32<<20)
	if err != nil {
		return nil, err
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("traces backend returned invalid JSON")
	}
	return raw, nil
}
