package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// Search returns standard Jaeger JSON: the traces of every service
// that carry the selector's attributes. Correlation attributes live in
// the RESOURCE (Jaeger: process tags), and tag search implementations
// differ on whether process tags participate — so the filter runs
// here, over the standard response shape, and works everywhere.
func (j *Jaeger) Search(ctx context.Context, sel Selector, limit int) (json.RawMessage, error) {
	services, err := j.services(ctx)
	if err != nil {
		return nil, err
	}
	// The SUBJECT filters server-side: the correlation attribute lives
	// on the spans, and the backend indexes span tags — fetching
	// unfiltered traces and sieving them here found only whatever
	// happened to be recent. The namespace stays a post-filter: it is a
	// resource attribute (a process tag in Jaeger terms), stamped by
	// the door, and belongs to a different half of the trace.
	tags, err := json.Marshal(map[string]string{sel.Attribute: sel.Value})
	if err != nil {
		return nil, err
	}
	merged := []json.RawMessage{}
	for _, service := range services {
		q := url.Values{
			"service": {service},
			"tags":    {string(tags)},
			"limit":   {strconv.Itoa(limit)},
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
			if traceInNamespace(trace, sel.Namespace) && len(merged) < limit {
				merged = append(merged, trace)
			}
		}
	}
	out, err := json.Marshal(map[string]any{"data": merged})
	if err != nil {
		return nil, err
	}
	return out, nil
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(j.Base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := j.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("traces backend: %s: %s", resp.Status, truncate(raw, 512))
	}
	return raw, nil
}
