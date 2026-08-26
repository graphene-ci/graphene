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
	"time"
)

// PromQL reads metrics from any PromQL-conforming backend
// (VictoriaMetrics, Prometheus, Mimir, ...).
type PromQL struct {
	// Base is the API base URL (http://victoriametrics:8428).
	Base   string
	Client *http.Client
	// DotsToUnderscores translates attribute names to the backend's
	// label naming (classic Prometheus mangling).
	DotsToUnderscores bool
}

// Series returns the standard PromQL range response for every series
// carrying the selector's attributes.
func (p *PromQL) Series(ctx context.Context, sel Selector, start, end time.Time) (json.RawMessage, error) {
	matcher := fmt.Sprintf("{%s=%q,%s=%q}",
		p.label("graphene.namespace"), sel.Namespace,
		p.label(sel.Attribute), sel.Value)
	if sel.AltAttribute != "" {
		matcher = fmt.Sprintf("%s or {%s=%q,%s=%q}", matcher,
			p.label("graphene.namespace"), sel.Namespace,
			p.label(sel.AltAttribute), sel.AltValue)
	}
	step := max(int(end.Sub(start).Seconds())/200, 15)
	q := url.Values{
		"query": {matcher},
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

func (p *PromQL) label(attr string) string {
	if p.DotsToUnderscores {
		return strings.ReplaceAll(attr, ".", "_")
	}
	return quoteIfNeeded(attr)
}

// quoteIfNeeded renders a UTF-8 label name for PromQL (Prometheus 3
// syntax: {"a.b"="v"}).
func quoteIfNeeded(name string) string {
	if strings.ContainsAny(name, ".-/") {
		return strconv.Quote(name)
	}
	return name
}

func truncate(raw []byte, n int) string {
	if len(raw) <= n {
		return string(raw)
	}
	return string(raw[:n]) + "…"
}
