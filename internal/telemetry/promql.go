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

// PromQL reads metrics from any PromQL-conforming backend
// (VictoriaMetrics, Prometheus, Mimir, ...). A SCOPED query — the
// caller's own expression inside one record — needs the backend to lay
// label filters over every selector of an arbitrary expression;
// VictoriaMetrics does that with extra_filters, and that is the one
// backend the scoped form runs on (ExtraFilters).
type PromQL struct {
	// Base is the API base URL (http://victoriametrics:8428).
	Base   string
	Client *http.Client
	// DotsToUnderscores translates attribute names to the backend's
	// label naming (classic Prometheus mangling).
	DotsToUnderscores bool
	// ExtraFilters marks a backend that honors the extra_filters[] query
	// argument (VictoriaMetrics): the scoped form's only mechanism.
	ExtraFilters bool
}

// Series returns the standard PromQL range response: every series of the
// record when q.Expr is empty, the caller's expression evaluated inside
// the record's scope otherwise.
func (p *PromQL) Series(ctx context.Context, sel Selector, q MetricsQuery) (json.RawMessage, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	// A record born inside the window starts the window: the series of an
	// earlier record of the same name carry the same labels.
	if q.Start = sel.After(q.Start); q.Start.After(q.End) {
		q.Start = q.End
	}
	if q.Expr == "" {
		return p.rangeQuery(ctx, strings.Join(p.matchers(sel), " or "), q, nil)
	}
	if !p.ExtraFilters {
		return nil, ErrScopedQueryUnsupported
	}
	// The backend ANDs each extra filter into every selector and subquery
	// of the expression, and ORs the filters with one another: one per
	// axis per label spelling, the namespace inside each.
	return p.rangeQuery(ctx, q.Expr, q, p.matchers(sel))
}

// matchers are the record's selectors — one per axis, in every label
// spelling the store may hold.
func (p *PromQL) matchers(sel Selector) []string {
	spellings := []*PromQL{p}
	if !p.DotsToUnderscores {
		// A store can contain UTF-8 labels from Graphene and normalized
		// labels from an OTLP-to-Prometheus exporter. Scope every branch to
		// the tenant.
		normalized := *p
		normalized.DotsToUnderscores = true
		spellings = append(spellings, &normalized)
	}
	var out []string
	for _, sp := range spellings {
		out = append(out, fmt.Sprintf("{%s=%q,%s=%q}", sp.label("graphene.namespace"), sel.Namespace, sp.label(sel.Attribute), sel.Value))
		if sel.AltAttribute != "" {
			out = append(out, fmt.Sprintf("{%s=%q,%s=%q}", sp.label("graphene.namespace"), sel.Namespace, sp.label(sel.AltAttribute), sel.AltValue))
		}
	}
	return out
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

// RawMetrics runs one PromQL range query as given — the raw view, the
// whole store.
func (p *PromQL) RawMetrics(ctx context.Context, query string, q MetricsQuery) (json.RawMessage, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return p.rangeQuery(ctx, query, q, nil)
}

const maxMetricsBytes = 8 << 20

func (p *PromQL) rangeQuery(ctx context.Context, query string, q MetricsQuery, extraFilters []string) (json.RawMessage, error) {
	values := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(q.Start.Unix(), 10)},
		"end":   {strconv.FormatInt(q.End.Unix(), 10)},
		"step":  {strconv.Itoa(int(q.StepOrDefault().Seconds())) + "s"},
	}
	for _, f := range extraFilters {
		values.Add("extra_filters[]", f)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(p.Base, "/")+"/api/v1/query_range?"+values.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	raw, err := readBackend("metrics", resp, maxMetricsBytes+1)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxMetricsBytes {
		return nil, &ClientError{Msg: "metrics backend response exceeds 8 MiB; narrow the time range, target an individual resource, or select fewer metrics"}
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("metrics backend returned invalid JSON")
	}
	return raw, nil
}
