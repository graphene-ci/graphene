package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
	matcher := p.selectorMatcher(sel)
	if !p.DotsToUnderscores {
		// A store can contain UTF-8 labels from Graphene and normalized labels
		// from an OTLP-to-Prometheus exporter. Scope every branch to the tenant.
		normalized := *p
		normalized.DotsToUnderscores = true
		matcher += " or " + normalized.selectorMatcher(sel)
	}
	return p.RawMetrics(ctx, matcher, start, end)
}

func (p *PromQL) selectorMatcher(sel Selector) string {
	matcher := fmt.Sprintf("{%s=%q,%s=%q}",
		p.label("graphene.namespace"), sel.Namespace,
		p.label(sel.Attribute), sel.Value)
	if sel.AltAttribute != "" {
		matcher = fmt.Sprintf("%s or {%s=%q,%s=%q}", matcher,
			p.label("graphene.namespace"), sel.Namespace,
			p.label(sel.AltAttribute), sel.AltValue)
	}
	return matcher
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
