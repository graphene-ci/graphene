// Package telemetry is the READ side of the telemetry plane:
// dimensions 3-5 of the observe surface, queried from the backends by
// the graphene correlation attributes. Metrics and traces speak
// STANDARD surfaces (PromQL, the Jaeger API) — those drivers work
// against any conforming backend; logs have no de-facto standard, so
// that one access is isolated behind the Logs interface (LogsQL driver
// today).
package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Selector correlates signals to one entity: the namespace always, and
// the attribute the ref maps to (graphene.run for runs, graphene.agent
// for agents, graphene.entity for everything else).
type Selector struct {
	Namespace string
	// Attribute/Value is the ref's correlation pair.
	Attribute string
	Value     string
	// AltAttribute names the SECOND axis of the same question, ORed
	// with the first; empty means one axis is enough.
	AltAttribute string
	AltValue     string
	// Since is when THIS record was born. A ref is a name, and names are
	// reused: run after run declares agent/db-1, docker/pg. Each is a new
	// record, and the signals of the previous bearer of the name are not
	// its history. Zero means unknown — no bound.
	Since time.Time
}

// After is the later of the caller's own lower bound and the record's
// birth: nothing older than the record belongs to it.
func (s Selector) After(since time.Time) time.Time {
	if s.Since.After(since) {
		return s.Since
	}
	return since
}

// SelectorFor maps an entity ref to its correlation attribute.
func SelectorFor(namespace, ref string) Selector {
	s := Selector{Namespace: namespace, Attribute: "graphene.entity", Value: ref}
	// A run's and an agent's OWN signals carry the context attributes
	// (the emitter's identity); the operational signals ABOUT the same
	// record carry the subject attribute. One question, two axes — the
	// selector names both and the drivers OR them.
	if id, ok := strings.CutPrefix(ref, "run/"); ok {
		s.Attribute, s.Value = "graphene.run", id
		s.AltAttribute, s.AltValue = "graphene.entity", ref
	} else if id, ok := strings.CutPrefix(ref, "agent/"); ok {
		s.Attribute, s.Value = "graphene.agent", id
		s.AltAttribute, s.AltValue = "graphene.entity", ref
	}
	return s
}

// LogRecord is one log line of an entity.
type LogRecord struct {
	Time       time.Time
	Severity   string
	Body       string
	Attributes map[string]string
}

// Limits of a log selection.
const (
	DefaultLogLimit   = 1000
	MaxLogLimit       = 10000
	DefaultFacetLimit = 50
)

// LogQuery is one selection of a record's logs: the bounds, the page, the
// filters — and Filter, the caller's own LogsQL, ANDed INSIDE the record's
// scope so no expression escapes it.
type LogQuery struct {
	Since, Until time.Time
	// Limit caps one page; zero means DefaultLogLimit.
	Limit int
	// Desc orders newest first.
	Desc bool
	// Cursor continues a page: the time of the last record delivered and
	// how many records with that very time were delivered already.
	Cursor LogCursor
	// Filter is a LogsQL filter in the backend's language.
	Filter string
	// Severities keeps records of these severities (INFO, WARN, ...).
	Severities []string
	// Attributes keeps records whose attribute equals the value (stream,
	// graphene.agent, graphene.entity, a job's name).
	Attributes map[string]string
	// Text keeps records whose body contains the phrase.
	Text string
}

// Admits tells whether a record that arrived LIVE belongs to this
// selection: the same severities, attributes, text and upper bound the
// backend applied to the history. Filter — the backend's language — has
// no evaluator here; a follow with a Filter is refused at the door.
func (q LogQuery) Admits(rec LogRecord) bool {
	if !q.Until.IsZero() && !rec.Time.Before(q.Until) {
		return false
	}
	if len(q.Severities) > 0 && !slices.ContainsFunc(q.Severities, func(s string) bool { return strings.EqualFold(s, rec.Severity) }) {
		return false
	}
	for k, v := range q.Attributes {
		if rec.Attributes[k] != v {
			return false
		}
	}
	if q.Text != "" && !strings.Contains(strings.ToLower(rec.Body), strings.ToLower(q.Text)) {
		return false
	}
	return true
}

// LogCursor is the position of a page: records are ordered by (time,
// stream, body), so the time of the last delivered record and the count
// of records carrying that time name the next one exactly — equal
// timestamps are not lost.
type LogCursor struct {
	Time time.Time
	Skip int
}

// IsZero reports an unset cursor.
func (c LogCursor) IsZero() bool { return c.Time.IsZero() }

// LogPage is one page of a selection.
type LogPage struct {
	Records []LogRecord
	// Truncated: the selection had more than Limit; Next continues it.
	Truncated bool
	Next      LogCursor
}

// Facet is the values one field takes within a selection, with counts.
type Facet struct {
	Field  string
	Values []FacetValue
}

// FacetValue is one value and how many records carry it.
type FacetValue struct {
	Value string
	Hits  int64
}

// Logs reads dimension 3.
type Logs interface {
	// Query returns one page of the selection.
	Query(ctx context.Context, sel Selector, q LogQuery) (LogPage, error)
	// Facets counts the values of fields within the selection.
	Facets(ctx context.Context, sel Selector, q LogQuery, fields []string, limit int) ([]Facet, error)
}

// Metric range limits.
const (
	// MaxMetricPoints bounds one range query per series — Prometheus's own
	// ceiling for query_range.
	MaxMetricPoints = 11000
	MinMetricStep   = time.Second
	// defaultStepFloor is the coarsest default: range/200 but never finer.
	defaultStepFloor = 15 * time.Second
)

// MetricsQuery is one range read: the window, the resolution, and Expr —
// the caller's own PromQL evaluated inside the record's scope; empty Expr
// reads every series of the record.
type MetricsQuery struct {
	Start, End time.Time
	// Step is the resolution; zero lets the backend pick (range/200, at
	// least 15s).
	Step time.Duration
	Expr string
}

// StepOrDefault is the step a query runs with.
func (q MetricsQuery) StepOrDefault() time.Duration {
	if q.Step > 0 {
		return q.Step
	}
	return max(q.End.Sub(q.Start)/200, defaultStepFloor).Truncate(time.Second)
}

// Validate refuses a resolution the backend would refuse.
func (q MetricsQuery) Validate() error {
	if q.Step == 0 {
		return nil
	}
	if q.Step < MinMetricStep {
		return &ClientError{Msg: fmt.Sprintf("step %s is below %s", q.Step, MinMetricStep)}
	}
	if points := q.End.Sub(q.Start) / q.Step; points > MaxMetricPoints {
		return &ClientError{Msg: fmt.Sprintf("step %s over %s makes %d points per series; the limit is %d — widen the step or narrow the range", q.Step, q.End.Sub(q.Start).Truncate(time.Second), points, MaxMetricPoints)}
	}
	return nil
}

// Metrics reads dimension 4; the result is the backend's standard JSON
// (a PromQL range response).
type Metrics interface {
	Series(ctx context.Context, sel Selector, q MetricsQuery) (json.RawMessage, error)
}

// Traces reads dimension 5; the result is standard Jaeger JSON. Params
// are the caller's own Jaeger search parameters, evaluated inside the
// record's scope; empty reads the record's traces.
type Traces interface {
	Search(ctx context.Context, sel Selector, params string, limit int) (json.RawMessage, error)
}

// ClientError is a request the backend or the door refuses as malformed —
// the caller's fault, InvalidArgument at the door.
type ClientError struct{ Msg string }

func (e *ClientError) Error() string { return e.Msg }

// BackendError is a non-2xx answer of a telemetry backend. A 4xx is the
// query's fault (InvalidArgument), anything else the backend's
// (Unavailable) — the door tells them apart by this.
type BackendError struct {
	Backend string
	Status  int
	Body    string
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("%s backend: %d: %s", e.Backend, e.Status, strings.TrimSpace(e.Body))
}

// ClientFault reports whether the backend blamed the query.
func (e *BackendError) ClientFault() bool { return e.Status >= 400 && e.Status < 500 }

// ErrScopedQueryUnsupported is a backend that cannot lay a scope over an
// arbitrary expression of its language.
var ErrScopedQueryUnsupported = errors.New("this backend cannot scope an arbitrary query to one record")

// readBackend reads a backend reply and turns a non-2xx status into a
// BackendError.
func readBackend(backend string, resp *http.Response, limit int64) ([]byte, error) {
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, &BackendError{Backend: backend, Status: resp.StatusCode, Body: string(raw)}
	}
	return io.ReadAll(io.LimitReader(resp.Body, limit))
}
