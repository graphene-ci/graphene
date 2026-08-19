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
}

// SelectorFor maps an entity ref to its correlation attribute.
func SelectorFor(namespace, ref string) Selector {
	s := Selector{Namespace: namespace, Attribute: "graphene.entity", Value: ref}
	if id, ok := strings.CutPrefix(ref, "run/"); ok {
		s.Attribute, s.Value = "graphene.run", id
	} else if id, ok := strings.CutPrefix(ref, "agent/"); ok {
		s.Attribute, s.Value = "graphene.agent", id
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

// Logs reads dimension 3.
type Logs interface {
	// Query returns records since the given time, oldest first.
	Query(ctx context.Context, sel Selector, since time.Time, limit int) ([]LogRecord, error)
}

// Metrics reads dimension 4; the result is the backend's standard JSON
// (a PromQL range response).
type Metrics interface {
	Series(ctx context.Context, sel Selector, start, end time.Time) (json.RawMessage, error)
}

// Traces reads dimension 5; the result is standard Jaeger JSON.
type Traces interface {
	Search(ctx context.Context, sel Selector, limit int) (json.RawMessage, error)
}
