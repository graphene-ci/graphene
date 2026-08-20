package services

// Submit-time validation of run params against the pipeline's published
// manifest: an invalid submit fails at the door, not on a machine.

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// validateParams checks the params JSON against the manifest's params
// schema and returns the params to hand the workflow. String inputs
// ("1h", "256") are coerced by the schema itself (manifest schemas are
// built with coercion on); the one form schemapb cannot know is the Go
// wire form of durations — nanosecond numbers — so those are converted
// here before validation. Best-effort by design: an absent or
// unreadable manifest never blocks a run — only a schema violation
// does.
func validateParams(manifestJSON json.RawMessage, params []byte) ([]byte, error) {
	var m manifestpb.Manifest
	if protojson.Unmarshal(manifestJSON, &m) != nil {
		return params, nil
	}
	schema := m.GetParamsSchema()
	if schema == nil {
		return params, nil
	}
	values := map[string]any{}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &values); err != nil {
			return nil, fmt.Errorf("params are not a JSON object: %w", err)
		}
	}
	numericDurations(schema, values)
	_, res, err := schema.Bake(values)
	if err != nil {
		// A schema that does not compile is the record's problem, not
		// the caller's — do not block the run on it.
		return params, nil
	}
	if res != nil && res.Blocking() {
		parts := make([]string, 0, len(res.GetErrors()))
		for _, e := range res.GetErrors() {
			part := e.GetCode().String()
			if e.GetPath() != "" {
				part = e.GetPath() + ": " + part
			}
			if e.GetConstraint() != "" {
				part += " (" + e.GetConstraint() + ")"
			}
			parts = append(parts, part)
		}
		return nil, fmt.Errorf("params do not match the pipeline's manifest: %s", strings.Join(parts, "; "))
	}
	// Bake resolved values in place (coerced strings included); the Go
	// workflow side reads durations as nanosecond numbers, which is
	// exactly how time.Duration marshals.
	normalized, err := json.Marshal(values)
	if err != nil {
		return params, nil
	}
	return normalized, nil
}

// numericDurations rewrites nanosecond numbers on duration-typed fields
// into time.Duration, walking nested objects, lists, and maps. The Go
// binary's own flags produce this form; schemapb accepts native
// durations and parseable strings, never bare numbers.
func numericDurations(s *schemapb.Schema, values map[string]any) {
	if s == nil {
		return
	}
	for _, f := range s.GetFields() {
		v, ok := values[f.GetName()]
		if !ok {
			continue
		}
		values[f.GetName()] = numericValue(f, v)
	}
}

func numericValue(f *schemapb.Schema_Field, v any) any {
	switch {
	case f.GetDuration() != nil:
		if n, ok := v.(float64); ok {
			return time.Duration(int64(n))
		}
	case f.GetObject() != nil:
		if m, ok := v.(map[string]any); ok {
			numericDurations(f.GetObject().GetSchema(), m)
		}
	case f.GetList() != nil:
		items := f.GetList().GetItems()
		if list, ok := v.([]any); ok && len(items) == 1 {
			for i, item := range list {
				list[i] = numericValue(items[0], item)
			}
		}
	case f.GetMap() != nil:
		if m, ok := v.(map[string]any); ok {
			for _, mv := range m {
				if mm, ok := mv.(map[string]any); ok {
					numericDurations(f.GetMap().GetValueSchema(), mm)
				}
			}
		}
	}
	return v
}
