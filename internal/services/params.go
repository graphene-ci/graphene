package services

// Submit-time validation of run params against the pipeline's published
// manifest: an invalid submit fails at the door, not on a machine.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/pipeline/pkg/manifest"
	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"

	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/pipeline/pkg/id"
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
	return manifest.NormalizeParams(manifestJSON, params)
}

// substituteVars replaces whole-string "${var:name}" values with the
// namespace's variables, walking nested objects and lists. Runs BEFORE
// schema validation, so validation (and the workflow) see final
// values; a missing variable fails the submit at the door.
func substituteVars(params []byte, vars secrets.Store) ([]byte, error) {
	if len(params) == 0 || !bytes.Contains(params, []byte("${var:")) {
		return params, nil
	}
	var v any
	if err := json.Unmarshal(params, &v); err != nil {
		return params, nil // not an object — validation reports it
	}
	var missing []string
	var walk func(any) any
	walk = func(x any) any {
		switch t := x.(type) {
		case map[string]any:
			for k, val := range t {
				t[k] = walk(val)
			}
			return t
		case []any:
			for i, val := range t {
				t[i] = walk(val)
			}
			return t
		case string:
			if strings.HasPrefix(t, "${var:") && strings.HasSuffix(t, "}") {
				name := t[len("${var:") : len(t)-1]
				val, err := vars.Get(id.SecretId(name))
				if err != nil {
					missing = append(missing, name)
					return t
				}
				return val
			}
			return t
		default:
			return x
		}
	}
	v = walk(v)
	if len(missing) > 0 {
		return nil, fmt.Errorf("variables not configured: %s", strings.Join(missing, ", "))
	}
	out, err := json.Marshal(v)
	if err != nil {
		return params, nil
	}
	return out, nil
}

// checkSecretRefs verifies every secret-marked params field (a
// SecretRef in the pipeline's Go type) names an EXISTING secret.
func checkSecretRefs(manifestJSON json.RawMessage, params []byte, store secrets.Store) error {
	var m manifestpb.Manifest
	if protojson.Unmarshal(manifestJSON, &m) != nil {
		return nil
	}
	schema := m.GetParamsSchema()
	if schema == nil || len(params) == 0 {
		return nil
	}
	values := map[string]any{}
	if json.Unmarshal(params, &values) != nil {
		return nil
	}
	return checkSecretFields(schema, values, store)
}

func checkSecretFields(s *schemapb.Schema, values map[string]any, store secrets.Store) error {
	for _, f := range s.GetFields() {
		v, ok := values[f.GetName()]
		if !ok {
			continue
		}
		if f.GetSecret() {
			name, _ := v.(string)
			if name == "" {
				continue // presence is the schema's business
			}
			if _, err := store.Get(id.SecretId(name)); err != nil {
				return fmt.Errorf("params field %q: secret %q is not configured", f.GetName(), name)
			}
			continue
		}
		if f.GetObject() != nil {
			if mm, ok := v.(map[string]any); ok {
				if err := checkSecretFields(f.GetObject().GetSchema(), mm, store); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
