package ctl

// The terminal form: `run start` without params on a TTY walks the
// pipeline's params schema (from the record's manifest) and asks field
// by field. Answers travel as strings — the schema itself coerces
// ("1h", "256") and validates on submit; compound fields take JSON.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"golang.org/x/term"
	"google.golang.org/protobuf/encoding/protojson"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// stdinIsTerminal reports whether a human is on the other end.
func stdinIsTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd()))
}

// paramsSchemaOf pulls the params schema out of a pipeline record's
// manifest; nil when there is none to ask about.
func paramsSchemaOf(manifestJSON []byte) *schemapb.Schema {
	if len(manifestJSON) == 0 {
		return nil
	}
	var m manifestpb.Manifest
	if protojson.Unmarshal(manifestJSON, &m) != nil {
		return nil
	}
	s := m.GetParamsSchema()
	if s == nil || len(s.GetFields()) == 0 {
		return nil
	}
	return s
}

// promptParams asks for every field of the schema and returns the
// collected params as JSON.
func promptParams(in io.Reader, schema *schemapb.Schema) ([]byte, error) {
	reader := bufio.NewReader(in)
	values := map[string]any{}
	fmt.Fprintln(os.Stderr, "params (an empty answer skips an optional field):")
	for _, f := range schema.GetFields() {
		hint := fieldHint(f)
		if f.GetRequired() {
			hint += ", required"
		}
		for {
			fmt.Fprintf(os.Stderr, "  %s (%s): ", f.GetName(), hint)
			line, err := reader.ReadString('\n')
			if err != nil {
				return nil, fmt.Errorf("read answer: %w", err)
			}
			line = strings.TrimSpace(line)
			if line == "" {
				if f.GetRequired() {
					fmt.Fprintln(os.Stderr, "    the field is required")
					continue
				}
				break
			}
			values[f.GetName()] = parseAnswer(line)
			break
		}
	}
	return json.Marshal(values)
}

// fieldHint names the field's kind for the prompt.
func fieldHint(f *schemapb.Schema_Field) string {
	switch {
	case f.GetString_() != nil:
		return "string"
	case f.GetBool() != nil:
		return "true/false"
	case f.GetInt32() != nil, f.GetInt64() != nil, f.GetUint32() != nil, f.GetUint64() != nil:
		return "integer"
	case f.GetFloat() != nil, f.GetDouble() != nil:
		return "number"
	case f.GetDuration() != nil:
		return "duration, e.g. 1h30m"
	case f.GetTimestamp() != nil:
		return "RFC3339 time"
	case f.GetChoice() != nil:
		return "choice"
	case f.GetBytes() != nil:
		return "base64 bytes"
	default:
		return "JSON"
	}
}

// parseAnswer keeps answers as strings (the schema coerces), except
// obvious JSON compounds.
func parseAnswer(line string) any {
	if strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[") {
		var v any
		if json.Unmarshal([]byte(line), &v) == nil {
			return v
		}
	}
	return line
}
