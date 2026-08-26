package cmdutil

// The terminal form: a command that needs a typed payload and got
// none, on a TTY, walks the SCHEMA the installation published and asks
// field by field. The schema is never spelled out in this client — it
// comes from the record's own kind — so a new field appears in the
// prompt without the client changing.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"
)

// ParseSchema reads a schema as the door publishes it (protojson).
func ParseSchema(raw []byte) *schemapb.Schema {
	if len(raw) == 0 {
		return nil
	}
	var s schemapb.Schema
	if protojson.Unmarshal(raw, &s) != nil {
		return nil
	}
	if len(s.GetFields()) == 0 {
		return nil
	}
	return &s
}

// PromptSchema asks for every field of the schema and returns the
// collected values as JSON.
func PromptSchema(in io.Reader, title string, schema *schemapb.Schema) ([]byte, error) {
	reader := bufio.NewReader(in)
	values := map[string]any{}
	fmt.Fprintln(os.Stderr, title+" (an empty answer skips an optional field):")
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
