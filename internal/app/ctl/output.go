package ctl

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// Format selects how resources are rendered.
type Format string

const (
	// FormatYAML is the canonical exchange form: what it prints, apply reads.
	FormatYAML Format = "yaml"
	// FormatJSON is the same content for machines that prefer json.
	FormatJSON Format = "json"
	// FormatName prints addresses only — the form other commands take.
	FormatName Format = "name"
	// FormatTable is columns: the path and the state.
	FormatTable Format = "table"
	// FormatWide is the table plus the spec.
	FormatWide Format = "wide"
)

// NeedsDefinition reports whether rendering this format requires the
// kind's definition (the column names come from the schema).
func (f Format) NeedsDefinition() bool {
	return f == FormatTable || f == FormatWide
}

// ErrUnknownFormat — an output format nobody implements.
var ErrUnknownFormat = errors.New("ctl: unknown output format")

// ParseFormat validates an -o value.
func ParseFormat(value string) (Format, error) {
	switch Format(strings.ToLower(value)) {
	case FormatYAML, "":
		return FormatYAML, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatName:
		return FormatName, nil
	case FormatTable:
		return FormatTable, nil
	case FormatWide:
		return FormatWide, nil
	default:
		return "", fmt.Errorf("%w: %q (want yaml, json, name, table or wide)", ErrUnknownFormat, value)
	}
}

// Write renders resources in the chosen format. def is needed only by the
// column formats and may be nil otherwise.
func Write(
	out io.Writer,
	format Format,
	def *graphenepbv1.ResourceDefinition,
	resources []*graphenepbv1.Resource,
) error {
	switch format {
	case FormatTable:
		return WriteTable(out, def, resources, false)
	case FormatWide:
		return WriteTable(out, def, resources, true)
	case FormatName:
		return writeNames(out, resources)
	case FormatJSON:
		return writeJSON(out, resources)
	case FormatYAML:
		return WriteResources(out, resources)
	default:
		return fmt.Errorf("%w: %q", ErrUnknownFormat, format)
	}
}

func writeNames(out io.Writer, resources []*graphenepbv1.Resource) error {
	for _, res := range resources {
		line := res.GetKey().GetKind() + " " + strings.Join(res.GetKey().GetPath(), "/") + "\n"
		if _, err := io.WriteString(out, line); err != nil {
			return fmt.Errorf("ctl: write: %w", err)
		}
	}

	return nil
}

func writeJSON(out io.Writer, resources []*graphenepbv1.Resource) error {
	marshal := protojson.MarshalOptions{Multiline: true, Indent: "  "}

	for _, res := range resources {
		raw, err := marshal.Marshal(res)
		if err != nil {
			return fmt.Errorf("ctl: encode json: %w", err)
		}

		if _, err := out.Write(append(raw, '\n')); err != nil {
			return fmt.Errorf("ctl: write: %w", err)
		}
	}

	return nil
}
