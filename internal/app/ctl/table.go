package ctl

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// Table rendering answers the question a list is usually asked: what is
// there, and what state is it in. The columns are not hard-coded per kind
// the way kubectl needs them to be — the kind's definition names its path
// segments and its fields, so the table builds itself from the schema.
//
//	table — path columns plus the status (what is going on)
//	wide  — plus the spec (what was asked for)
const (
	tabMinWidth = 0
	tabWidth    = 8
	tabPadding  = 2
	emptyCell   = "-"
	// nestedCell marks a value a table cannot show honestly.
	nestedCell = "…"
)

// WriteTable renders resources as columns. def may be nil: without a
// definition the path columns fall back to positional names.
func WriteTable(
	out io.Writer,
	def *graphenepbv1.ResourceDefinition,
	resources []*graphenepbv1.Resource,
	wide bool,
) error {
	if len(resources) == 0 {
		return nil
	}

	specFields := fieldNames(def.GetSpecSchema())
	statusFields := fieldNames(def.GetStatusSchema())

	if !wide {
		specFields = nil
	}

	writer := tabwriter.NewWriter(out, tabMinWidth, tabWidth, tabPadding, ' ', 0)

	header := append(pathHeader(def, resources[0]), "REVISION")
	header = append(header, upper(statusFields)...)
	header = append(header, upper(specFields)...)

	if _, err := fmt.Fprintln(writer, strings.Join(header, "\t")); err != nil {
		return fmt.Errorf("ctl: write: %w", err)
	}

	for _, res := range resources {
		row := append([]string{}, res.GetKey().GetPath()...)
		row = append(row, strconv.FormatUint(res.GetRevision(), 10))
		row = append(row, values(res.GetStatus(), statusFields)...)
		row = append(row, values(res.GetSpec(), specFields)...)

		if _, err := fmt.Fprintln(writer, strings.Join(row, "\t")); err != nil {
			return fmt.Errorf("ctl: write: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("ctl: write: %w", err)
	}

	return nil
}

// pathHeader names the path columns from the definition, falling back to
// positional names when the kind is unknown to the caller.
func pathHeader(def *graphenepbv1.ResourceDefinition, sample *graphenepbv1.Resource) []string {
	segments := def.GetPathSegments()
	if len(segments) == 0 {
		segments = make([]string, len(sample.GetKey().GetPath()))
		for i := range segments {
			segments[i] = fmt.Sprintf("seg%d", i+1)
		}
	}

	return upper(segments)
}

// fieldNames lists a schema's fields in declaration order — the order the
// author chose, not map order.
func fieldNames(schema *schemapb.Schema) []string {
	fields := schema.GetFields()

	out := make([]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, field.GetName())
	}

	return out
}

// values renders the named fields of a struct, in order.
func values(source *schemapb.StructValue, names []string) []string {
	if len(names) == 0 {
		return nil
	}

	native := source.ToGo()

	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, cell(native[name]))
	}

	return out
}

// cell renders one value: scalars as themselves, anything nested as a
// marker — a table that pretended to show a whole object would lie.
func cell(value any) string {
	switch typed := value.(type) {
	case nil:
		return emptyCell
	case string:
		if typed == "" {
			return emptyCell
		}

		return typed
	case bool, int32, int64, uint32, uint64, float32, float64:
		return fmt.Sprint(typed)
	case []any, map[string]any:
		return nestedCell
	default:
		return fmt.Sprint(typed)
	}
}

func upper(names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, strings.ToUpper(name))
	}

	return out
}

// pathShape renders a kind's path as the template an operator fills in:
// "/<tenant>/<kernel>" — segment names are placeholders, not literals.
func pathShape(segments []string) string {
	var shape strings.Builder
	for _, segment := range segments {
		shape.WriteString("/<")
		shape.WriteString(segment)
		shape.WriteString(">")
	}

	return shape.String()
}

// WriteDefinitionsTable prints the kind table: what a kernel knows and the
// shape of each kind's path.
func WriteDefinitionsTable(out io.Writer, defs []*graphenepbv1.ResourceDefinition) error {
	writer := tabwriter.NewWriter(out, tabMinWidth, tabWidth, tabPadding, ' ', 0)

	if _, err := fmt.Fprintln(writer, "KIND\tVERSION\tPATH"); err != nil {
		return fmt.Errorf("ctl: write: %w", err)
	}

	sorted := append([]*graphenepbv1.ResourceDefinition{}, defs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].GetKind() < sorted[j].GetKind() })

	for _, def := range sorted {
		line := fmt.Sprintf("%s\tv%d\t%s",
			def.GetKind(), def.GetVersion(), pathShape(def.GetPathSegments()))

		if _, err := fmt.Fprintln(writer, line); err != nil {
			return fmt.Errorf("ctl: write: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("ctl: write: %w", err)
	}

	return nil
}
