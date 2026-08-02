package ctl_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
	"github.com/graphene-ci/graphene/internal/core/builtin"
)

// Columns come from the kind's definition, so a table needs no per-kind
// code: path segments name the leading columns, the status schema names
// the rest, and wide adds the spec.
func TestTableColumnsComeFromTheSchema(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	client := startKernel(ctx, t)

	doc := "key:\n  kind: " + builtin.KindKernel + "\n  path: [k1]\n" +
		"spec:\n  fields:\n    os: { stringValue: linux }\n    arch: { stringValue: amd64 }\n"
	if _, err := client.Apply(ctx, []byte(doc)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	def, err := client.Definition(ctx, builtin.KindKernel)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}

	resources, err := client.List(ctx, builtin.KindKernel, nil, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	var table bytes.Buffer
	if err := appctl.Write(&table, appctl.FormatTable, def, resources); err != nil {
		t.Fatalf("table: %v", err)
	}

	head, row := lines(t, table.String())

	// Path columns are named by the definition, not invented.
	for _, want := range []string{"KERNEL", "REVISION", "ONLINE"} {
		if !strings.Contains(head, want) {
			t.Fatalf("table header %q misses %s", head, want)
		}
	}

	// The spec belongs to wide only.
	if strings.Contains(head, "OS") {
		t.Fatalf("plain table shows spec columns: %q", head)
	}

	if !strings.Contains(row, "k1") {
		t.Fatalf("table row %q misses the path", row)
	}

	// An absent status field reads as empty, not as a missing column.
	if !strings.Contains(row, "-") {
		t.Fatalf("table row %q does not mark the empty status", row)
	}

	var wide bytes.Buffer
	if err := appctl.Write(&wide, appctl.FormatWide, def, resources); err != nil {
		t.Fatalf("wide: %v", err)
	}

	wideHead, wideRow := lines(t, wide.String())
	for _, want := range []string{"OS", "ARCH"} {
		if !strings.Contains(wideHead, want) {
			t.Fatalf("wide header %q misses %s", wideHead, want)
		}
	}

	if !strings.Contains(wideRow, "linux") || !strings.Contains(wideRow, "amd64") {
		t.Fatalf("wide row %q misses the spec values", wideRow)
	}
}

// Without a definition (an unknown kind) the table still prints rather
// than failing: positional column names are better than nothing.
func TestTableSurvivesMissingDefinition(t *testing.T) {
	t.Parallel()

	res := &graphenepbv1.Resource{
		Key:      &graphenepbv1.Key{Kind: "Mystery", Path: []string{"a", "b"}},
		Spec:     schemapb.MustStructFromGo(map[string]any{"x": "y"}),
		Revision: 4,
	}

	var out bytes.Buffer
	if err := appctl.Write(&out, appctl.FormatTable, nil, []*graphenepbv1.Resource{res}); err != nil {
		t.Fatalf("table: %v", err)
	}

	head, row := lines(t, out.String())
	if !strings.Contains(head, "SEG1") || !strings.Contains(head, "SEG2") {
		t.Fatalf("fallback header: %q", head)
	}

	if !strings.Contains(row, "a") || !strings.Contains(row, "4") {
		t.Fatalf("fallback row: %q", row)
	}
}

// Nested values cannot be shown honestly in a cell, so they are marked
// rather than mangled.
func TestTableMarksNestedValues(t *testing.T) {
	t.Parallel()

	def := &graphenepbv1.ResourceDefinition{
		Kind:         "Thing",
		PathSegments: []string{"tenant", "name"},
		SpecSchema: schemapb.NewSchema(&schemapb.SchemaIdentity{Name: "thing"}).
			Fields(schemapb.List("tags", schemapb.Str("tag"))).
			MustBuild(),
	}

	res := &graphenepbv1.Resource{
		Key:  &graphenepbv1.Key{Kind: "Thing", Path: []string{"acme", "one"}},
		Spec: schemapb.MustStructFromGo(map[string]any{"tags": []any{"a", "b"}}),
	}

	var out bytes.Buffer
	if err := appctl.Write(&out, appctl.FormatWide, def, []*graphenepbv1.Resource{res}); err != nil {
		t.Fatalf("wide: %v", err)
	}

	if _, row := lines(t, out.String()); !strings.Contains(row, "…") {
		t.Fatalf("nested value not marked: %q", row)
	}
}

// The default is what a person gets when they ask for nothing: columns,
// not the exchange form. The exchange forms are requested explicitly, so
// `get -o yaml | apply` stays a deliberate act.
func TestDefaultFormatIsTable(t *testing.T) {
	t.Parallel()

	format, err := appctl.ParseFormat("")
	if err != nil {
		t.Fatalf("parse empty format: %v", err)
	}

	if format != appctl.FormatTable {
		t.Fatalf("default format is %q, want table", format)
	}

	if !format.NeedsDefinition() {
		t.Fatal("the default format must be told its kind's definition")
	}
}

func lines(t *testing.T, text string) (string, string) {
	t.Helper()

	rows := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(rows) < 2 {
		t.Fatalf("expected a header and at least one row, got %q", text)
	}

	return rows[0], rows[1]
}
