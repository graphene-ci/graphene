package ctl_test

import (
	"bytes"
	"strings"
	"testing"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/cmd/graphene/commands/ctl"
	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
)

// An empty result must not be silent: the table formats say so on stderr,
// while stdout stays empty for pipes; the exchange formats stay silent —
// zero documents is a valid stream.
func TestRenderSaysWhenEmpty(t *testing.T) {
	t.Parallel()

	addr := appctl.ParseAddress("Kernel", "acme")

	var out, errOut bytes.Buffer
	if err := ctl.Render(&out, &errOut, appctl.FormatTable, addr, nil, nil); err != nil {
		t.Fatalf("render: %v", err)
	}

	if out.Len() != 0 {
		t.Fatalf("stdout is not pipe-clean: %q", out.String())
	}

	if !strings.Contains(errOut.String(), "Kernel acme") {
		t.Fatalf("stderr does not name the address: %q", errOut.String())
	}

	out.Reset()
	errOut.Reset()

	if err := ctl.Render(&out, &errOut, appctl.FormatYAML, addr, nil, nil); err != nil {
		t.Fatalf("render yaml: %v", err)
	}

	if out.Len() != 0 || errOut.Len() != 0 {
		t.Fatalf("empty yaml must be silent: stdout=%q stderr=%q", out.String(), errOut.String())
	}
}

// A non-empty result renders as before and writes nothing to stderr.
func TestRenderTableWritesRows(t *testing.T) {
	t.Parallel()

	res := &graphenepbv1.Resource{
		Key:      &graphenepbv1.Key{Kind: "Kernel", Path: []string{"acme", "k1"}},
		Spec:     schemapb.MustStructFromGo(map[string]any{"os": "linux"}),
		Revision: 3,
	}

	var out, errOut bytes.Buffer
	if err := ctl.Render(&out, &errOut, appctl.FormatTable, appctl.ParseAddress("Kernel", ""),
		nil, []*graphenepbv1.Resource{res}); err != nil {
		t.Fatalf("render: %v", err)
	}

	if !strings.Contains(out.String(), "k1") {
		t.Fatalf("table misses the row: %q", out.String())
	}

	if errOut.Len() != 0 {
		t.Fatalf("stderr is not clean: %q", errOut.String())
	}
}
