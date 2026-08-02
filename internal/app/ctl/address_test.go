package ctl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	appctl "github.com/graphene-ci/graphene/internal/app/ctl"
	"github.com/graphene-ci/graphene/internal/core/builtin"
)

func TestParseAddress(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		kind string
		path string
		want []string
	}{
		"kind only":       {kind: "Kernel", path: "", want: nil},
		"single segment":  {kind: "Kernel", path: "acme", want: []string{"acme"}},
		"full path":       {kind: "Kernel", path: "acme/k1", want: []string{"acme", "k1"}},
		"leading slash":   {kind: "Kernel", path: "/acme/k1", want: []string{"acme", "k1"}},
		"trailing slash":  {kind: "Kernel", path: "acme/", want: []string{"acme"}},
		"only separators": {kind: "Kernel", path: "//", want: nil},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := appctl.ParseAddress(tc.kind, tc.path)
			if len(got.Path) != len(tc.want) {
				t.Fatalf("path: got %v, want %v", got.Path, tc.want)
			}

			for i := range tc.want {
				if got.Path[i] != tc.want[i] {
					t.Fatalf("path: got %v, want %v", got.Path, tc.want)
				}
			}
		})
	}
}

// The kind's arity decides whether an address names one resource or a
// subtree — no flag, and a path longer than the kind allows is an error
// rather than a silent empty list.
func TestAddressArity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	client := startKernel(ctx, t)

	exact, err := client.Exact(ctx, appctl.ParseAddress(builtin.KindKernel, "acme/k1"))
	if err != nil {
		t.Fatalf("exact: %v", err)
	}

	if !exact {
		t.Fatal("a full path must name one resource")
	}

	prefix, err := client.Exact(ctx, appctl.ParseAddress(builtin.KindKernel, "acme"))
	if err != nil {
		t.Fatalf("prefix: %v", err)
	}

	if prefix {
		t.Fatal("a short path must name a subtree")
	}

	if _, err := client.Exact(ctx, appctl.ParseAddress(builtin.KindKernel, "acme/k1/extra")); !errors.Is(err, appctl.ErrPathTooLong) {
		t.Fatalf("overlong path: want ErrPathTooLong, got %v", err)
	}

	if _, err := client.Exact(ctx, appctl.ParseAddress("Nope", "x")); !errors.Is(err, appctl.ErrUnknownKind) {
		t.Fatalf("unknown kind: want ErrUnknownKind, got %v", err)
	}
}

func TestOutputFormats(t *testing.T) {
	t.Parallel()

	res := &graphenepbv1.Resource{
		Key:      &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{"acme", "k1"}},
		Revision: 3,
	}

	// protojson deliberately varies its whitespace, so the json case is
	// checked by parsing rather than by comparing text.
	cases := map[string]struct {
		format string
		want   string
		parse  bool
	}{
		"yaml": {format: "yaml", want: "kind: Kernel"},
		"json": {format: "json", parse: true},
		"name": {format: "name", want: "Kernel acme/k1"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			format, err := appctl.ParseFormat(tc.format)
			if err != nil {
				t.Fatalf("parse format: %v", err)
			}

			var out bytes.Buffer
			if err := appctl.Write(&out, format, []*graphenepbv1.Resource{res}); err != nil {
				t.Fatalf("write: %v", err)
			}

			if tc.parse {
				var decoded map[string]any
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatalf("json output does not parse: %v\n%s", err, out.String())
				}

				keyPart, _ := decoded["key"].(map[string]any)
				if keyPart["kind"] != builtin.KindKernel {
					t.Fatalf("json output: %v", decoded)
				}

				return
			}

			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("output %q does not contain %q", out.String(), tc.want)
			}
		})
	}

	if _, err := appctl.ParseFormat("xml"); !errors.Is(err, appctl.ErrUnknownFormat) {
		t.Fatalf("unknown format: got %v", err)
	}
}

// The suggester answers from the live kernel: kinds, path segments, and
// selector fields taken from the kind's schema.
func TestSuggester(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	client := startKernel(ctx, t)

	doc := "key:\n  kind: " + builtin.KindKernel + "\n  path: [acme, k9]\n" +
		"spec:\n  fields:\n    os: { stringValue: linux }\n    arch: { stringValue: amd64 }\n"
	if _, err := client.Apply(ctx, []byte(doc)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	suggester := appctl.NewSuggester(client.Target())

	kinds := suggester.Kinds("Kern")
	if !contains(kinds, builtin.KindKernel) || !contains(kinds, builtin.KindKernelLease) {
		t.Fatalf("kind suggestions: %v", kinds)
	}

	if contains(suggester.Kinds("Kern"), builtin.KindRole) {
		t.Fatalf("prefix ignored: %v", kinds)
	}

	tenants := suggester.Paths(builtin.KindKernel, "")
	if !contains(tenants, "acme") {
		t.Fatalf("tenant suggestions: %v", tenants)
	}

	names := suggester.Paths(builtin.KindKernel, "acme/")
	if !contains(names, "acme/k9") {
		t.Fatalf("name suggestions: %v", names)
	}

	// Selector fields come from the kind's schema — a generic client
	// cannot know them, we can.
	fields := suggester.Fields(builtin.KindKernel, "")
	if !contains(fields, "spec.os=") || !contains(fields, "status.online=") {
		t.Fatalf("field suggestions: %v", fields)
	}

	// An unreachable kernel suggests nothing instead of hanging or failing.
	dead := appctl.NewSuggester(appctl.Target{Socket: "/nonexistent.sock", Token: "x"})
	if got := dead.Kinds(""); len(got) != 0 {
		t.Fatalf("unreachable kernel suggested %v", got)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}
