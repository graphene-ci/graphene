package ctl

import (
	"context"
	"sort"
	"strings"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
)

// suggestTimeout bounds every completion round trip: a shell tab must
// never hang because a kernel is unreachable. Failures suggest nothing.
const suggestTimeout = 2 * time.Second

// Suggester answers shell completion questions from a live kernel — the
// same API, the same token. Every method fails silently: an empty result
// is a fine answer for a tab press.
type Suggester struct {
	target Target
}

// NewSuggester binds the completion source to a target.
func NewSuggester(target Target) *Suggester {
	return &Suggester{target: target}
}

// Kinds suggests kind names the kernel defines.
func (s *Suggester) Kinds(prefix string) []string {
	var out []string

	s.with(func(ctx context.Context, client *Client) {
		defs, err := client.Definitions(ctx)
		if err != nil {
			return
		}

		for _, def := range defs {
			if strings.HasPrefix(def.GetKind(), prefix) {
				out = append(out, def.GetKind())
			}
		}
	})

	sort.Strings(out)

	return out
}

// Paths suggests the next path segment under what has been typed so far.
//
// Completion is per segment: "acme/" offers the children of acme, and a
// partial segment narrows them. Results keep the typed prefix so the shell
// replaces the whole word.
func (s *Suggester) Paths(kind, typed string) []string {
	done, partial := splitTyped(typed)

	seen := map[string]struct{}{}

	var out []string

	s.with(func(ctx context.Context, client *Client) {
		resources, err := client.List(ctx, kind, done, nil)
		if err != nil {
			return
		}

		for _, res := range resources {
			path := res.GetKey().GetPath()
			if len(path) <= len(done) {
				continue
			}

			next := path[len(done)]
			if !strings.HasPrefix(next, partial) {
				continue
			}

			if _, ok := seen[next]; ok {
				continue
			}

			seen[next] = struct{}{}
			out = append(out, joinTyped(done, next))
		}
	})

	sort.Strings(out)

	return out
}

// Fields suggests selector paths from the kind's schema — something a
// generic client cannot do, and we can: the schemas live in the API.
func (s *Suggester) Fields(kind, prefix string) []string {
	var out []string

	s.with(func(ctx context.Context, client *Client) {
		defs, err := client.Definitions(ctx)
		if err != nil {
			return
		}

		for _, def := range defs {
			if def.GetKind() != kind {
				continue
			}

			out = append(out, fieldPaths("spec", def.GetSpecSchema().GetFields(), prefix)...)
			out = append(out, fieldPaths("status", def.GetStatusSchema().GetFields(), prefix)...)
		}
	})

	sort.Strings(out)

	return out
}

func fieldPaths(root string, fields []*schemapb.Schema_Field, prefix string) []string {
	var out []string

	for _, field := range fields {
		path := root + "." + field.GetName() + "="
		if strings.HasPrefix(path, prefix) {
			out = append(out, path)
		}
	}

	return out
}

// with runs the body against a connected client, doing nothing at all when
// anything goes wrong — a completion must never report errors.
func (s *Suggester) with(body func(ctx context.Context, client *Client)) {
	if s.target.Token == "" || (s.target.Address == "" && s.target.Socket == "") {
		return
	}

	client, err := Connect(s.target)
	if err != nil {
		return
	}

	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), suggestTimeout)
	defer cancel()

	body(ctx, client)
}

// splitTyped separates the completed segments from the one being typed.
func splitTyped(typed string) ([]string, string) {
	if typed == "" {
		return nil, ""
	}

	segments := strings.Split(strings.TrimPrefix(typed, "/"), "/")
	last := len(segments) - 1

	return segments[:last], segments[last]
}

func joinTyped(done []string, next string) string {
	if len(done) == 0 {
		return next
	}

	return strings.Join(done, "/") + "/" + next
}
