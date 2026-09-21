package cmdutil

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// An EMPTY answer and a WRONG QUESTION look the same from the door: a
// listing of a misspelled kind is an empty listing, the logs of a record
// that never existed are no logs. The helpers here are the second look a
// command takes before it prints "nothing" — so that "nothing" is only ever
// said about something that exists.

// NoRecord is the CLI's one phrasing of a missing record. The door's own
// not-found is Temporal's "workflow not found for ID" — an implementation
// detail nobody should have to read.
func NoRecord(ref string) error {
	if id, ok := strings.CutPrefix(ref, "run/"); ok {
		return &NotFoundError{What: "run " + id}
	}
	return &NotFoundError{What: "record " + ref}
}

// Exit codes: a script tells "it broke" from "there is no such thing" from
// "the run itself failed" without parsing words.
const (
	ExitError     = 1
	ExitNotFound  = 2
	ExitRunFailed = 3
)

// NotFoundError is a target that does not exist; graphenectl exits 2.
type NotFoundError struct{ What string }

func (e *NotFoundError) Error() string { return "no " + e.What }

// RunFailedError is a run that ended any way but Completed: the command
// worked, the RUN did not; graphenectl exits 3.
type RunFailedError struct{ RunId, Status string }

func (e *RunFailedError) Error() string {
	return fmt.Sprintf("run %s: %s", e.RunId, strings.ToLower(e.Status))
}

// ExitCode maps an error onto the process exit code.
func ExitCode(err error) int {
	var notFound *NotFoundError
	var runFailed *RunFailedError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &notFound):
		return ExitNotFound
	case errors.As(err, &runFailed):
		return ExitRunFailed
	}
	return ExitError
}

// OrNoRecord rewrites a not-found from the door into NoRecord and passes
// everything else through.
func OrNoRecord(err error, ref string) error {
	if err != nil && connect.CodeOf(err) == connect.CodeNotFound {
		return NoRecord(ref)
	}
	return err
}

// Lookup reads one record; a missing one answers (nil, NoRecord).
func (d *Door) Lookup(ctx context.Context, ref string) (*managementv1.Resource, error) {
	resp, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{Ref: ref}))
	if err != nil {
		return nil, OrNoRecord(err, ref)
	}
	return resp.Msg.GetResource(), nil
}

// Exists is the second look of an EMPTY dimension: no events, no logs —
// of a record that is there, or of nothing at all? A run answers through
// its own API, every other record through Get.
func (d *Door) Exists(ctx context.Context, ref string) error {
	if id, ok := strings.CutPrefix(ref, "run/"); ok {
		_, err := d.Runs.GetRun(ctx, connect.NewRequest(&managementv1.GetRunRequest{RunId: id}))
		return OrNoRecord(err, ref)
	}
	_, err := d.Lookup(ctx, ref)
	return err
}

// CheckKind is the second look of an EMPTY listing: a kind the dictionary
// does not know is a typo, not an empty set. The nearest known kinds ride
// along in the error.
func (f *Factory) CheckKind(ctx context.Context, d *Door, kind string) error {
	if _, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{Ref: "kind/" + kind})); err == nil {
		return nil
	} else if connect.CodeOf(err) != connect.CodeNotFound {
		// The dictionary could not be asked (no right to read it, say):
		// the empty listing stands as it is.
		return nil
	}
	msg := fmt.Sprintf("unknown kind %q", kind)
	if near := nearest(kind, f.LiveKinds()); len(near) > 0 {
		msg += " — did you mean " + strings.Join(near, ", ") + "?"
	}
	return fmt.Errorf("%s (`graphenectl kinds` lists them)", msg)
}

// nearest picks up to three candidates within a small edit distance, or
// sharing a prefix with the word; closest first.
func nearest(word string, candidates []string) []string {
	type scored struct {
		name string
		dist int
	}
	var hits []scored
	for _, c := range candidates {
		dist := editDistance(strings.ToLower(word), strings.ToLower(c))
		if dist <= max(2, len(word)/3) || (len(word) >= 3 && strings.HasPrefix(c, word)) {
			hits = append(hits, scored{name: c, dist: dist})
		}
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].dist != hits[j].dist {
			return hits[i].dist < hits[j].dist
		}
		return hits[i].name < hits[j].name
	})
	out := make([]string, 0, 3)
	for _, h := range hits {
		if len(out) == 3 {
			break
		}
		out = append(out, h.name)
	}
	return out
}

// editDistance is Levenshtein over bytes — kind names are ASCII.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
