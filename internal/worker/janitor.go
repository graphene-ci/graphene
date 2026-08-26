package worker

// The blob janitor: bytes whose record died WITHOUT its finalizer.
// Deletion normally sweeps a record's prefix (the finalize activities),
// but a terminated workflow runs no finalizer — its bytes stay forever
// with no reference pointing at them. The janitor walks the store's
// known prefix families, asks the records, and removes what nothing
// owns any more.

import (
	"context"
	"strings"
	"time"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// janitorEvery is how often orphans are collected; the first pass runs
// shortly after start, so a terminated record's bytes do not wait a
// whole period.
const (
	janitorEvery = 6 * time.Hour
	janitorFirst = 10 * time.Minute
)

// RunJanitor loops until the context ends.
func (s *Worker) RunJanitor(ctx context.Context) {
	timer := time.NewTimer(janitorFirst)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			removed, err := s.SweepOrphanBlobs(ctx)
			if err != nil {
				s.deps.Log.Warn("blob janitor", xlog.Err(err))
			} else if removed > 0 {
				s.deps.Log.Info("blob janitor collected orphans", xlog.Int("removed", removed))
				obs.Count(ctx, "graphene.janitor.blobs.removed", int64(removed))
			}
			timer.Reset(janitorEvery)
		}
	}
}

// SweepOrphanBlobs removes every blob under a prefix family whose
// owning record no longer exists. Absence of the record is the ONLY
// criterion — phase does not matter, because a record in any phase
// still answers for its bytes.
func (s *Worker) SweepOrphanBlobs(ctx context.Context) (int, error) {
	if s.deps.Blobs == nil {
		return 0, nil
	}
	removed := 0
	// sources/<id>/** — owned by a gitsource OR managedsource record.
	n, err := s.sweepFamily(ctx, "sources/", 1, func(id string) bool {
		if _, _, _, err := s.DescribeGitSource(ctx, id); err == nil {
			return true
		}
		_, _, _, err := s.DescribeManagedSource(ctx, id)
		return err == nil
	})
	removed += n
	if err != nil {
		return removed, err
	}
	// revisions/<pipeline>/<rev>/** — owned by a revision record.
	n, err = s.sweepFamily(ctx, "revisions/", 2, func(id string) bool {
		parts := strings.SplitN(id, "/", 2)
		if len(parts) != 2 {
			return true // unrecognized layout: never guess-delete
		}
		_, _, _, err := s.DescribeRevision(ctx, parts[0], parts[1])
		return err == nil
	})
	removed += n
	if err != nil {
		return removed, err
	}
	// uploads/<pipeline>/** — owned by a pipeline record.
	n, err = s.sweepFamily(ctx, "uploads/", 1, func(id string) bool {
		_, err := s.GetPipeline(ctx, id)
		return err == nil
	})
	removed += n
	return removed, err
}

// sweepFamily lists one prefix family, groups blobs by their first
// `depth` path segments (the owner id), and removes the groups whose
// owner check says "gone".
func (s *Worker) sweepFamily(ctx context.Context, family string, depth int, alive func(id string) bool) (int, error) {
	locations, err := s.deps.Blobs.List(ctx, s.deps.Namespace, family)
	if err != nil {
		return 0, err
	}
	verdict := map[string]bool{}
	removed := 0
	for _, loc := range locations {
		rest := strings.TrimPrefix(loc, family)
		parts := strings.SplitN(rest, "/", depth+1)
		if len(parts) < depth+1 {
			continue // a stray file at family root: leave it
		}
		id := strings.Join(parts[:depth], "/")
		keep, seen := verdict[id]
		if !seen {
			keep = alive(id)
			verdict[id] = keep
		}
		if keep {
			continue
		}
		if err := s.deps.Blobs.Delete(ctx, s.deps.Namespace, loc); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}
