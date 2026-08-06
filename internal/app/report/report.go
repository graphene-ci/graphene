// Package report is what a kernel says about itself.
//
// One record per kernel, written by nobody but the kernel it describes,
// which is why its spec is empty: there is nothing here anybody could
// tell it. A kernel is configured by a file, and the reason is
// recoverability — a configuration reachable only through the kernel
// could not be fixed when it was what broke the kernel.
//
// What is left is a report, and it is worth having. A person asks how a
// kernel is configured; another kernel asks what platform to build a
// controller for, because a controller is a binary and that question has
// to be answerable before one can be sent.
package report

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Recorder is the little of a kernel this package needs: read the
// record, create it if it is not there, write the status.
//
// An interface because a kernel writes this record whether it keeps a
// store or forwards to another one, and it is the SAME record either
// way. A subordinate that recorded itself by some other route would drift
// from one that did not, and the two are supposed to be indistinguishable
// from above.
type Recorder interface {
	Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error)
	Put(ctx context.Context, intent resource.Intent, expect revision.Revision) (revision.Revision, error)
	Report(
		ctx context.Context,
		id resource.Id,
		status *schemapb.StructValue,
		expect revision.Revision,
	) (revision.Revision, error)
}

// Publish makes sure the kernel's own kind is defined.
//
// It goes through Define like anybody's kind, so it is idempotent against
// the shape and does nothing on every start but the first.
func Publish(ctx context.Context, k kernel.Kernel) error {
	if _, err := k.Define(ctx, Definition()); err != nil {
		return fmt.Errorf("define %s: %w", KernelKind, err)
	}

	return nil
}

// Write records what this kernel is running with, and that it is there.
//
// It runs at every start, again whenever the configuration changes, and
// again every Beat — because what it says is what is running NOW, and
// "now" is half of what the record says. One writer and one code path:
// the beat is not a second kind of write that could disagree with this
// one about anything else in the record.
//
// The record is created if it is not there; its spec stays empty either
// way.
func Write(ctx context.Context, k Recorder, running config.Config, version string) error {
	id, err := Id(running.Name())
	if err != nil {
		return err
	}

	stored, err := k.Get(ctx, id)

	switch {
	case errors.Is(err, store.ErrNotFound):
		intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{}))
		if err != nil {
			return err
		}

		if _, err := k.Put(ctx, intent, revision.Absent); err != nil {
			return fmt.Errorf("create %s: %w", id, err)
		}

		stored, err = k.Get(ctx, id)
		if err != nil {
			return err
		}

	case err != nil:
		return err
	}

	if _, err := k.Report(ctx, id, status(running, version, time.Now()), stored.Revision); err != nil {
		return fmt.Errorf("report %s: %w", id, err)
	}

	return nil
}

// status is what a kernel reports: what it is, and what it was told to be.
//
// A subordinate says where it forwards and nothing about a store, because
// it has none — and NOT its token, which is a secret and is why the top
// of this file says what it says. The address is not: knowing which
// kernel a kernel answers to is the point of the record.
func status(running config.Config, version string, now time.Time) *schemapb.StructValue {
	reported := map[string]any{
		osField:      runtime.GOOS,
		archField:    runtime.GOARCH,
		versionField: version,
		listenField:  running.Listen(),
		// When it last said it was here, and how often it means to say
		// it. The second one is written down rather than assumed by
		// readers, so a reader from a different build does not call a
		// kernel with a different beat dead.
		heartbeatField: now.UTC().Format(time.RFC3339),
		beatField:      uint64(Beat / time.Second),
	}

	if up, forwards := running.Upstream(); forwards {
		reported[upstreamField] = up.Address()
	}

	if local, keeps := running.Local(); keeps {
		reported[storeField] = local.Store()
		reported[cacheField] = uint64(local.Cache())
	}

	return schemapb.MustStructFromGo(reported)
}
