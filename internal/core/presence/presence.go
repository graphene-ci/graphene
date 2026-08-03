// Package presence is how a kernel announces itself: it registers what it
// is, and then keeps saying it is still there.
//
// Definitions are ensured at every start (builtin.Ensure) — this is the
// same idea one level down, for instances. Nobody types a kernel's os and
// arch into a manifest: the kernel knows them, so the kernel writes them.
// An operator provisions the identity and the grants; the machine
// describes itself. kubelet registers its own Node for the same reason.
//
// Two writes, deliberately different:
//
//	Ensure — make a resource present with this spec, and DO NOTHING when
//	         it already says that. An unchanged resource must not bump a
//	         revision: every watcher would see an event for no change.
//	Renew  — write even when nothing changed. That is the whole point of a
//	         lease: the revision bump IS the heartbeat.
package presence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/key"
)

// Ensure makes res present with exactly its spec: absent → create,
// drifted → overwrite by CAS, identical → leave alone.
func Ensure(ctx context.Context, writer controller.Writer, res *graphenepbv1.Resource) error {
	current, err := writer.Get(ctx, key.FromProto(res.GetKey()))

	switch {
	case errors.Is(err, controller.ErrAbsent):
		return put(ctx, writer, res, 0)
	case err != nil:
		return fmt.Errorf("presence: read %s: %w", key.FromProto(res.GetKey()), err)
	case proto.Equal(current.GetSpec(), res.GetSpec()):
		return nil
	}

	// Keep whatever the controllers have written into the status: this
	// kernel owns its spec, not its judged state.
	res.Status = current.GetStatus()

	return put(ctx, writer, res, current.GetRevision())
}

// Renew writes res unconditionally, at whatever revision it currently has.
func Renew(ctx context.Context, writer controller.Writer, res *graphenepbv1.Resource) error {
	var expected uint64

	current, err := writer.Get(ctx, key.FromProto(res.GetKey()))

	switch {
	case errors.Is(err, controller.ErrAbsent):
	case err != nil:
		return fmt.Errorf("presence: read %s: %w", key.FromProto(res.GetKey()), err)
	default:
		expected = current.GetRevision()
	}

	return put(ctx, writer, res, expected)
}

func put(ctx context.Context, writer controller.Writer, res *graphenepbv1.Resource, expected uint64) error {
	if err := writer.Put(ctx, res, expected); err != nil {
		return fmt.Errorf("presence: write %s: %w", key.FromProto(res.GetKey()), err)
	}

	return nil
}

// Kernel is one kernel's presence: the Kernel resource describing it and
// the KernelLease proving it is still running.
type Kernel struct {
	Writer controller.Writer
	// Name is this kernel's identity — the path of both resources and the
	// value its grants interpolate as ${principal.name}.
	Name string
	// OS and Arch describe the machine; the scheduler matches on them.
	OS, Arch string
	// TTL is how long a renewal vouches for this kernel; Interval is how
	// often it renews. The far side judges expiry with its own clock.
	TTL, Interval time.Duration

	Log *slog.Logger
}

// Run registers this kernel and renews its lease until ctx is done.
//
// Registration comes first and is retried until it succeeds: the lease
// controller marks a kernel online only when its Kernel resource is there
// to mark, and it only reacts to the FIRST renewal it sees. A lease that
// arrived before the registration would leave the kernel reading offline
// while it is plainly running.
func (k *Kernel) Run(ctx context.Context) error {
	ticker := time.NewTicker(k.Interval)
	defer ticker.Stop()

	registered := false

	for {
		if !registered {
			if err := Ensure(ctx, k.Writer, k.resource()); err != nil {
				k.logf("kernel registration failed", err)
			} else {
				registered = true
			}
		}

		if registered {
			if err := Renew(ctx, k.Writer, k.lease()); err != nil {
				k.logf("lease renewal failed", err)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (k *Kernel) resource() *graphenepbv1.Resource {
	return &graphenepbv1.Resource{
		Key:  &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{k.Name}},
		Spec: schemapb.MustStructFromGo(map[string]any{"os": k.OS, "arch": k.Arch}),
	}
}

func (k *Kernel) lease() *graphenepbv1.Resource {
	return &graphenepbv1.Resource{
		Key: &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{k.Name}},
		Spec: schemapb.MustStructFromGo(map[string]any{
			"kernel":      k.Name,
			"ttl_seconds": int64(k.TTL / time.Second),
		}),
	}
}

func (k *Kernel) logf(message string, err error) {
	if k.Log == nil {
		return
	}

	k.Log.Warn(message, "kernel", k.Name, "error", err)
}
