package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

// Lease is the liveness controller: a worker renews its KernelLease by
// ordinary Puts (a renewal is a revision bump, nothing more), and THIS
// side judges expiry with the control kernel's clock — the store holds no
// timestamps and no worker clock is ever trusted.
//
// State is in-memory: after a controller restart the countdown starts
// anew (expiry is delayed by at most one ttl — the safe direction).
//
// Kernel.status.online is written by this controller only; the Kernel
// resource itself is created by the operator when the join token is
// issued.
type Lease struct {
	resources *service.Resources
	st        store.Store
	now       func() time.Time

	mu    sync.Mutex
	seen  map[string]*leaseState // key: tenant/kernel path joined
	putMu sync.Mutex             // serializes status flips
}

type leaseState struct {
	path     []string // {tenant, kernel}
	lastSeen time.Time
	ttl      time.Duration
	online   bool
}

// NewLease wires the controller; now is injectable for deterministic tests.
func NewLease(resources *service.Resources, st store.Store, now func() time.Time) *Lease {
	return &Lease{
		resources: resources,
		st:        st,
		now:       now,
		seen:      make(map[string]*leaseState),
	}
}

// Run consumes lease events until ctx is done; pair it with a sweeper.
func (l *Lease) Run(ctx context.Context) error {
	loop := &Loop{Store: l.st, Kind: builtin.KindKernelLease, Handle: l.handle}

	return loop.Run(ctx)
}

// RunSweeper checks expirations every interval until ctx is done.
func (l *Lease) RunSweeper(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.Sweep(ctx)
		}
	}
}

func (l *Lease) handle(ctx context.Context, typ store.EventType, res *graphenepbv1.Resource) error {
	path := res.GetKey().GetPath()
	stored := key.New(builtin.KindKernelLease, path...).Encode()

	l.mu.Lock()

	if typ == store.EventDelete {
		delete(l.seen, string(stored))
		l.mu.Unlock()

		// A removed lease means the kernel is administratively gone.
		return l.setOnline(ctx, path, false)
	}

	state, ok := l.seen[string(stored)]
	if !ok {
		state = &leaseState{path: append([]string(nil), path...)}
		l.seen[string(stored)] = state
	}

	state.lastSeen = l.now()
	state.ttl = time.Duration(leaseTTLSeconds(res)) * time.Second
	wasOnline := state.online
	state.online = true
	l.mu.Unlock()

	if wasOnline {
		return nil
	}

	return l.setOnline(ctx, path, true)
}

// Sweep flips kernels whose leases ran out; errors are per-kernel and
// deliberately swallowed (the next tick retries).
func (l *Lease) Sweep(ctx context.Context) {
	now := l.now()

	l.mu.Lock()

	var expired []*leaseState

	for _, state := range l.seen {
		if state.online && state.ttl > 0 && now.Sub(state.lastSeen) > state.ttl {
			state.online = false

			expired = append(expired, state)
		}
	}
	l.mu.Unlock()

	for _, state := range expired {
		_ = l.setOnline(ctx, state.path, false)
	}
}

// setOnline writes Kernel.status.online with a small CAS retry.
func (l *Lease) setOnline(ctx context.Context, path []string, online bool) error {
	l.putMu.Lock()
	defer l.putMu.Unlock()

	ctx = SystemContext(ctx)

	for {
		got, err := l.resources.Get(ctx, &graphenepbv1.GetRequest{
			Key: &graphenepbv1.Key{Kind: builtin.KindKernel, Path: path},
		})
		if status.Code(err) == codes.NotFound {
			return nil // the operator has not registered this kernel; nothing to mark
		}

		if err != nil {
			return fmt.Errorf("lease: read kernel: %w", err)
		}

		res := got.GetResource()
		if kernelOnline(res) == online {
			return nil
		}

		res.Status = schemapb.MustStructFromGo(map[string]any{"online": online})

		_, err = l.resources.Put(ctx, &graphenepbv1.PutRequest{
			Resource:         res,
			ExpectedRevision: res.GetRevision(),
		})
		if status.Code(err) == codes.Aborted {
			continue // lost a CAS race; re-read
		}

		if err != nil {
			return fmt.Errorf("lease: write kernel status: %w", err)
		}

		return nil
	}
}

func leaseTTLSeconds(res *graphenepbv1.Resource) int64 {
	ttl, ok := res.GetSpec().ToGo()["ttl_seconds"].(int64)
	if !ok {
		return 0
	}

	return ttl
}

func kernelOnline(res *graphenepbv1.Resource) bool {
	online, ok := res.GetStatus().ToGo()["online"].(bool)

	return ok && online
}
