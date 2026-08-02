package controller_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/auth/static"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

// clock is a settable time source: expiry tests advance it explicitly —
// no sleeps, no flakes.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type env struct {
	resources *service.Resources
	lease     *controller.Lease
	clk       *clock
	operator  context.Context
	worker    context.Context
}

func newEnv(t *testing.T) *env {
	t.Helper()

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	reg := registry.New(st)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := builtin.Ensure(controller.SystemContext(ctx), reg); err != nil {
		t.Fatalf("ensure builtins: %v", err)
	}

	resources := service.NewResources(st, reg)
	clk := &clock{now: time.Unix(1_000_000, 0)}
	lease := controller.NewLease(resources, st, clk.Now)

	go func() { _ = lease.Run(ctx) }()

	return &env{
		resources: resources,
		lease:     lease,
		clk:       clk,
		operator:  auth.WithCredentials(ctx, static.Admin("op")),
		worker:    auth.WithCredentials(ctx, static.Kernel("k1")),
	}
}

func (e *env) registerKernel(t *testing.T) {
	t.Helper()

	_, err := e.resources.Put(e.operator, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key:  &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{"k1"}},
			Spec: schemapb.MustStructFromGo(map[string]any{"os": "linux", "arch": "amd64"}),
		},
	})
	if err != nil {
		t.Fatalf("register kernel: %v", err)
	}
}

func (e *env) renewLease(t *testing.T) {
	t.Helper()

	key := &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{"k1"}}

	var expected uint64
	if got, err := e.resources.Get(e.worker, &graphenepbv1.GetRequest{Key: key}); err == nil {
		expected = got.GetResource().GetRevision()
	}

	_, err := e.resources.Put(e.worker, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key:  key,
			Spec: schemapb.MustStructFromGo(map[string]any{"kernel": "k1", "ttl_seconds": int64(10)}),
		},
		ExpectedRevision: expected,
	})
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
}

func (e *env) online(t *testing.T) bool {
	t.Helper()

	got, err := e.resources.Get(e.operator, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: builtin.KindKernel, Path: []string{"k1"}},
	})
	if err != nil {
		t.Fatalf("get kernel: %v", err)
	}

	online, _ := got.GetResource().GetStatus().ToGo()["online"].(bool)

	return online
}

func (e *env) waitOnline(t *testing.T, want bool) {
	t.Helper()

	// Generous on purpose: this waits for a watch round trip, and a loaded
	// machine running the whole suite in parallel is exactly when a tight
	// deadline turns a healthy system into a flaky test.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if e.online(t) == want {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("kernel online never became %v", want)
}

func TestLeaseLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.registerKernel(t)

	// Fresh kernel: no lease seen yet, no status.
	if e.online(t) {
		t.Fatal("kernel online before any lease")
	}

	// First renewal → online.
	e.renewLease(t)
	e.waitOnline(t, true)

	// Renewals within ttl keep it online.
	e.clk.Advance(5 * time.Second)
	e.renewLease(t)
	e.lease.Sweep(context.Background())

	if !e.online(t) {
		t.Fatal("kernel dropped offline despite renewals")
	}

	// Silence beyond ttl → offline (judged by the CONTROL clock only).
	e.clk.Advance(11 * time.Second)
	e.lease.Sweep(context.Background())
	e.waitOnline(t, false)

	// Coming back: a new renewal flips it online again.
	e.renewLease(t)
	e.waitOnline(t, true)
}

func TestLeaseDeleteMarksOffline(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.registerKernel(t)
	e.renewLease(t)
	e.waitOnline(t, true)

	got, err := e.resources.Get(e.operator, &graphenepbv1.GetRequest{
		Key: &graphenepbv1.Key{Kind: builtin.KindKernelLease, Path: []string{"k1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = e.resources.Delete(e.operator, &graphenepbv1.DeleteRequest{
		Key:              got.GetResource().GetKey(),
		ExpectedRevision: got.GetResource().GetRevision(),
	})
	if err != nil {
		t.Fatalf("delete lease: %v", err)
	}

	e.waitOnline(t, false)
}

func TestEnsureIsIdempotent(t *testing.T) {
	t.Parallel()

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = st.Close() })

	reg := registry.New(st)
	ctx := controller.SystemContext(context.Background())

	for range 3 {
		if err := builtin.Ensure(ctx, reg); err != nil {
			t.Fatalf("ensure: %v", err)
		}
	}

	for _, kind := range []string{builtin.KindKernel, builtin.KindKernelLease} {
		def, err := reg.Get(ctx, kind, 0)
		if err != nil {
			t.Fatalf("get %s: %v", kind, err)
		}

		if def.GetVersion() != 1 {
			t.Fatalf("%s: version %d after repeated ensure, want 1", kind, def.GetVersion())
		}
	}
}
