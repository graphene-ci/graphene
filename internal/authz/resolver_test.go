package authz

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingStore serves fixed records, counts reads, and fails or holds
// them on demand.
type countingStore struct {
	bindings []Binding
	reads    atomic.Int64
	fail     atomic.Bool
	hold     chan struct{}
}

func (s *countingStore) Roles(context.Context) (map[string]Rules, error) {
	s.reads.Add(1)
	if s.hold != nil {
		<-s.hold
	}
	if s.fail.Load() {
		return nil, errors.New("namespace rate limit exceeded")
	}
	return Builtins(), nil
}

func (s *countingStore) Bindings(context.Context) ([]Binding, error) {
	return s.bindings, nil
}

func resolverAt(store Store, clock *time.Time) *Resolver {
	r := NewResolver(store)
	r.now = func() time.Time { return *clock }
	return r
}

var (
	alice = Identity{Subject: Subject{SubjectUser, "alice"}, Namespace: "default"}
	carol = Identity{Subject: Subject{SubjectUser, "carol"}, Namespace: "prod"}
)

func twoNamespaces() *countingStore {
	return &countingStore{bindings: []Binding{
		{Role: "developer", Subjects: []Subject{alice.Subject}, Namespace: "default"},
		{Role: "developer", Subjects: []Subject{carol.Subject}, Namespace: "prod"},
	}}
}

// The records are installation-wide: callers of every namespace share
// one read, and each is still decided in its own namespace.
func TestResolverReadsOnceForEveryNamespace(t *testing.T) {
	store := twoNamespaces()
	clock := time.Unix(0, 0)
	r := resolverAt(store, &clock)
	for range 3 {
		if d := r.Allow(context.Background(), alice, "", VerbRun, KindPipeline); !d.Allowed {
			t.Fatalf("alice: %s", d.Reason)
		}
		if d := r.Allow(context.Background(), carol, "", VerbRun, KindPipeline); !d.Allowed {
			t.Fatalf("carol: %s", d.Reason)
		}
	}
	if d := r.Allow(context.Background(), Identity{Subject: alice.Subject, Namespace: "prod"}, "", VerbRun, KindPipeline); d.Allowed {
		t.Fatal("a binding in default must not reach prod")
	}
	if n := store.reads.Load(); n != 1 {
		t.Fatalf("store read %d times, want 1", n)
	}
	clock = clock.Add(cacheTTL)
	r.Allow(context.Background(), alice, "", VerbRun, KindPipeline)
	if n := store.reads.Load(); n != 2 {
		t.Fatalf("an expired snapshot must be read again: %d reads", n)
	}
}

// A store that sheds load for a moment must not deny: the last good
// snapshot answers until a read succeeds again.
func TestResolverKeepsTheLastSnapshotOnError(t *testing.T) {
	store := twoNamespaces()
	clock := time.Unix(0, 0)
	r := resolverAt(store, &clock)
	if d := r.Allow(context.Background(), alice, "", VerbRun, KindPipeline); !d.Allowed {
		t.Fatalf("first read: %s", d.Reason)
	}
	store.fail.Store(true)
	clock = clock.Add(time.Minute)
	if d := r.Allow(context.Background(), alice, "", VerbRun, KindPipeline); !d.Allowed {
		t.Fatalf("a failed refresh denied: %s", d.Reason)
	}
	if d := r.Allow(context.Background(), alice, "run", VerbCreate, KindResource); !d.Allowed {
		t.Fatalf("a failed refresh denied a bound role: %s", d.Reason)
	}

	// With no snapshot ever read, the failure is the answer.
	fresh := resolverAt(store, &clock)
	if d := fresh.Allow(context.Background(), alice, "", VerbRun, KindPipeline); d.Allowed || d.Reason == "" {
		t.Fatalf("no snapshot and no store must refuse with a reason: %+v", d)
	}
}

// Callers that find the snapshot stale while a refresh is running keep
// the snapshot they have; only one read reaches the store.
func TestResolverRefreshesOnceAtATime(t *testing.T) {
	store := twoNamespaces()
	clock := time.Unix(0, 0)
	r := resolverAt(store, &clock)
	r.Allow(context.Background(), alice, "", VerbRun, KindPipeline)

	store.hold = make(chan struct{})
	clock = clock.Add(time.Minute)
	refreshing := make(chan struct{})
	go func() {
		defer close(refreshing)
		r.Allow(context.Background(), alice, "", VerbRun, KindPipeline)
	}()
	for store.reads.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if d := r.Allow(context.Background(), carol, "", VerbRun, KindPipeline); !d.Allowed {
				t.Errorf("caller during a refresh: %s", d.Reason)
			}
		})
	}
	wg.Wait()
	close(store.hold)
	<-refreshing
	if n := store.reads.Load(); n != 2 {
		t.Fatalf("store read %d times, want 2", n)
	}
}
