// Package storetest is the conformance suite every store backend must
// pass. A backend's tests call Run with a factory producing a fresh empty
// store per subtest.
package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/core/store"
)

// Factory returns a fresh, empty store; cleanup is the caller's t.Cleanup.
type Factory func(t *testing.T) store.Store

// Run executes the whole conformance suite against the backend.
func Run(t *testing.T, factory Factory) {
	t.Run("GetPutCAS", func(t *testing.T) { testGetPutCAS(t, factory(t)) })
	t.Run("CreatedRevision", func(t *testing.T) { testCreatedRevision(t, factory(t)) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, factory(t)) })
	t.Run("Scan", func(t *testing.T) { testScan(t, factory(t)) })
	t.Run("PrefixIsolation", func(t *testing.T) { testPrefixIsolation(t, factory(t)) })
	t.Run("WatchReplay", func(t *testing.T) { testWatchReplay(t, factory(t)) })
	t.Run("WatchSnapshot", func(t *testing.T) { testWatchSnapshot(t, factory(t)) })
	t.Run("WatchLive", func(t *testing.T) { testWatchLive(t, factory(t)) })
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func testGetPutCAS(t *testing.T, s store.Store) {
	c := ctx(t)
	key := store.EncodeKey("Secret", "acme", "prod", "aws")

	if _, err := s.Get(c, key); err != store.ErrNotFound {
		t.Fatalf("get missing: want ErrNotFound, got %v", err)
	}

	rev1, err := s.Put(c, key, []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rev1 == 0 {
		t.Fatal("create: zero revision")
	}

	// Create again must fail: it exists.
	if _, err := s.Put(c, key, []byte("v1b"), 0); err != store.ErrRevisionMismatch {
		t.Fatalf("second create: want ErrRevisionMismatch, got %v", err)
	}
	// Wrong CAS token must fail.
	if _, err := s.Put(c, key, []byte("v2"), rev1+100); err != store.ErrRevisionMismatch {
		t.Fatalf("wrong rev: want ErrRevisionMismatch, got %v", err)
	}

	rev2, err := s.Put(c, key, []byte("v2"), rev1)
	if err != nil {
		t.Fatalf("cas update: %v", err)
	}
	if rev2 <= rev1 {
		t.Fatalf("revisions not monotonic: %d then %d", rev1, rev2)
	}

	e, err := s.Get(c, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(e.Value) != "v2" || e.Revision != rev2 {
		t.Fatalf("get: value=%q rev=%d, want v2/%d", e.Value, e.Revision, rev2)
	}
}

func testCreatedRevision(t *testing.T, s store.Store) {
	c := ctx(t)
	key := store.EncodeKey("Run", "acme", "prod", "wf", "1")

	rev1, err := s.Put(c, key, []byte("a"), 0)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Get(c, key)
	if err != nil {
		t.Fatal(err)
	}
	if e.CreatedRevision != rev1 {
		t.Fatalf("created: got %d want %d", e.CreatedRevision, rev1)
	}

	// Stable across updates.
	rev2, err := s.Put(c, key, []byte("b"), rev1)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ = s.Get(c, key); e.CreatedRevision != rev1 || e.Revision != rev2 {
		t.Fatalf("after update: created=%d rev=%d, want %d/%d", e.CreatedRevision, e.Revision, rev1, rev2)
	}

	// New incarnation after delete+recreate.
	if _, err := s.Delete(c, key, rev2); err != nil {
		t.Fatal(err)
	}
	rev3, err := s.Put(c, key, []byte("c"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ = s.Get(c, key); e.CreatedRevision != rev3 {
		t.Fatalf("recreate: created=%d want %d", e.CreatedRevision, rev3)
	}

	// Watch events carry the incarnation too.
	ch, err := s.Watch(c, key, rev2)
	if err != nil {
		t.Fatal(err)
	}
	ev := recv(t, ch) // the delete at rev2+1
	if ev.Type != store.EventDelete {
		t.Fatalf("expected delete replay, got %v", ev.Type)
	}
	ev = recv(t, ch) // the recreate
	if ev.Entry.CreatedRevision != rev3 {
		t.Fatalf("watch created: got %d want %d", ev.Entry.CreatedRevision, rev3)
	}
}

func testDelete(t *testing.T, s store.Store) {
	c := ctx(t)
	key := store.EncodeKey("Secret", "acme", "prod", "gone")

	rev, err := s.Put(c, key, []byte("x"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := s.Delete(c, key, rev+1); err != store.ErrRevisionMismatch {
		t.Fatalf("delete wrong rev: want ErrRevisionMismatch, got %v", err)
	}
	if _, err := s.Delete(c, key, rev); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get(c, key); err != store.ErrNotFound {
		t.Fatalf("get after delete: want ErrNotFound, got %v", err)
	}
	// Recreate after delete works with expected 0.
	if _, err := s.Put(c, key, []byte("y"), 0); err != nil {
		t.Fatalf("recreate: %v", err)
	}
}

func testScan(t *testing.T, s store.Store) {
	c := ctx(t)
	for _, name := range []string{"a", "b", "c", "d"} {
		if _, err := s.Put(c, store.EncodeKey("Artifact", "acme", "prod", "wf", name), []byte(name), 0); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	prefix := store.EncodePrefix("Artifact", "acme", "prod", "wf")
	var got []string
	var cursor []byte
	for {
		entries, next, err := s.Scan(c, prefix, 3, cursor)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		for _, e := range entries {
			got = append(got, string(e.Value))
		}
		if next == nil {
			break
		}
		cursor = next
	}
	want := []string{"a", "b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("scan: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan order: got %v, want %v", got, want)
		}
	}
}

func testPrefixIsolation(t *testing.T, s store.Store) {
	c := ctx(t)
	if _, err := s.Put(c, store.EncodeKey("Artifact", "acme", "prod", "app"), []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(c, store.EncodeKey("Artifact", "acme", "prod", "app2"), []byte("2"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(c, store.EncodeKey("Secret", "acme", "prod", "app"), []byte("3"), 0); err != nil {
		t.Fatal(err)
	}

	// Whole-segment prefix: "app" must not match "app2"; kind spaces are
	// isolated.
	entries, _, err := s.Scan(c, store.EncodePrefix("Artifact", "acme", "prod", "app"), 0, nil)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Value) != "1" {
		t.Fatalf("prefix isolation broken: %d entries", len(entries))
	}
}

func testWatchReplay(t *testing.T, s store.Store) {
	c := ctx(t)
	key := store.EncodeKey("Execution", "acme", "prod", "wf", "1", "build", "1")

	rev1, err := s.Put(c, key, []byte("pending"), 0)
	if err != nil {
		t.Fatal(err)
	}
	rev2, err := s.Put(c, key, []byte("running"), rev1)
	if err != nil {
		t.Fatal(err)
	}

	// Resume after rev1: must replay exactly the rev2 event, then live.
	ch, err := s.Watch(c, store.EncodePrefix("Execution"), rev1)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	ev := recv(t, ch)
	if ev.StoreRevision != rev2 || string(ev.Entry.Value) != "running" {
		t.Fatalf("replay: got rev=%d value=%q, want %d/running", ev.StoreRevision, ev.Entry.Value, rev2)
	}

	rev3, err := s.Put(c, key, []byte("done"), rev2)
	if err != nil {
		t.Fatal(err)
	}
	ev = recv(t, ch)
	if ev.StoreRevision != rev3 || string(ev.Entry.Value) != "done" {
		t.Fatalf("live after replay: got rev=%d value=%q, want %d/done", ev.StoreRevision, ev.Entry.Value, rev3)
	}
}

func testWatchSnapshot(t *testing.T, s store.Store) {
	c := ctx(t)
	k1 := store.EncodeKey("Node", "acme", "prod", "wf", "1", "a")
	k2 := store.EncodeKey("Node", "acme", "prod", "wf", "1", "b")
	if _, err := s.Put(c, k1, []byte("A"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(c, k2, []byte("B"), 0); err != nil {
		t.Fatal(err)
	}

	ch, err := s.Watch(c, store.EncodePrefix("Node"), 0)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	// Snapshot: both current entries as PUT, in key order.
	e1, e2 := recv(t, ch), recv(t, ch)
	if string(e1.Entry.Value) != "A" || string(e2.Entry.Value) != "B" {
		t.Fatalf("snapshot: got %q,%q want A,B", e1.Entry.Value, e2.Entry.Value)
	}
}

func testWatchLive(t *testing.T, s store.Store) {
	c := ctx(t)
	head, err := s.Revision(c)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}
	ch, err := s.Watch(c, store.EncodePrefix("Run"), head)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Event outside the prefix must not arrive.
	if _, err := s.Put(c, store.EncodeKey("Node", "x"), []byte("noise"), 0); err != nil {
		t.Fatal(err)
	}
	key := store.EncodeKey("Run", "acme", "prod", "wf", "7")
	rev, err := s.Put(c, key, []byte("started"), 0)
	if err != nil {
		t.Fatal(err)
	}

	ev := recv(t, ch)
	if ev.Type != store.EventPut || ev.StoreRevision != rev || string(ev.Entry.Value) != "started" {
		t.Fatalf("live: got type=%d rev=%d value=%q", ev.Type, ev.StoreRevision, ev.Entry.Value)
	}

	// Delete arrives as EventDelete.
	drev, err := s.Delete(c, key, rev)
	if err != nil {
		t.Fatal(err)
	}
	ev = recv(t, ch)
	if ev.Type != store.EventDelete || ev.StoreRevision != drev {
		t.Fatalf("live delete: got type=%d rev=%d, want delete/%d", ev.Type, ev.StoreRevision, drev)
	}
}

func recv(t *testing.T, ch <-chan store.Event) store.Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch event")
		panic("unreachable")
	}
}
