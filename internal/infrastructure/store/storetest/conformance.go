// Package storetest is the conformance suite every store backend must
// pass. A backend's tests call Run with a factory producing a fresh empty
// store per subtest.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/core/store"
)

const (
	caseTimeout = 10 * time.Second
	recvTimeout = 5 * time.Second
	scanPage    = 3
)

// Factory returns a fresh, empty store; cleanup is the caller's t.Cleanup.
type Factory func(t *testing.T) store.Store

// Run executes the whole conformance suite against the backend.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	cases := map[string]func(*testing.T, store.Store){
		"GetPutCAS":       testGetPutCAS,
		"CreatedRevision": testCreatedRevision,
		"Delete":          testDelete,
		"Scan":            testScan,
		"PrefixIsolation": testPrefixIsolation,
		"WatchReplay":     testWatchReplay,
		"WatchSync":       testWatchSync,
		"WatchSnapshot":   testWatchSnapshot,
		"WatchLive":       testWatchLive,
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testCase(t, factory(t))
		})
	}
}

func testCtx(t *testing.T) context.Context {
	t.Helper()

	c, cancel := context.WithTimeout(context.Background(), caseTimeout)
	t.Cleanup(cancel)

	return c
}

func testGetPutCAS(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)
	key := store.EncodeKey("Secret", "acme", "prod", "aws")

	if _, err := s.Get(c, key); !errors.Is(err, store.ErrNotFound) {
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
	if _, err := s.Put(c, key, []byte("v1b"), 0); !errors.Is(err, store.ErrRevisionMismatch) {
		t.Fatalf("second create: want ErrRevisionMismatch, got %v", err)
	}
	// Wrong CAS token must fail.
	const revGap = 100
	if _, err := s.Put(c, key, []byte("v2"), rev1+revGap); !errors.Is(err, store.ErrRevisionMismatch) {
		t.Fatalf("wrong rev: want ErrRevisionMismatch, got %v", err)
	}

	rev2, err := s.Put(c, key, []byte("v2"), rev1)
	if err != nil {
		t.Fatalf("cas update: %v", err)
	}

	if rev2 <= rev1 {
		t.Fatalf("revisions not monotonic: %d then %d", rev1, rev2)
	}

	entry, err := s.Get(c, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if string(entry.Value) != "v2" || entry.Revision != rev2 {
		t.Fatalf("get: value=%q rev=%d, want v2/%d", entry.Value, entry.Revision, rev2)
	}
}

func testCreatedRevision(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)
	key := store.EncodeKey("Run", "acme", "prod", "wf", "1")

	rev1, err := s.Put(c, key, []byte("a"), 0)
	if err != nil {
		t.Fatal(err)
	}

	entry, err := s.Get(c, key)
	if err != nil {
		t.Fatal(err)
	}

	if entry.CreatedRevision != rev1 {
		t.Fatalf("created: got %d want %d", entry.CreatedRevision, rev1)
	}

	// Stable across updates.
	rev2, err := s.Put(c, key, []byte("b"), rev1)
	if err != nil {
		t.Fatal(err)
	}

	if entry, _ = s.Get(c, key); entry.CreatedRevision != rev1 || entry.Revision != rev2 {
		t.Fatalf("after update: created=%d rev=%d, want %d/%d", entry.CreatedRevision, entry.Revision, rev1, rev2)
	}

	// New incarnation after delete+recreate.
	if _, err := s.Delete(c, key, rev2); err != nil {
		t.Fatal(err)
	}

	rev3, err := s.Put(c, key, []byte("c"), 0)
	if err != nil {
		t.Fatal(err)
	}

	if entry, _ = s.Get(c, key); entry.CreatedRevision != rev3 {
		t.Fatalf("recreate: created=%d want %d", entry.CreatedRevision, rev3)
	}

	// Watch events carry the incarnation too.
	events, err := s.Watch(c, key, rev2)
	if err != nil {
		t.Fatal(err)
	}

	event := recvData(t, events) // the delete at rev2+1
	if event.Type != store.EventDelete {
		t.Fatalf("expected delete replay, got %v", event.Type)
	}

	event = recvData(t, events) // the recreate
	if event.Entry.CreatedRevision != rev3 {
		t.Fatalf("watch created: got %d want %d", event.Entry.CreatedRevision, rev3)
	}
}

func testDelete(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)
	key := store.EncodeKey("Secret", "acme", "prod", "gone")

	rev, err := s.Put(c, key, []byte("x"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := s.Delete(c, key, rev+1); !errors.Is(err, store.ErrRevisionMismatch) {
		t.Fatalf("delete wrong rev: want ErrRevisionMismatch, got %v", err)
	}

	if _, err := s.Delete(c, key, rev); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := s.Get(c, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get after delete: want ErrNotFound, got %v", err)
	}
	// Recreate after delete works with expected 0.
	if _, err := s.Put(c, key, []byte("y"), 0); err != nil {
		t.Fatalf("recreate: %v", err)
	}
}

func testScan(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)

	for _, name := range []string{"a", "b", "c", "d"} {
		if _, err := s.Put(c, store.EncodeKey("Artifact", "acme", "prod", "wf", name), []byte(name), 0); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	prefix := store.EncodePrefix("Artifact", "acme", "prod", "wf")

	var (
		got    []string
		cursor []byte
	)

	for {
		entries, next, err := s.Scan(c, prefix, scanPage, cursor)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		for _, entry := range entries {
			got = append(got, string(entry.Value))
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
	t.Helper()

	c := testCtx(t)

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
	t.Helper()

	c := testCtx(t)
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
	events, err := s.Watch(c, store.EncodePrefix("Execution"), rev1)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	event := recvData(t, events)
	if event.StoreRevision != rev2 || string(event.Entry.Value) != "running" {
		t.Fatalf("replay: got rev=%d value=%q, want %d/running", event.StoreRevision, event.Entry.Value, rev2)
	}

	rev3, err := s.Put(c, key, []byte("done"), rev2)
	if err != nil {
		t.Fatal(err)
	}

	event = recvData(t, events)
	if event.StoreRevision != rev3 || string(event.Entry.Value) != "done" {
		t.Fatalf("live after replay: got rev=%d value=%q, want %d/done", event.StoreRevision, event.Entry.Value, rev3)
	}
}

func testWatchSnapshot(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)
	key1 := store.EncodeKey("Node", "acme", "prod", "wf", "1", "a")
	key2 := store.EncodeKey("Node", "acme", "prod", "wf", "1", "b")

	if _, err := s.Put(c, key1, []byte("A"), 0); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Put(c, key2, []byte("B"), 0); err != nil {
		t.Fatal(err)
	}

	events, err := s.Watch(c, store.EncodePrefix("Node"), 0)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Snapshot: both current entries as PUT, in key order.
	first, second := recvData(t, events), recvData(t, events)
	if string(first.Entry.Value) != "A" || string(second.Entry.Value) != "B" {
		t.Fatalf("snapshot: got %q,%q want A,B", first.Entry.Value, second.Entry.Value)
	}
}

func testWatchLive(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)

	head, err := s.Revision(c)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}

	events, err := s.Watch(c, store.EncodePrefix("Run"), head)
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

	event := recvData(t, events)
	if event.Type != store.EventPut || event.StoreRevision != rev || string(event.Entry.Value) != "started" {
		t.Fatalf("live: got type=%d rev=%d value=%q", event.Type, event.StoreRevision, event.Entry.Value)
	}

	// Delete arrives as EventDelete.
	drev, err := s.Delete(c, key, rev)
	if err != nil {
		t.Fatal(err)
	}

	event = recvData(t, events)
	if event.Type != store.EventDelete || event.StoreRevision != drev {
		t.Fatalf("live delete: got type=%d rev=%d, want delete/%d", event.Type, event.StoreRevision, drev)
	}
}

// recvData returns the next non-sync event: most assertions are about
// data, and exactly one sync marker precedes the live stream.
func recvData(t *testing.T, events <-chan store.Event) store.Event {
	t.Helper()

	for {
		event := recv(t, events)
		if event.Type != store.EventSync {
			return event
		}
	}
}

// testWatchSync pins the catch-up contract: exactly one sync marker after
// the catch-up part and before any live event, carrying the revision that
// is safe to resume from.
func testWatchSync(t *testing.T, s store.Store) {
	t.Helper()

	c := testCtx(t)
	key := store.EncodeKey("Run", "acme", "prod", "wf", "1")

	rev, err := s.Put(c, key, []byte("a"), 0)
	if err != nil {
		t.Fatal(err)
	}

	events, err := s.Watch(c, store.EncodePrefix("Run"), 0)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot first...
	if snap := recv(t, events); snap.Type != store.EventPut || string(snap.Entry.Value) != "a" {
		t.Fatalf("snapshot: got type=%d value=%q", snap.Type, snap.Entry.Value)
	}

	// ...then the boundary, carrying the head it caught up to.
	sync := recv(t, events)
	if sync.Type != store.EventSync {
		t.Fatalf("expected sync marker, got type=%d", sync.Type)
	}

	if sync.StoreRevision < rev {
		t.Fatalf("sync revision %d predates the snapshot (%d)", sync.StoreRevision, rev)
	}

	// Resuming from the sync revision must replay nothing already seen.
	resumed, err := s.Watch(c, store.EncodePrefix("Run"), sync.StoreRevision)
	if err != nil {
		t.Fatal(err)
	}

	if first := recv(t, resumed); first.Type != store.EventSync {
		t.Fatalf("resume replayed %d (already-seen data), want sync first", first.Type)
	}
}

func recv(t *testing.T, events <-chan store.Event) store.Event {
	t.Helper()

	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}

		return event
	case <-time.After(recvTimeout):
		t.Fatal("timeout waiting for watch event")
		panic("unreachable")
	}
}
