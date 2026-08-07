package link_test

import (
	"errors"
	"testing"

	"google.golang.org/grpc"

	"github.com/graphene-ci/graphene/internal/link"
)

// The whole point of two steps: at no moment is there a client that was
// configured correctly and cannot connect.
//
// This walks the rotation in order and checks the connection at every
// step, which is the only way to test "there is no window".
func TestAKeyIsReplacedWithoutAWindow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	current, err := link.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Before anything: the kernel serves its key and a client pinned to
	// it gets through.
	address := serve(t, current)

	if err := check(t, address, reachingAny(t, current.Pin())); err != nil {
		t.Fatalf("before: %v", err)
	}

	// Step one. The next key exists and is not served.
	next, err := link.Prepare(dir)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if next.Eq(current.Pin()) {
		t.Fatal("prepare handed back the key already in use")
	}

	// Twice is the same key: somebody halfway through handing a pin out
	// should not have it change under them.
	again, err := link.Prepare(dir)
	if err != nil {
		t.Fatalf("prepare again: %v", err)
	}

	if !again.Eq(next) {
		t.Fatalf("a second prepare made another key: %s then %s", next, again)
	}

	// Step two. A client told BOTH still reaches the kernel serving the
	// old one, which is what makes it safe to distribute early.
	if err := check(t, address, reachingAny(t, current.Pin(), next)); err != nil {
		t.Fatalf("during: %v", err)
	}

	// Step three. The kernel commits and restarts.
	committed, err := link.Commit(dir)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if !committed.Eq(next) {
		t.Fatalf("committed %s, prepared %s", committed, next)
	}

	replaced, err := link.Open(dir)
	if err != nil {
		t.Fatalf("open after commit: %v", err)
	}

	if !replaced.Pin().Eq(next) {
		t.Fatalf("after committing, the kernel is %s and not %s", replaced.Pin(), next)
	}

	moved := serve(t, replaced)

	// The client that was told both still gets through — this is the step
	// that would fail without the second pin.
	if err := check(t, moved, reachingAny(t, current.Pin(), next)); err != nil {
		t.Fatalf("after: %v", err)
	}

	// And step four, at leisure: the old pin alone no longer reaches it,
	// which is what makes the rotation a rotation rather than an addition.
	if err := check(t, moved, reachingAny(t, current.Pin())); err == nil {
		t.Fatal("the replaced key still answered to its old pin")
	}
}

// Committing what was never prepared is refused, rather than leaving a
// kernel serving a key it invented at that moment and told nobody about.
func TestCommittingNothingIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	if _, err := link.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := link.Commit(dir); !errors.Is(err, link.ErrNoNextKey) {
		t.Fatalf("want ErrNoNextKey, got %v", err)
	}
}

// A prepared key is visible before it is served, so a person checking can
// see that a rotation is under way.
func TestAPreparedKeyCanBeSeen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	if _, err := link.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, waiting, err := link.Pending(dir); err != nil || waiting {
		t.Fatalf("nothing prepared, and pending says %v (%v)", waiting, err)
	}

	next, err := link.Prepare(dir)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	found, waiting, err := link.Pending(dir)
	if err != nil || !waiting {
		t.Fatalf("prepared, and pending says %v (%v)", waiting, err)
	}

	if !found.Eq(next) {
		t.Fatalf("pending is %s, prepared %s", found, next)
	}
}

func reachingAny(t *testing.T, pinned ...link.Pin) grpc.DialOption {
	t.Helper()

	creds, err := link.Reaching(pinned...)
	if err != nil {
		t.Fatalf("reaching: %v", err)
	}

	return grpc.WithTransportCredentials(creds)
}
