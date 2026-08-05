package kv_test

import (
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
)

// The value is the larger half and the one a caller keeps, so it is the
// one that hurts when a store hands back the page it read from.
func TestCloningAnEntryDoesNotAliasTheOriginal(t *testing.T) {
	t.Parallel()

	entry := kv.Entry{Key: kv.Key("k"), Value: []byte("v"), Revision: 1}
	held := entry.Clone()

	entry.Key[0] = 'X'
	entry.Value[0] = 'X'

	if held.Key.Equal(entry.Key) || held.Value[0] != 'v' {
		t.Fatalf("the copy followed the original: %s", held)
	}
}

// A zero entry is one nobody wrote: every stored record has a revision.
func TestAZeroEntryIsOneNobodyWrote(t *testing.T) {
	t.Parallel()

	var none kv.Entry

	if !none.IsZero() {
		t.Fatal("the zero entry claimed to have been stored")
	}

	if (kv.Entry{Key: kv.Key("k"), Revision: 1}).IsZero() {
		t.Fatal("a stored entry claimed it was never written")
	}
}
