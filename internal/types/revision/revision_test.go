package revision_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/graphene-ci/graphene/internal/types/revision"
)

// The three names of the zero are one value. This is the whole of the
// decision to keep one type instead of three, so it is written down where
// it will be noticed if somebody changes it.
func TestTheThreeZerosAreOneValue(t *testing.T) {
	t.Parallel()

	if revision.Absent != revision.None {
		t.Fatal("Absent is no longer the zero revision")
	}

	if revision.Beginning != revision.None {
		t.Fatal("Beginning is no longer the zero revision")
	}

	if !revision.None.IsZero() {
		t.Fatal("the zero revision did not report itself as zero")
	}

	var unset revision.Revision
	if unset != revision.None {
		t.Fatal("the zero value of the type is not None")
	}
}

// A revision that came out of the store is never zero: the first write
// stamps 1. Next is the only arithmetic there is, and this is what it is
// for.
func TestTheFirstWriteIsOne(t *testing.T) {
	t.Parallel()

	first := revision.None.Next()

	if first != 1 || first.IsZero() {
		t.Fatalf("first write stamped %s", first)
	}

	if first.Next() != 2 {
		t.Fatalf("second write stamped %s", first.Next())
	}
}

// Order is what a cursor and a compaction boundary are both read with.
func TestOrder(t *testing.T) {
	t.Parallel()

	older, newer := revision.Revision(7), revision.Revision(9)

	if !newer.After(older) || newer.Before(older) {
		t.Fatal("9 did not come after 7")
	}

	if !older.Before(newer) || older.After(newer) {
		t.Fatal("7 did not come before 9")
	}

	if older.After(older) || older.Before(older) {
		t.Fatal("a revision came before or after itself")
	}

	if !older.Eq(7) || older.Eq(newer) {
		t.Fatal("equality disagrees with the number")
	}
}

// Parse reads back what String wrote, and refuses anything else rather
// than guessing. A revision read wrong is a write against the wrong
// generation of a record.
func TestParseIsTheInverseOfString(t *testing.T) {
	t.Parallel()

	for _, want := range []revision.Revision{revision.None, 1, 42, 1 << 62} {
		got, err := revision.Parse(want.String())
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}

		if got != want {
			t.Fatalf("%s came back as %s", want, got)
		}
	}

	refused := []string{
		"", "r42", "42 ", " 42", "-1", "4.2", "0x2a", "42abc",
		strconv.FormatUint(1<<63, 10) + "0",
	}

	for _, raw := range refused {
		if _, err := revision.Parse(raw); !errors.Is(err, revision.ErrMalformed) {
			t.Fatalf("%q: want ErrMalformed, got %v", raw, err)
		}
	}
}

// A revision is read next to other revisions, so it prints as the bare
// number and nothing else.
func TestPrintsAsABareNumber(t *testing.T) {
	t.Parallel()

	if got := revision.Revision(42).String(); got != "42" {
		t.Fatalf("printed as %q", got)
	}

	if got := revision.Revision(42).Uint64(); got != 42 {
		t.Fatalf("uint64 gave %d", got)
	}
}
