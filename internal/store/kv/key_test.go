package kv_test

import (
	"testing"

	"github.com/graphene-ci/graphene/internal/store/kv"
)

// Prefix matching is what a scan, a watch and a grant are each built out
// of, so it lives on the type rather than being written out at every
// store that implements the port.
func TestPrefixIsAskedOfTheKeyItself(t *testing.T) {
	t.Parallel()

	under := kv.Key("Process\x1eacme\x1fweb\x1f")

	if !under.HasPrefix(kv.Key("Process\x1eacme\x1f")) {
		t.Fatal("a key is not under its own prefix")
	}

	if !under.HasPrefix(under) {
		t.Fatal("a key is not under itself")
	}

	// The empty key is not "no prefix", it is every prefix.
	if !under.HasPrefix(kv.Key("")) || !kv.Key("").IsEmpty() {
		t.Fatal("the empty key stopped covering everything")
	}
}

// A key handed back by a store points into memory the store owns — a
// bbolt page is only valid for the life of its read transaction. The bug
// this prevents does not look like one: the key is right when it is read
// and wrong later.
func TestCloningAKeyDoesNotAliasTheOriginal(t *testing.T) {
	t.Parallel()

	original := kv.Key("Process\x1eacme\x1f")
	copied := original.Clone()

	original[0] = 'X'

	if copied.Equal(original) || copied[0] != 'P' {
		t.Fatalf("the copy followed the original: %s", copied)
	}
}

// A key printed raw loses its separators, and "Process␞acme" would read
// the same as "Processacme" in the one message somebody is trying to
// understand.
func TestPrintingAKeyShowsItsSeparators(t *testing.T) {
	t.Parallel()

	got := kv.Key("Process\x1eacme\x1f").String()

	if got != `"Process\x1eacme\x1f"` {
		t.Fatalf("printed as %s", got)
	}
}
