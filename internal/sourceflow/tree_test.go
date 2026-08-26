package sourceflow

import (
	"bytes"
	"testing"
)

// An edit must never escape the tree.
func TestCleanPath(t *testing.T) {
	for _, p := range []string{"", ".", "/", "/etc/passwd", "../secrets", "a/../../b"} {
		if _, err := CleanPath(p); err == nil {
			t.Fatalf("path %q must be refused", p)
		}
	}
	for in, want := range map[string]string{
		"main.go": "main.go", "./main.go": "main.go",
		"internal/x/y.go": "internal/x/y.go", "a/./b.go": "a/b.go",
	} {
		got, err := CleanPath(in)
		if err != nil {
			t.Fatalf("path %q must be accepted: %v", in, err)
		}
		if got != want {
			t.Fatalf("path %q cleaned to %q, want %q", in, got, want)
		}
	}
}

// A tree survives the archive round trip byte for byte, and the same
// tree packs identically — the digest IS the identity of a revision,
// so a rebuild of unchanged files must deduplicate.
func TestTreeRoundTrip(t *testing.T) {
	tree := map[string][]byte{
		"main.go":         []byte("package main\n"),
		"go.mod":          []byte("module x\n"),
		"internal/a/b.go": []byte("package a\n"),
	}
	packed, err := PackTar(tree)
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnpackTar(packed, maxTestFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(tree) {
		t.Fatalf("round trip lost files: %d -> %d", len(tree), len(back))
	}
	for p, want := range tree {
		if !bytes.Equal(back[p], want) {
			t.Fatalf("file %q changed: %q", p, back[p])
		}
	}
	again, err := PackTar(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packed, again) {
		t.Fatal("packing is not deterministic: the same tree would build twice")
	}
}

// The index is the tree's identity: the same files must render the
// same bytes whatever order they were written in, or every write would
// look like a change.
func TestIndexDeterministic(t *testing.T) {
	a := Index{
		"main.go": {Blob: "b1", Size: 3, Digest: "sha256:aa"},
		"go.mod":  {Blob: "b2", Size: 4, Digest: "sha256:bb"},
	}
	b := Index{
		"go.mod":  {Blob: "b2", Size: 4, Digest: "sha256:bb"},
		"main.go": {Blob: "b1", Size: 3, Digest: "sha256:aa"},
	}
	da, raw, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	db, _, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if da != db {
		t.Fatalf("same tree, different digests: %s vs %s", da, db)
	}
	back, err := ParseIndex(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 2 || back["main.go"].Blob != "b1" {
		t.Fatalf("index did not survive the round trip: %#v", back)
	}
	// A changed file changes the tree's digest — that is what makes a
	// revision of an edited tree a different revision.
	a["main.go"] = Entry{Blob: "b3", Size: 5, Digest: "sha256:cc"}
	dc, _, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if dc == da {
		t.Fatal("an edited file left the tree digest unchanged")
	}
}

const maxTestFile = 1 << 20
