package services

import (
	"bytes"
	"testing"
)

// An edit must never escape the pipeline, and a tree must survive a
// pack/unpack round trip byte for byte.
func TestCleanPath(t *testing.T) {
	for _, p := range []string{"", ".", "/", "/etc/passwd", "../secrets", "a/../../b"} {
		if _, err := cleanPath(p); err == nil {
			t.Fatalf("path %q must be refused", p)
		}
	}
	for in, want := range map[string]string{
		"main.go": "main.go", "./main.go": "main.go",
		"internal/x/y.go": "internal/x/y.go", "a/./b.go": "a/b.go",
	} {
		got, err := cleanPath(in)
		if err != nil {
			t.Fatalf("path %q must be accepted: %v", in, err)
		}
		if got != want {
			t.Fatalf("path %q cleaned to %q, want %q", in, got, want)
		}
	}
}

func TestTreeRoundTrip(t *testing.T) {
	tree := map[string][]byte{
		"main.go":         []byte("package main\n"),
		"go.mod":          []byte("module x\n"),
		"internal/a/b.go": []byte("package a\n"),
	}
	packed, err := packTree(tree)
	if err != nil {
		t.Fatal(err)
	}
	back, err := unpackTree(bytes.NewReader(packed))
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
	// The same tree must pack identically — the digest is the identity
	// of a revision.
	again, err := packTree(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packed, again) {
		t.Fatal("packing is not deterministic: the same tree would build twice")
	}
}
