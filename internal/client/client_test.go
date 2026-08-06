package client_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/client"
	"github.com/graphene-ci/graphene/internal/link"
)

// Contexts survive being written down, and the first one saved is the one
// commands mean.
//
// The first one, because somebody who has saved exactly one kernel meant
// that kernel — and leaving it unselected would make the next command
// fail for a reason that reads like a bug.
func TestContextsSurviveBeingWrittenDown(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "contexts.yaml")

	all, err := client.Read(at)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	first, err := client.NewContext("local", "127.0.0.1:7373", "root.a", examplePin)
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	second, err := client.NewContext("prod", "kernel.prod:7373", "ci.b", examplePin)
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	if err := all.Save(first); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := all.Save(second); err != nil {
		t.Fatalf("save: %v", err)
	}

	back, err := client.Read(at)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if len(back.All()) != 2 {
		t.Fatalf("came back as %v", back.All())
	}

	current, err := back.Current()
	if err != nil {
		t.Fatalf("current: %v", err)
	}

	if current.Name() != "local" || current.Token() != "root.a" {
		t.Fatalf("the current kernel is %s", current)
	}

	if err := back.Use("prod"); err != nil {
		t.Fatalf("use: %v", err)
	}

	moved, err := client.Read(at)
	if err != nil {
		t.Fatalf("read again: %v", err)
	}

	if current, _ := moved.Current(); current.Name() != "prod" {
		t.Fatalf("use did not stick: %s", current)
	}
}

// Forgetting the kernel in use leaves NONE in use.
//
// Rather than quietly promoting another one: the next command would then
// go somewhere nobody named, which is the one mistake this file exists to
// prevent.
func TestForgettingTheCurrentKernelLeavesNone(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "contexts.yaml")

	all, err := client.Read(at)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	for _, one := range []struct{ name, address string }{
		{"local", "127.0.0.1:7373"},
		{"prod", "kernel.prod:7373"},
	} {
		saved, err := client.NewContext(one.name, one.address, "t.s", examplePin)
		if err != nil {
			t.Fatalf("context: %v", err)
		}

		if err := all.Save(saved); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	if err := all.Forget("local"); err != nil {
		t.Fatalf("forget: %v", err)
	}

	if _, err := all.Current(); !errors.Is(err, client.ErrNoContext) {
		t.Fatalf("want ErrNoContext, got %v", err)
	}
}

// A credential is not in what gets printed.
func TestPrintingAContextDoesNotPrintTheToken(t *testing.T) {
	t.Parallel()

	one, err := client.NewContext("local", "127.0.0.1:7373", "root.s3cret", examplePin)
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	if strings.Contains(one.String(), "s3cret") {
		t.Fatalf("the credential is in %q", one)
	}
}

// A client with nothing saved finds the kernel on THIS machine, and saves
// what it found.
//
// Discovery rather than shared configuration: the two files stay
// separate, but on the machine that runs a kernel both halves are already
// there, so making somebody copy them would be ceremony.
func TestAClientFindsTheKernelOnThisMachine(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kernelFile := filepath.Join(dir, "kernel.yaml")
	contexts := filepath.Join(dir, "contexts.yaml")

	if err := config.Write(kernelFile, config.NewLocal(
		"top", "127.0.0.1:9999", filepath.Join(dir, "kernel.db"), 8, "root.s3cret",
	)); err != nil {
		t.Fatalf("write kernel: %v", err)
	}

	// What a kernel makes at its first start. Discovery reads the pin off
	// it rather than asking a person to retype a fingerprint they could
	// have read from the file beside them.
	running, err := link.Open(dir)
	if err != nil {
		t.Fatalf("kernel key: %v", err)
	}

	all, err := client.Read(contexts)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	found, err := client.Reach(all, "", kernelFile)
	if err != nil {
		t.Fatalf("reach: %v", err)
	}

	if found.Address() != "127.0.0.1:9999" || found.Token() != "root.s3cret" {
		t.Fatalf("found %s with %q", found, found.Token())
	}

	if !found.Pin().Eq(running.Pin()) {
		t.Fatalf("discovered the pin %s, the kernel's is %s", found.Pin(), running.Pin())
	}

	// SAVED, so it is discovered once and is an ordinary context after
	// that — visible, replaceable, and not a magic answer that changes
	// under somebody when they edit the kernel's file.
	back, err := client.Read(contexts)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if current, err := back.Current(); err != nil || current.Address() != "127.0.0.1:9999" {
		t.Fatalf("the discovered kernel was not saved: %v %s", err, current)
	}
}

// A kernel that FORWARDS is not discovered.
//
// It has no identities of its own, and the credential in its file is its
// own rather than one that would let a person do anything. Whoever wants
// that kernel names it themselves.
func TestASubordinateIsNotDiscovered(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kernelFile := filepath.Join(dir, "kernel.yaml")

	forwarding, err := config.NewUpstream("edge", "127.0.0.1:9999", "above:7373", "edge.s", t.TempDir(), examplePin)
	if err != nil {
		t.Fatalf("upstream: %v", err)
	}

	if err := config.Write(kernelFile, forwarding); err != nil {
		t.Fatalf("write kernel: %v", err)
	}

	all, err := client.Read(filepath.Join(dir, "contexts.yaml"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if _, err := client.Reach(all, "", kernelFile); !errors.Is(err, client.ErrNoContext) {
		t.Fatalf("want ErrNoContext, got %v", err)
	}
}

// The file is written 0600: it is nothing but credentials.
func TestTheContextsFileIsPrivate(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "contexts.yaml")

	all, err := client.Read(at)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	one, err := client.NewContext("local", "127.0.0.1:7373", "root.a", examplePin)
	if err != nil {
		t.Fatalf("context: %v", err)
	}

	if err := all.Save(one); err != nil {
		t.Fatalf("save: %v", err)
	}

	written, err := os.Stat(at)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if written.Mode().Perm() != 0o600 {
		t.Fatalf("the file is %v", written.Mode().Perm())
	}
}

// examplePin is a pin's shape, which is all these tests need: they are
// about contexts and configuration, not about what a key hashes to.
const examplePin = "sha256:" +
	"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
