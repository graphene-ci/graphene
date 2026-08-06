package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/app/config"
)

// An upstream configuration survives being written down, credential and
// all — the file is where the credential lives, so a round trip that lost
// it would produce a kernel refused everything it asks for.
func TestAnUpstreamSurvivesBeingWrittenDown(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "kernel.yaml")

	original, err := config.NewUpstream("edge", "0.0.0.0:9999", "above:7373", "edge.s3cret", "/var/lib/edge", examplePin)
	if err != nil {
		t.Fatalf("upstream: %v", err)
	}

	if err := config.Write(at, original); err != nil {
		t.Fatalf("write: %v", err)
	}

	back, err := config.Read(at)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if !back.Eq(original) {
		t.Fatalf("%s came back as %s", original, back)
	}

	up, forwards := back.Upstream()
	if !forwards {
		t.Fatal("a file with an upstream section came back as a store")
	}

	if _, keeps := back.Local(); keeps {
		t.Fatal("a subordinate came back keeping a store as well")
	}

	if up.Token() != "edge.s3cret" {
		t.Fatalf("the credential came back as %q", up.Token())
	}
}

// A kernel is one thing or the other, and a file saying both is refused
// rather than resolved by precedence.
//
// A precedence rule is a silent answer to a question somebody got wrong,
// and the wrong half of this one is a kernel quietly storing locally what
// it meant to forward — which nobody notices until they look for the data
// somewhere else.
func TestAFileCannotDescribeBothKernels(t *testing.T) {
	t.Parallel()

	at := filepath.Join(t.TempDir(), "kernel.yaml")

	both := "" +
		"name: confused\n" +
		"store:\n  path: /tmp/k.db\n" +
		"upstream:\n  address: above:7373\n  token: a.b\n"

	if err := os.WriteFile(at, []byte(both), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := config.Read(at); !errors.Is(err, config.ErrTwoModes) {
		t.Fatalf("want ErrTwoModes, got %v", err)
	}
}

// An upstream with a piece missing is refused, and neither piece has a
// default worth guessing: an address nobody gave is a kernel talking to
// itself, and a credential nobody gave is a kernel refused everything.
func TestAnUpstreamNeedsBothHalves(t *testing.T) {
	t.Parallel()

	refused := map[string]struct {
		written string
		want    error
	}{
		"no address": {"upstream:\n  token: a.b\n", config.ErrNoAddress},
		"no token":   {"upstream:\n  address: above:7373\n", config.ErrNoToken},
	}

	for name, one := range refused {
		at := filepath.Join(t.TempDir(), "kernel.yaml")

		if err := os.WriteFile(at, []byte(one.written), 0o600); err != nil {
			t.Fatalf("%s: write: %v", name, err)
		}

		if _, err := config.Read(at); !errors.Is(err, one.want) {
			t.Fatalf("%s: want %v, got %v", name, one.want, err)
		}
	}
}

// A credential does not go where a configuration is printed.
//
// This string ends up in logs and inside error text, and a token that
// reached either of those is a token that has to be changed.
func TestPrintingAConfigDoesNotPrintTheToken(t *testing.T) {
	t.Parallel()

	forwarding, err := config.NewUpstream("edge", "", "above:7373", "edge.s3cret", "/var/lib/edge", examplePin)
	if err != nil {
		t.Fatalf("upstream: %v", err)
	}

	if strings.Contains(forwarding.String(), "s3cret") {
		t.Fatalf("the credential is in %q", forwarding.String())
	}
}

// Only the address takes effect while a kernel runs.
//
// A store cannot slide out from under one using it, a cache is sized when
// it is built, and a connection is made once — so everything else
// describes the next start, and applying it now would make the kernel
// report a configuration it is not running.
func TestOnlyTheAddressMoves(t *testing.T) {
	t.Parallel()

	running := config.NewLocal("local", "127.0.0.1:1", "/tmp/one.db", 8, "")
	moved := running.At("127.0.0.1:2")

	if moved.Listen() != "127.0.0.1:2" {
		t.Fatalf("the address stayed at %s", moved.Listen())
	}

	local, keeps := moved.Local()
	if !keeps || local.Store() != "/tmp/one.db" || local.Cache() != 8 {
		t.Fatalf("moving the address changed the store: %s", moved)
	}
}

// examplePin is a pin's shape, which is all these tests need: they are
// about contexts and configuration, not about what a key hashes to.
const examplePin = "sha256:" +
	"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
