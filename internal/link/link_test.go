package link_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/graphene-ci/graphene/internal/link"
)

// Key material is made once and kept. A kernel that minted a new key address
// every start would be a different kernel every start, and everything
// pointing address it would stop.
func TestKeyMaterialIsMadeOnceAndKept(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	first, err := link.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	again, err := link.Open(dir)
	if err != nil {
		t.Fatalf("open again: %v", err)
	}

	if !first.Pin().Eq(again.Pin()) {
		t.Fatalf("a second start became a different kernel: %s then %s", first.Pin(), again.Pin())
	}

	// And a client on the same machine can read the pin without making
	// any, which is what keeps discovery from being a copied fingerprint.
	read, err := link.PinIn(dir)
	if err != nil {
		t.Fatalf("read the pin: %v", err)
	}

	if !read.Eq(first.Pin()) {
		t.Fatalf("the pin read from disk is %s, the kernel's is %s", read, first.Pin())
	}
}

// The key never leaves the machine, so what protects it is the file mode.
func TestTheKeyIsReadableOnlyByItsOwner(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	if _, err := link.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "link.key"))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("the key is mode %o", mode)
	}
}

// Two kernels are two pins. Otherwise pinning would say nothing.
func TestTwoKernelsAreTwoPins(t *testing.T) {
	t.Parallel()

	one, err := link.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	other, err := link.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if one.Pin().Eq(other.Pin()) {
		t.Fatal("two kernels got the same pin")
	}
}

// The whole point, end to end: a client that was told the right pin gets
// through, and one that was told a different kernel's does not.
func TestOnlyTheKernelThatWasPinnedAnswers(t *testing.T) {
	t.Parallel()

	serving, err := link.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	somebodyElse, err := link.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	address := serve(t, serving)

	if err := check(t, address, reaching(t, serving.Pin())); err != nil {
		t.Fatalf("the kernel it was told about: %v", err)
	}

	// A different kernel's pin. This is what an attacker who answers address
	// the right address looks like from here.
	if err := check(t, address, reaching(t, somebodyElse.Pin())); err == nil {
		t.Fatal("a kernel nobody pinned answered anyway")
	}
}

// A plaintext client finds a TLS listener and fails. There is no
// negotiation and no fallback: a switch that turned this off would be
// found and turned off.
func TestAPlaintextClientCannotGetIn(t *testing.T) {
	t.Parallel()

	serving, err := link.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	plaintext := grpc.WithTransportCredentials(insecure.NewCredentials())
	if err := check(t, serve(t, serving), plaintext); err == nil {
		t.Fatal("a plaintext client got an answer")
	}
}

// Reaching a kernel without being told which one is refused rather than
// remembered: the first connection is the one worth being in the middle
// of.
func TestReachingWithoutAPinIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := link.Reaching(""); !errors.Is(err, link.ErrNoPin) {
		t.Fatalf("want ErrNoPin, got %v", err)
	}
}

// A pin is checked where it is read, not where it is used.
func TestAPinIsCheckedWhereItIsRead(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"",
		"sha256:",
		"deadbeef",
		"sha256:" + "zz" + "00000000000000000000000000000000000000000000000000000000000000",
		"md5:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	} {
		if _, err := link.NewPin(raw); err == nil {
			t.Fatalf("%q was taken for a pin", raw)
		}
	}

	good, err := link.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	read, err := link.NewPin(good.Pin().String())
	if err != nil {
		t.Fatalf("a pin this package wrote: %v", err)
	}

	if !read.Eq(good.Pin()) {
		t.Fatalf("a pin changed on the way through: %s", read)
	}
}

// serve stands a TLS listener up and hands back its address.
func serve(t *testing.T, identity link.Identity) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer(grpc.Creds(identity.Serving()))
	hv1.RegisterHealthServer(server, health.NewServer())

	go func() { _ = server.Serve(listener) }()

	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

func reaching(t *testing.T, pinned link.Pin) grpc.DialOption {
	t.Helper()

	creds, err := link.Reaching(pinned)
	if err != nil {
		t.Fatalf("reaching: %v", err)
	}

	return grpc.WithTransportCredentials(creds)
}

// check makes one call, which is what actually opens the connection.
func check(t *testing.T, address string, option grpc.DialOption) error {
	t.Helper()

	conn, err := grpc.NewClient(address, option)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = hv1.NewHealthClient(conn).Check(ctx, &hv1.HealthCheckRequest{})

	return err
}
