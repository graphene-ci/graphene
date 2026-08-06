package daemon_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/app"
	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/app/daemon"
)

// A kernel that cannot start ENDS, and says why.
//
// The service library's own wait blocks on a signal and nothing else, so
// a kernel that failed to come up left a process sitting there having
// done nothing at all, with the failure on a goroutine nobody was
// reading — found by running one, not by reading the code. The wait is
// this program's now, and it watches for either.
func TestAKernelThatCannotStartDoesNotHang(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	at := filepath.Join(dir, "kernel.yaml")

	// A store under a path that is a FILE: the directory cannot be made,
	// so opening it fails, which is the ordinary shape of "this kernel
	// was configured with somewhere it may not write".
	blocked := filepath.Join(dir, "occupied")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("occupy: %v", err)
	}

	if err := config.Write(at, config.NewLocal("local", "127.0.0.1:0",
		filepath.Join(blocked, "kernel.db"), 8, "")); err != nil {
		t.Fatalf("write config: %v", err)
	}

	running, err := daemon.New(app.Bootstrap{Config: at, Version: "test"}, discard())
	if err != nil {
		t.Fatalf("describe: %v", err)
	}

	ended := make(chan error, 1)

	go func() { ended <- running.Run() }()

	select {
	case err := <-ended:
		if err == nil {
			t.Fatal("a kernel that could not open its store reported success")
		}

	case <-time.After(10 * time.Second):
		t.Fatal("a kernel that could not start never stopped")
	}
}

func discard() *xlog.Logger {
	if os.Getenv("LOUD") != "" {
		return xlog.NewConsole(xlog.WithWriter(os.Stderr))
	}

	return xlog.New(xlog.NopCore{})
}
