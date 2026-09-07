//go:build !windows

package agentcmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/term"
)

// watchResize forwards the terminal size to the remote pty on every
// window-change signal (and once up front) until ctx ends.
func watchResize(ctx context.Context, fd int, send func(cols, rows uint32)) {
	report := func() {
		if w, h, err := term.GetSize(fd); err == nil {
			send(uint32(w), uint32(h)) //nolint:gosec // terminal sizes are small
		}
	}
	report()
	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	go func() {
		defer signal.Stop(winch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-winch:
				report()
			}
		}
	}()
}
