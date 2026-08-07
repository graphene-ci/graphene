package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/graphene-ci/graphene/internal/app/server"
)

// ErrNotRunning — there is no kernel on this machine to relay to.
//
// A separate answer from "the socket refused me", because it is a
// different thing to do about it: start the kernel.
var ErrNotRunning = errors.New("no kernel is running on this machine")

// Stdio relays this process's pipes to the kernel running on this
// machine.
//
// IT OPENS NOTHING. An earlier version of this opened the store and
// served a whole kernel on the pipe, which cannot work for the case it
// exists for: `ssh host graphened stdio` is aimed at a machine where a
// kernel is ALREADY running, and one store is one process — the second
// one waits for a lock it will never get. It stopped, silently, forever.
//
// So it is a relay. What crosses the pipe is what would have crossed a
// socket, byte for byte, and everything that decides anything — who the
// caller is, what they may do — happens in the kernel at the other end,
// exactly as it does over a port.
func Stdio(ctx context.Context, dir string, in io.Reader, out io.Writer) error {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "unix", server.Socket(dir))
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrNotRunning, server.Socket(dir), err)
	}

	defer func() { _ = conn.Close() }()

	// Two copies, and the FIRST one to end ends both. A relay that waited
	// for the other direction would hang on a client that stopped talking
	// and is waiting to be told the conversation is over. The channel
	// holds both so the copy that loses the race is not left blocked on
	// one nobody reads.
	const directions = 2

	done := make(chan error, directions)

	go func() { _, err := io.Copy(conn, in); done <- err }()
	go func() { _, err := io.Copy(out, conn); done <- err }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-done:
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("relay: %w", err)
		}

		return nil
	}
}
