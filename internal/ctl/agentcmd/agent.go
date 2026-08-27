// Package agentcmd is the operator's hand on a machine: `agent shell`
// opens an interactive terminal on the box whose agent is connected —
// through the door, no ssh reachability, the caller's own token and
// audit. The wire is the browser-shaped half-duplex pair (a server
// stream of output, unary input frames), so the CLI and Studio speak
// the exact same door.
package agentcmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// New builds the agent command group.
func New(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Machines: an interactive shell on the box, over the door",
	}
	cmd.AddCommand(newShell(f))
	return cmd
}

func newShell(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:   "shell <agent-id>",
		Short: "Open an interactive shell on the machine",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			return shell(cmd.Context(), d, args[0])
		},
	}
}

func shell(ctx context.Context, d *cmdutil.Door, agent string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cols, rows := uint32(80), uint32(24)
	fd := int(os.Stdin.Fd())
	interactive := term.IsTerminal(fd)
	if interactive {
		if w, h, err := term.GetSize(fd); err == nil {
			cols, rows = uint32(w), uint32(h) //nolint:gosec // terminal sizes are small
		}
	}

	stream, err := d.Agents.Pty(ctx, connect.NewRequest(&managementv1.PtyRequest{
		Agent: agent, Cols: cols, Rows: rows,
	}))
	if err != nil {
		return err
	}
	if !stream.Receive() {
		return fmt.Errorf("shell did not open: %w", stream.Err())
	}
	opened := stream.Msg().GetOpened()
	if opened == nil {
		return fmt.Errorf("the first chunk was not the session id")
	}
	sessionId := opened.GetSessionId()

	// Raw mode: the REMOTE pty owns echo and line discipline; local
	// keystrokes travel as-is, Ctrl-C included.
	if interactive {
		state, err := term.MakeRaw(fd)
		if err != nil {
			return err
		}
		defer func() { _ = term.Restore(fd, state) }()
	}

	// Keystrokes: one unary frame per read burst.
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				if _, err := d.Agents.PtyInput(ctx, connect.NewRequest(&managementv1.PtyInputRequest{
					SessionId: sessionId,
					Body:      &managementv1.PtyInputRequest_Data{Data: data},
				})); err != nil {
					cancel()
					return
				}
			}
			if err != nil {
				// stdin ended: bury the shell explicitly.
				_, _ = d.Agents.PtyInput(context.WithoutCancel(ctx), connect.NewRequest(&managementv1.PtyInputRequest{
					SessionId: sessionId,
					Body:      &managementv1.PtyInputRequest_Close{Close: true},
				}))
				cancel()
				return
			}
		}
	}()

	// The window follows the viewer.
	if interactive {
		winch := make(chan os.Signal, 1)
		signal.Notify(winch, syscall.SIGWINCH)
		defer signal.Stop(winch)
		go func() {
			for range winch {
				if w, h, err := term.GetSize(fd); err == nil {
					_, _ = d.Agents.PtyInput(ctx, connect.NewRequest(&managementv1.PtyInputRequest{
						SessionId: sessionId,
						Body: &managementv1.PtyInputRequest_Resize_{Resize: &managementv1.PtyInputRequest_Resize{
							Cols: uint32(w), Rows: uint32(h), //nolint:gosec // terminal sizes are small
						}},
					}))
				}
			}
		}()
	}

	for stream.Receive() {
		msg := stream.Msg()
		if data := msg.GetData(); len(data) > 0 {
			_, _ = os.Stdout.Write(data)
		}
		if closed := msg.GetClosed(); closed != nil {
			if closed.GetMessage() != "" {
				fmt.Fprintf(os.Stderr, "\r\nshell closed: %s\r\n", closed.GetMessage())
			}
			return nil
		}
	}
	if err := stream.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
