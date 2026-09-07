//go:build windows

package agentcmd

import "context"

// watchResize is a no-op on Windows, which has no window-change signal; the
// remote pty keeps its initial size.
func watchResize(_ context.Context, _ int, _ func(cols, rows uint32)) {}
