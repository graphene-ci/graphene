// Package link is the port for obtaining a byte pipe to the control
// kernel. HOW the pipe comes to be — a TLS dial-out, the stdin/stdout of
// an ssh-spawned process, a relay chain — is an infrastructure concern;
// everything above (gRPC client, watches, receipts, blobs) sees only a
// net.Conn factory and never learns the topology (R8–R9).
package link

import (
	"context"
	"net"
)

// Link produces live byte pipes to the control kernel.
//
// Every Dial call returns a NEW pipe: reconnecting clients simply dial
// again. Implementations backed by a single inherently-unique channel
// (a process's own stdio) return the pipe once and fail afterwards — the
// death of such a pipe is the death of the session by design.
type Link interface {
	Dial(ctx context.Context) (net.Conn, error)
}
