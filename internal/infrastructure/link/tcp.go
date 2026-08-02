package link

import (
	"context"
	"fmt"
	"net"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
)

// TCP is the dial-out link: a plain TCP pipe to the control kernel's
// endpoint. TLS is NOT done here — the gRPC layer performs the handshake
// through whatever pipe the link yields (which is exactly what makes the
// same handshake work end-to-end through relays).
func TCP(addr string) corelink.Link {
	return tcpLink{addr: addr}
}

type tcpLink struct {
	addr string
}

func (l tcpLink) Dial(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", l.addr)
	if err != nil {
		return nil, fmt.Errorf("link: tcp dial %s: %w", l.addr, err)
	}

	return conn, nil
}
