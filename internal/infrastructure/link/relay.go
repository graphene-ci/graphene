package link

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
)

// The relay is a dumb byte forwarder with a token gate. It never parses
// what it forwards: the worker's TLS handshake with the control kernel
// happens THROUGH it, end to end — a relay sees ciphertext only. The
// relay token is a coarse gate against port scanners, not a crypto
// boundary.
//
// Wire preamble (client → relay, before any forwarded byte):
//
//	RELAY1 <token>\n
//
// relay → client: "OK\n" and the pipe turns transparent, or the
// connection is closed.
const (
	relayHello = "RELAY1 "
	relayOK    = "OK\n"

	preambleLimit   = 512
	preambleTimeout = 10 * time.Second
)

var (
	// ErrRelayRejected — the relay refused the token.
	ErrRelayRejected = errors.New("link: relay rejected the token")
	errBadPreamble   = errors.New("link: bad relay preamble")
)

// Via is the client side: dial the relay, pass the gate, hand the
// transparent pipe up. Chains compose server-side: every relay knows only
// its own upstream link — the client knows only the first hop.
func Via(addr, token string) corelink.Link {
	return viaLink{addr: addr, token: token}
}

type viaLink struct {
	addr  string
	token string
}

func (l viaLink) Dial(ctx context.Context) (net.Conn, error) {
	conn, err := TCP(l.addr).Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("link: via %s: %w", l.addr, err)
	}

	if err := l.gate(conn); err != nil {
		_ = conn.Close()

		return nil, err
	}

	return conn, nil
}

func (l viaLink) gate(conn net.Conn) error {
	_ = conn.SetDeadline(time.Now().Add(preambleTimeout))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	if _, err := io.WriteString(conn, relayHello+l.token+"\n"); err != nil {
		return fmt.Errorf("link: relay hello: %w", err)
	}

	reply := make([]byte, len(relayOK))
	if _, err := io.ReadFull(conn, reply); err != nil {
		return fmt.Errorf("link: relay reply: %w", err)
	}

	if string(reply) != relayOK {
		return ErrRelayRejected
	}

	return nil
}

// ServeRelay runs the relay accept loop until ctx is done or the listener
// fails: gate by token, dial the upstream link, pump bytes both ways.
func ServeRelay(ctx context.Context, lis net.Listener, upstream corelink.Link, token string) error {
	go func() {
		<-ctx.Done()

		_ = lis.Close()
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // shutdown via context is a clean exit
			}

			return fmt.Errorf("link: relay accept: %w", err)
		}

		go serveRelayConn(ctx, conn, upstream, token)
	}
}

func serveRelayConn(ctx context.Context, conn net.Conn, upstream corelink.Link, token string) {
	defer func() { _ = conn.Close() }()

	if err := readGate(conn, token); err != nil {
		return // silent close: no oracle for scanners
	}

	upConn, err := upstream.Dial(ctx)
	if err != nil {
		return
	}

	defer func() { _ = upConn.Close() }()

	if _, err := io.WriteString(conn, relayOK); err != nil {
		return
	}

	pump(conn, upConn)
}

func readGate(conn net.Conn, token string) error {
	_ = conn.SetDeadline(time.Now().Add(preambleTimeout))
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	line, err := bufio.NewReaderSize(conn, preambleLimit).ReadString('\n')
	if err != nil {
		return fmt.Errorf("link: relay gate read: %w", err)
	}

	if !strings.HasPrefix(line, relayHello) || strings.TrimSuffix(strings.TrimPrefix(line, relayHello), "\n") != token {
		return errBadPreamble
	}

	return nil
}

// pump copies both directions until either side closes.
//
// NOTE: the gate reader above is buffered; by protocol the client sends
// nothing after the preamble until it receives OK, so the buffer holds no
// stray bytes when pumping starts.
func pump(a, b net.Conn) {
	done := make(chan struct{}, 2) //nolint:mnd // two directions

	copyDir := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()

		done <- struct{}{}
	}

	go copyDir(a, b)
	go copyDir(b, a)

	<-done
	<-done
}
