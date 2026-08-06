package link

import (
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// A RESUMED session is checked too, and this test is the reason the check
// lives where it does.
//
// TLS 1.3 lets a client skip the full handshake on a second connection by
// presenting a ticket from the first. VerifyPeerCertificate — the obvious
// place for a custom check, and where this one was first written — does
// not run on those. A client that had once connected would keep
// connecting, to whoever answered, without ever asking again which kernel
// it was.
func TestAResumedSessionIsStillChecked(t *testing.T) {
	t.Parallel()

	serving, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	somebodyElse, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	address := listen(t, serving)

	// The first connection is a full handshake and hands out a ticket.
	// Reading is what makes the client take it: the ticket arrives after
	// the handshake, so a client that never read would never have one.
	cache := tls.NewLRUClientSessionCache(4)

	if resumed := connect(t, address, reaching(serving.Pin()), cache); resumed {
		t.Fatal("the first connection resumed something")
	}

	// The second one uses it — which is the state that used to skip the
	// check.
	if resumed := connect(t, address, reaching(serving.Pin()), cache); !resumed {
		t.Skip("this TLS stack did not resume; the test has nothing to say")
	}

	// And now the same cache, with the wrong kernel pinned. If the check
	// only ran on a full handshake, this would succeed.
	wrong := reaching(somebodyElse.Pin())

	conn, err := tls.Dial("tcp", address, withCache(wrong, cache))
	if err == nil {
		_ = conn.Close()

		t.Fatal("a resumed session got through without being checked")
	}
}

// listen stands a plain TLS server up. Not gRPC: what is being tested is
// a handshake, and gRPC's connection management would decide when those
// happen.
func listen(t *testing.T, identity Identity) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = listener.Close() })

	// ONE configuration for every connection. The ticket keys live on it,
	// so a server that built a fresh one per connection could not resume
	// anything — and this test would quietly pass by never reaching the
	// state it exists to check.
	config := identity.serving()

	go func() {
		for {
			accepted, err := listener.Accept()
			if err != nil {
				return
			}

			go answer(tls.Server(accepted, config))
		}
	}()

	return listener.Addr().String()
}

// answer completes a handshake and says one byte, which is what makes the
// client read — and reading is what makes it keep the ticket.
func answer(conn *tls.Conn) {
	defer func() { _ = conn.Close() }()

	if err := conn.Handshake(); err != nil {
		return
	}

	_, _ = conn.Write([]byte{'.'})

	// Held open briefly so the client's read lands before the close.
	time.Sleep(50 * time.Millisecond)
}

// connect makes one connection and reports whether it was resumed.
func connect(t *testing.T, address string, config *tls.Config, cache tls.ClientSessionCache) bool {
	t.Helper()

	conn, err := tls.Dial("tcp", address, withCache(config, cache))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	defer func() { _ = conn.Close() }()

	one := make([]byte, 1)
	if _, err := conn.Read(one); err != nil {
		t.Fatalf("read: %v", err)
	}

	return conn.ConnectionState().DidResume
}

func withCache(config *tls.Config, cache tls.ClientSessionCache) *tls.Config {
	shared := config.Clone()
	shared.ClientSessionCache = cache

	return shared
}
