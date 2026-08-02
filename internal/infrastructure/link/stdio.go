package link

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
)

var (
	// ErrExhausted — a single-use link was dialed twice. The death of a
	// stdio pipe is the death of the session; the process is expected to
	// exit and be respawned by whoever owns the session.
	ErrExhausted = errors.New("link: single-use link already dialed")
	// ErrListenerClosed — the single-conn listener was closed.
	ErrListenerClosed = errors.New("link: listener closed")
)

// Stdio is the worker-side link of an ssh-spawned kernel: the process's
// own stdin/stdout as one single-use pipe. The channel security is the
// ssh session's; no TLS is layered inside (see Connect).
func Stdio() corelink.Link {
	return Single(NewPipeConn(os.Stdin, os.Stdout))
}

// Single wraps an existing pipe as a one-shot link.
func Single(conn net.Conn) corelink.Link {
	return &singleLink{conn: conn}
}

type singleLink struct {
	mu   sync.Mutex
	conn net.Conn
}

func (l *singleLink) Dial(context.Context) (net.Conn, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn == nil {
		return nil, ErrExhausted
	}

	conn := l.conn
	l.conn = nil

	return conn, nil
}

// NewPipeConn glues a read and a write end into a net.Conn (a process's
// stdio, an exec.Cmd's pipes). Deadlines are no-ops: the gRPC transport
// does not rely on them for correctness on this kind of channel.
func NewPipeConn(reader io.ReadCloser, writer io.WriteCloser) net.Conn {
	return &pipeConn{reader: reader, writer: writer}
}

type pipeConn struct {
	reader io.ReadCloser
	writer io.WriteCloser
}

func (c *pipeConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }  //nolint:wrapcheck // pass-through
func (c *pipeConn) Write(p []byte) (int, error) { return c.writer.Write(p) } //nolint:wrapcheck // pass-through

func (c *pipeConn) Close() error {
	rerr := c.reader.Close()
	werr := c.writer.Close()

	return errors.Join(rerr, werr)
}

func (c *pipeConn) LocalAddr() net.Addr                { return pipeAddr{} }
func (c *pipeConn) RemoteAddr() net.Addr               { return pipeAddr{} }
func (c *pipeConn) SetDeadline(time.Time) error        { return nil }
func (c *pipeConn) SetReadDeadline(time.Time) error    { return nil }
func (c *pipeConn) SetWriteDeadline(_ time.Time) error { return nil }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// SingleConnListener adapts one accepted pipe to the net.Listener the
// shared gRPC server consumes: the SAME server (same interceptors, same
// services) serves the TCP endpoint and one goroutine per ssh session.
func SingleConnListener(conn net.Conn) net.Listener {
	lis := &singleConnListener{done: make(chan struct{})}
	lis.conn = conn

	return lis
}

type singleConnListener struct {
	mu   sync.Mutex
	conn net.Conn
	done chan struct{}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()

	if conn != nil {
		return conn, nil
	}

	<-l.done

	return nil, ErrListenerClosed
}

func (l *singleConnListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-l.done:
	default:
		close(l.done)
	}

	return nil
}

func (l *singleConnListener) Addr() net.Addr { return pipeAddr{} }
