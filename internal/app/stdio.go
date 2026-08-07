package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	blobpb "github.com/graphene-ci/graphenepb/v1/blob"
)

// Stdio serves one client on this process's own pipes.
//
// It is a whole kernel, opened the same way `run` opens one and answering
// the same service — this is not a cut-down mode. What it is not is a
// LISTENER: there is one connection, it was already there when the
// process started, and when it ends so does this.
//
// Nothing about the pipe is encrypted or pinned, and that is not a gap.
// Encryption answers "can somebody on the path read this"; the path is a
// pipe belonging to whoever started this process, usually through ssh,
// and that is the thing being trusted. A second tunnel inside it would
// only hide which one was doing the work.
//
// The kernel it serves is the one this machine's configuration describes,
// including its store — so two of these at once are two kernels on one
// store, which the store refuses. One client at a time is what this is.
func Stdio(ctx context.Context, boot Bootstrap, in io.Reader, out io.Writer) error {
	// Its log goes NOWHERE. Stdout is the wire: a line of JSON written
	// into it would be framed as gRPC and understood as nothing, and the
	// client would report a protocol error instead of whatever happened.
	log := xlog.New(xlog.NopCore{})

	kernel, err := Open(ctx, boot, log)
	if err != nil {
		return err
	}

	defer func() { _ = kernel.Close() }()

	server := grpc.NewServer()
	graphenepbv1.RegisterKernelServiceServer(server, kernel.Service())

	if bytes := kernel.Bytes(); bytes != nil {
		blobpb.RegisterBlobServiceServer(server, bytes)
	}

	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	// One connection, handed over as if it had been accepted. gRPC's
	// Serve wants a listener, so it is given one that has exactly this to
	// hand out.
	spent := make(chan struct{})
	listener := &once{conn: &pipe{in: in, out: out, spent: spent}, spent: spent}

	if err := server.Serve(listener); err != nil && !errors.Is(err, errNoMore) {
		return fmt.Errorf("stdio: %w", err)
	}

	return nil
}

// once is a listener with one connection in it.
type once struct {
	conn  net.Conn
	spent <-chan struct{}
	done  bool
}

// errNoMore — the one connection has been served and has ended.
//
// gRPC's Serve stops when Accept fails, and stopping is right ONCE the
// connection is over. Saying so immediately is what the first version of
// this did, and it tore the connection down the moment it was handed out.
var errNoMore = errors.New("stdio serves one connection")

// Accept hands the connection over, and then waits.
//
// The second call blocks until that connection is finished, because gRPC
// calls Accept in a loop and treats a failure as "stop serving". A
// listener that answered at once would end the conversation it had just
// started.
func (o *once) Accept() (net.Conn, error) {
	if o.done {
		<-o.spent

		return nil, errNoMore
	}

	o.done = true

	return o.conn, nil
}

func (o *once) Close() error   { return nil }
func (o *once) Addr() net.Addr { return pipeAddr("stdio") }

// pipe wears this process's own input and output as a connection.
type pipe struct {
	in    io.Reader
	out   io.Writer
	spent chan struct{}
	once  sync.Once
}

//nolint:wrapcheck // the pipe's own error, given to whoever is reading it
func (p *pipe) Read(into []byte) (int, error) { return p.in.Read(into) }

//nolint:wrapcheck // see Read
func (p *pipe) Write(from []byte) (int, error) { return p.out.Write(from) }

// Close closes what can be closed. Standard input and output belong to
// the process rather than to this, and the process is about to end.
func (p *pipe) Close() error {
	p.once.Do(func() {
		if closer, can := p.in.(io.Closer); can {
			_ = closer.Close()
		}

		if closer, can := p.out.(io.Closer); can {
			_ = closer.Close()
		}

		// And this is what lets the listener stop waiting, which is what
		// lets Serve return, which is what ends the process.
		close(p.spent)
	})

	return nil
}

// The rest of net.Conn, which a pipe does not have and gRPC does not use.
func (p *pipe) LocalAddr() net.Addr                { return pipeAddr("stdio") }
func (p *pipe) RemoteAddr() net.Addr               { return pipeAddr("client") }
func (p *pipe) SetDeadline(_ time.Time) error      { return nil }
func (p *pipe) SetReadDeadline(_ time.Time) error  { return nil }
func (p *pipe) SetWriteDeadline(_ time.Time) error { return nil }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }
