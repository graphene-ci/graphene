package client

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Prefix names an address that is a COMMAND rather than a place.
//
//	exec:ssh build-01 graphened stdio
//
// It is how a kernel is reached with no port open: ssh is already a
// authenticated, encrypted way onto that machine, and `graphened stdio`
// answers on the pipe it lands in. Nothing is listening, so nothing can
// be found by scanning, and the credential that got there is the
// operator's own — theirs to lose rather than a second one to store on a
// bastion.
const Prefix = "exec:"

// errNoCommand — the prefix and nothing after it.
var errNoCommand = errors.New("exec: needs a command to run")

// Piped reports whether an address names a command, and what the command
// is.
func Piped(address string) (string, bool) {
	command, named := strings.CutPrefix(address, Prefix)

	return strings.TrimSpace(command), named
}

// piping spawns the command and hands back its pipes as a connection.
//
// NO TLS AND NO PIN over one of these, and that is not a gap. Encryption
// answers "can somebody on the path read this", and there is no path: the
// far end is a process this client started, exactly like the unix socket
// a kernel opens for a process it started. Whatever the command does to
// get where it is going — ssh, usually — is the thing that has to be
// trusted, and wrapping the result in a second tunnel would only hide
// which one was doing the work.
func piping(command string) (net.Conn, error) {
	words := strings.Fields(command)
	if len(words) == 0 {
		return nil, errNoCommand
	}

	// NOT CommandContext. The context here is the one gRPC dials with,
	// and gRPC cancels it as soon as the connection is up — which would
	// kill the command at the moment it started working. What ends this
	// process is Close, below, which is called when the connection is
	// done with.
	//nolint:gosec,noctx // the operator's own command; ended by Close, not by a context
	running := exec.Command(words[0], words[1:]...)

	// Its stderr is ours. A command that cannot reach the far machine
	// says so in words — "no route to host", "permission denied" — and
	// swallowing them would leave a client reporting only that the
	// connection ended.
	running.Stderr = os.Stderr

	out, err := running.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", words[0], err)
	}

	in, err := running.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", words[0], err)
	}

	if err := running.Start(); err != nil {
		return nil, fmt.Errorf("%s: %w", words[0], err)
	}

	return &pipe{in: in, out: out, running: running}, nil
}

// pipe is a spawned command, worn as a connection.
type pipe struct {
	in      io.ReadCloser
	out     io.WriteCloser
	running *exec.Cmd
}

func (p *pipe) Read(into []byte) (int, error)  { return p.in.Read(into) }   //nolint:wrapcheck // a pipe's own
func (p *pipe) Write(from []byte) (int, error) { return p.out.Write(from) } //nolint:wrapcheck // a pipe's own

// Close shuts the command down and waits for it.
//
// Waited for rather than left: a command that outlived every client would
// be an ssh session nobody closed, and enough of those is a machine that
// will not accept another.
func (p *pipe) Close() error {
	_ = p.out.Close()
	_ = p.in.Close()

	if p.running.Process != nil {
		_ = p.running.Process.Kill()
	}

	_ = p.running.Wait()

	return nil
}

// The rest of net.Conn, which a pipe does not have and gRPC does not use.
//
// Addresses: a pipe has none, and the two ends are named after what they
// are so that anything printing them says something true. Deadlines: a
// pipe has no timer of its own, and gRPC's own deadlines are on the CALL,
// which is where a person set them.
func (p *pipe) LocalAddr() net.Addr                { return pipeAddr("local") }
func (p *pipe) RemoteAddr() net.Addr               { return pipeAddr("pipe") }
func (p *pipe) SetDeadline(_ time.Time) error      { return nil }
func (p *pipe) SetReadDeadline(_ time.Time) error  { return nil }
func (p *pipe) SetWriteDeadline(_ time.Time) error { return nil }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }
