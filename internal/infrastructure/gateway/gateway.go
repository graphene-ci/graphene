// Package gateway is the door a spawned process talks back through.
//
// A process gets one socket, its own, opened before it starts and taken
// away when it ends. There it finds the ordinary kernel service — not a
// special "process API": whoever can watch a kind and write to it is that
// kind's controller, and a process is no different except in how it
// proves who it is.
//
// It proves nothing, and is not asked to. Minting it a credential would
// mean writing a digest into an Identity, handing the thing that spawns
// processes authority over the strongest kind in the system; putting one
// in the Process spec would mean a secret readable by anyone who may read
// where processes are written down. So instead the socket IS the claim:
// the kernel opened it for one process, and answers for that process on
// it. Nothing a caller sends can change the answer, because nothing a
// caller sends is read.
//
// WHAT THIS DOES NOT DO: under raw-exec, processes on one machine are not
// isolated from each other. Another process running as the same user can
// reach this socket, and no credential scheme would help — it could
// equally read a token out of /proc. Isolating processes is the runner's
// job, and raw-exec has never claimed to do it. A kernel's trust boundary
// is the machine it runs on.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"

	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/app/api"
	"github.com/graphene-ci/graphene/internal/app/upstream"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/process"
)

const (
	dirMode = 0o700

	// maxSocketPath is the operating system's limit on a unix socket
	// address, minus the terminating zero. Linux allows 108 bytes; the
	// other unixes allow less, and 104 is the smallest anyone still
	// ships.
	//
	// Worth naming because of how it fails otherwise: bind returns
	// EINVAL, which prints as "invalid argument" and says nothing about
	// length. A kernel installed under a long directory would refuse to
	// run anything, for a reason nobody could act on.
	maxSocketPath = 104
)

// errSocketPathTooLong — the door cannot be opened where it was asked for.
var errSocketPathTooLong = errors.New("socket path is longer than the operating system allows")

// Serve builds the server one process talks to.
//
// A whole server per process rather than one shared, because a process's
// identity is which socket it is on: sharing one would mean asking the
// caller who it is, which is the question this design exists to avoid.
// The identity is resolved differently on either side, which is why this
// takes both the process's name and the identity its record gives it: a
// kernel with a store answers as that identity itself, and a subordinate
// names the process and lets the kernel above work it out.
type Serve func(name, identity string) *grpc.Server

// Gateway opens one door per process under a directory.
type Gateway struct {
	dir  string
	open Serve
	log  *xlog.Logger
}

// New builds a gateway that serves each process its own kernel.
func New(dir string, open Serve, log *xlog.Logger) *Gateway {
	return &Gateway{dir: dir, open: open, log: log}
}

// Above serves processes by forwarding to the kernel this one answers to.
//
// The door signs with THIS kernel's credential and names the process it
// was opened for; the kernel above checks that claim against its own
// store. Which is the same trust that already exists — a kernel holds the
// bytes and runs them, so anything on it is within its reach — made
// checkable instead of assumed.
func Above(dir string, above *upstream.Upstream, log *xlog.Logger) *Gateway {
	return &Gateway{
		dir: dir,
		log: log,
		open: func(name, _ string) *grpc.Server {
			server := grpc.NewServer()
			graphenepbv1.RegisterKernelServiceServer(server, above.ForProcess(name))
			blobpb.RegisterBlobServiceServer(server, above.BlobsFor(name))

			return server
		},
	}
}

// Here serves processes from the kernel this machine holds.
//
// The vouch is resolved here rather than made: this kernel knows which
// process it opened the door for and holds the store that says what that
// process runs as, so there is nobody to ask.
func Here(
	dir string, guard auth.Guard, own kernel.Kernel, bytes blob.Store, log *xlog.Logger,
) *Gateway {
	return New(dir, func(_, identity string) *grpc.Server {
		who, err := principalFor(identity)
		if err != nil {
			// An identity the record names and the rules refuse is a
			// door for nobody, which refuses everything. Refusing to
			// open it instead would stop a process that may not have
			// wanted to talk at all.
			log.Warn("unusable identity on a process", xlog.Err(err))

			who = ""
		}

		server := grpc.NewServer()
		graphenepbv1.RegisterKernelServiceServer(server, api.As(guard, own, who, log))

		// The byte service too, and authorized the same way: a process
		// that may run something may also be a thing that PUTS something
		// there for somebody else to run. Which of the two it may do is
		// decided by the grants its identity holds, in one place, like
		// everything else.
		blobpb.RegisterBlobServiceServer(server, api.NewBlobs(
			api.Guarded(bytes, guard, func(context.Context) (auth.Principal, error) {
				return who, nil
			}), log))

		return server
	}, log)
}

// Open prepares a process's door and returns what to tell it.
//
// The identity comes from the record, and the record was written by
// somebody who was allowed to hand that identity out — which is what
// makes this safe without a secret. A process asking for no identity gets
// a door that answers as nobody, and nobody is granted nothing.
func (g *Gateway) Open(name, identity string) (process.Door, error) {
	if err := os.MkdirAll(g.dir, dirMode); err != nil {
		return nil, fmt.Errorf("gateway: %s: %w", g.dir, err)
	}

	path := filepath.Join(g.dir, name+".sock")
	if len(path) > maxSocketPath {
		return nil, fmt.Errorf("gateway: %s: %w (%d > %d bytes) — a shorter data directory",
			path, errSocketPathTooLong, len(path), maxSocketPath)
	}

	// A socket left behind by a process that died with its kernel would
	// otherwise make the new one unbindable.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("gateway: clear %s: %w", path, err)
	}

	var listen net.ListenConfig

	listener, err := listen.Listen(context.Background(), "unix", path)
	if err != nil {
		return nil, fmt.Errorf("gateway: listen on %s: %w", path, err)
	}

	opened := &door{path: path, name: name, server: g.open(name, identity), done: make(chan struct{})}

	go func() {
		defer close(opened.done)

		_ = opened.server.Serve(listener)
	}()

	return opened, nil
}

// door is one process's way back in.
type door struct {
	path   string
	name   string
	server *grpc.Server
	done   chan struct{}
	once   sync.Once
}

// Env is the whole of what a process is told about the system it is in:
// where to talk, and what it is called. No credential, because there is
// none.
func (d *door) Env() map[string]string {
	return map[string]string{
		process.EnvSocket:  d.path,
		process.EnvProcess: d.name,
	}
}

// Close stops answering and takes the socket away. A process that outlives
// its door finds a closed connection, which is the truthful answer: the
// kernel is no longer answering for it.
func (d *door) Close() error {
	d.once.Do(func() {
		d.server.Stop()
		<-d.done
		_ = os.Remove(d.path)
	})

	return nil
}

// principalFor turns the identity a record names into who the door
// answers as.
//
// An empty identity is not an error: a process that asked for no
// credentials gets a door that answers as nobody, and nobody holds no
// grants. That is a working door onto a kernel that will refuse
// everything, which is exactly what was asked for.
func principalFor(identity string) (auth.Principal, error) {
	if identity == "" {
		return "", nil
	}

	who, err := auth.NewPrincipal(identity)
	if err != nil {
		return "", fmt.Errorf("gateway: identity %q: %w", identity, err)
	}

	return who, nil
}
