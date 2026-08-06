package process

import (
	"context"
	"io"

	"github.com/graphene-ci/graphene/internal/blob"
)

// Fetcher turns a blob id into a local path. Where the bytes come from,
// how they are cached and whether they are what they claim to be are its
// business and not the agent's.
type Fetcher interface {
	Fetch(ctx context.Context, id blob.Id) (string, error)
}

// Spec is one process to start.
type Spec struct {
	Path string
	Args []string
	Env  map[string]string
	// Name is the record's own name — what the kernel vouches for when
	// this process calls back.
	Name string
}

// Started is a process that is running now.
type Started interface {
	// Wait blocks until it ends and reports its exit code.
	Wait() (int, error)
	// Stop asks it to end and waits for it to.
	Stop() error
}

// Runner starts prepared bytes. raw-exec is the only implementation the
// kernel ships; anything heavier goes behind the same interface.
type Runner interface {
	Start(ctx context.Context, spec Spec) (Started, error)
}

// Gateway gives a process its way back into the system: a door opened
// before it starts and taken away when it ends.
//
// A process holds no credentials — the door IS the credential. That is
// why it is opened per process and closed the moment the process is done
// with it: a door outliving its process would be a way in for whatever
// came next.
type Gateway interface {
	Open(name string) (Door, error)
}

// Door is one process's way back in.
type Door interface {
	// Env is what the process is told about where it can talk.
	Env() map[string]string
	io.Closer
}
