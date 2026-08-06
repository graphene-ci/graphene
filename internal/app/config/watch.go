package config

import (
	"context"
	"sync"

	"github.com/gopherex/xlog"
	"github.com/knadh/koanf/providers/file"
)

// Live is a configuration that follows its file.
//
// A pointer type: what it holds changes underneath readers, which is the
// whole of the rule about which receiver a type takes.
//
// The configuration itself is still a value, replaced whole rather than
// edited. A reader sees the old one or the new one and never an address
// from one beside a cache size from the other.
type Live struct {
	path string

	mu      sync.RWMutex
	current Config
	// changed is closed and replaced whenever the configuration is, which
	// is how one loop tells another without either holding a queue. A
	// closed channel wakes every waiter at once and none of them has to
	// be registered anywhere for it to.
	changed chan struct{}
}

// Open reads a configuration and prepares to follow it.
func Open(path string) (*Live, error) {
	current, err := Read(path)
	if err != nil {
		return nil, err
	}

	return &Live{path: path, current: current, changed: make(chan struct{})}, nil
}

// Path is the file this follows.
func (l *Live) Path() string { return l.path }

// Config is what the file says, as of now.
func (l *Live) Config() Config {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.current
}

// Listen is the address to serve on, which is the one thing that changes
// often enough to be worth asking for on its own.
func (l *Live) Listen() string { return l.Config().Listen() }

// Changed is closed the next time the configuration changes.
//
// TAKEN BEFORE READING, and waited on after. It is the only order that
// cannot miss one: a change landing between the read and the wait closes
// the channel already in hand. Reading first and subscribing after loses
// exactly the change that answers what was just read — which, for an
// address that would not bind, is the one correcting it.
//
//	changed := live.Changed()
//	at := live.Listen()
//	select { case <-changed: ...; case <-ctx.Done(): }
func (l *Live) Changed() <-chan struct{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.changed
}

// Begun writes the first caller's credential into the file, and takes it
// as the configuration now running.
//
// The ONE thing a kernel writes back into its own file, and once in the
// life of a store. It goes through Write like anything else, so the file
// keeps the comments a person reads — and the watcher, which is about to
// see the file change, finds nothing new in it and does nothing.
func (l *Live) Begun(token string) error {
	begun := l.Config().Begun(token)

	if err := Write(l.path, begun); err != nil {
		return err
	}

	l.hold(begun)

	return nil
}

// Watch keeps the configuration up to date with the file, until ctx is
// done, calling onChange whenever it moved.
//
// The watching is the library's and runs a goroutine of its own. That is
// the same boundary grpc's per-call goroutines are: it belongs to a
// package, it lives inside the lifetime of this loop, and it is stopped
// where the loop ends. What the rule is about is code of ours, and this
// file starts nothing.
func (l *Live) Watch(ctx context.Context, log *xlog.Logger, onChange func(Config)) error {
	provider := file.Provider(l.path)

	if err := provider.Watch(func(_ any, err error) {
		if err != nil {
			log.Error("config watch", xlog.Err(err))

			return
		}

		l.reread(log, onChange)
	}); err != nil {
		return err
	}

	defer func() { _ = provider.Unwatch() }()

	// Read once more, now that the watch is on. Between Open reading the
	// file and this line there is a gap, and an edit landing in it would
	// otherwise wait for the NEXT edit to be noticed. Subscribe first,
	// read second — the same order Changed is documented with, and the
	// same one the store's watch is used in.
	l.reread(log, onChange)

	<-ctx.Done()

	return nil
}

// reread takes what the file says now.
//
// A file that will not parse is LOGGED AND IGNORED rather than applied.
// An editor saving halfway, an invalid line, a truncated write: none of
// them are a reason to take a running kernel's configuration away, and
// what it already has is the last thing anybody successfully said.
func (l *Live) reread(log *xlog.Logger, onChange func(Config)) {
	read, err := Read(l.path)
	if err != nil {
		log.Error("keeping the configuration that parsed",
			xlog.String("file", l.path), xlog.Err(err))

		return
	}

	running := l.Config()

	// ONLY THE ADDRESS MOVES. A store cannot slide out from under a
	// kernel using it, a cache is sized when it is built, a connection is
	// made once, and a rename would leave the old record behind — so
	// everything but the address describes the NEXT start, and taking it
	// now would make the kernel report a configuration it is not running.
	//
	// Kept rather than refused: an edit meant for the next start is not a
	// mistake, it is just early.
	next := running.At(read.Listen())
	if next.Eq(running) {
		return
	}

	l.hold(next)

	log.Info("configuration changed", xlog.String("now", next.String()))

	if onChange != nil {
		onChange(next)
	}
}

// hold replaces the configuration, and wakes whoever is waiting.
func (l *Live) hold(config Config) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.current = config

	close(l.changed)
	l.changed = make(chan struct{})
}
