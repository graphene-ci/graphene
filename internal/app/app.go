// Package app assembles a kernel out of the pieces below it.
//
// This is the composition root, and it is the only place that knows which
// implementation is under which port: bbolt behind the byte layer, a
// cache in front of it, a guard in front of the kernel. Everything else
// in the program was written against an interface and does not know what
// is on the other side of it.
//
// It is also where every goroutine in the process starts. Nothing below
// this package spawns one — a watch is pulled rather than pushed for
// exactly that reason — so the concurrency in a running kernel is the
// loops main chose to run, and they are all visible in one file.
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/infrastructure/kv/bbolt"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/cache"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Bootstrap is what has to be known before the store can be opened.
//
// Two fields, and both are here for the same reason: they are needed to
// find the configuration, so they cannot come from it. The path says
// which file, the name says which record inside it describes this kernel.
type Bootstrap struct {
	// Store is the file the kernel keeps everything in.
	Store string
	// Name is which kernel this is, and the path of its own record.
	Name string
	// Version is what the build calls itself, reported in the status.
	Version string
}

// App is a running kernel and everything under it.
//
// A pointer type: it holds a file, a cache and a configuration that
// changes underneath readers. All three are state something else
// observes, which is the whole of the rule.
type App struct {
	bootstrap Bootstrap

	bytes  kv.Store
	kernel kernel.Kernel
	guard  auth.Guard

	// mu guards the configuration, which is replaced whole rather than
	// edited: a reader either sees the old one or the new one, and never
	// a listen address from one beside a cache size from the other.
	mu     sync.RWMutex
	config Config
}

// Open assembles a kernel on a store, publishing what it needs to work.
//
// Everything it publishes goes through Define, which is idempotent
// against the shape — so this runs at every start and does nothing on all
// but the first. There is no separate "have I been here before" flag to
// get wrong.
func Open(ctx context.Context, boot Bootstrap) (*App, error) {
	bytes, err := bbolt.Open(boot.Store)
	if err != nil {
		return nil, err
	}

	// The cache is a Store wrapping a Store and closing it closes what it
	// wraps, so from here on there is one thing to shut.
	cached := cache.New(bytes)

	app := &App{
		bootstrap: boot,
		bytes:     cached,
		kernel:    kernel.New(cached),
		config:    NewConfig("", 0),
	}

	app.guard = auth.New(app.kernel)

	if err := app.prepare(ctx); err != nil {
		_ = cached.Close()

		return nil, err
	}

	return app, nil
}

// Kernel is the unguarded kernel, for whoever is trusted by construction.
func (a *App) Kernel() kernel.Kernel { return a.kernel }

// Guard hands out sessions, which is what anything reached over a wire
// talks to.
func (a *App) Guard() auth.Guard { return a.guard }

// Config is what the kernel is configured to do, as of now.
func (a *App) Config() Config {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.config
}

// Close shuts the kernel and the file under it.
func (a *App) Close() error { return a.bytes.Close() }

// prepare publishes the builtin kinds and makes sure this kernel has a
// record of its own.
func (a *App) prepare(ctx context.Context) error {
	if err := auth.Bootstrap(ctx, a.kernel); err != nil {
		return err
	}

	if _, err := a.kernel.Define(ctx, Kernel()); err != nil {
		return fmt.Errorf("define %s: %w", KernelKind, err)
	}

	return a.describeSelf(ctx)
}

// describeSelf writes the kernel's own record if it is not there, and
// reports what this build is either way.
//
// The spec is written ONCE, with defaults, and never again: it is an
// administrator's to edit afterwards, and a kernel that rewrote it at
// every start would undo their edits on every restart. The status is
// written every time, because it says what is running and that is what
// just changed.
func (a *App) describeSelf(ctx context.Context) error {
	id, err := KernelId(a.bootstrap.Name)
	if err != nil {
		return err
	}

	stored, err := a.kernel.Get(ctx, id)

	switch {
	case errors.Is(err, store.ErrNotFound):
		if err := a.create(ctx, id); err != nil {
			return err
		}

		stored, err = a.kernel.Get(ctx, id)
		if err != nil {
			return err
		}

	case err != nil:
		return err
	}

	a.hold(ConfigFrom(stored.Value.Spec()))

	if _, err := a.kernel.Report(ctx, id, status(a.bootstrap.Version), stored.Revision); err != nil {
		return fmt.Errorf("report %s: %w", id, err)
	}

	return nil
}

// create writes this kernel's record for the first time.
func (a *App) create(ctx context.Context, id resource.Id) error {
	intent, err := resource.NewIntent(id, NewConfig("", 0).Spec())
	if err != nil {
		return err
	}

	if _, err := a.kernel.Put(ctx, intent, revision.Absent); err != nil {
		return fmt.Errorf("create %s: %w", id, err)
	}

	return nil
}

// hold replaces the configuration the kernel is working from.
func (a *App) hold(config Config) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.config = config
}
