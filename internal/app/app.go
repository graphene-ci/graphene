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
	"os"
	"path/filepath"
	"sync"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/infrastructure/kv/bbolt"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/store/kv/cache"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Bootstrap is the one thing that cannot come from the configuration:
// where the configuration is.
//
// Everything else moved into the file. The store path and the kernel's
// name used to be here beside it, on the argument that they are needed to
// FIND the configuration — and they are not, once the configuration is
// the file rather than a record inside the store.
type Bootstrap struct {
	// Config is the file the kernel is configured by.
	Config string
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
	// changed is closed and replaced whenever the configuration does,
	// which is how one loop tells another without either of them holding
	// a queue. A closed channel wakes every waiter at once and none of
	// them has to be registered anywhere for it to.
	changed chan struct{}
}

// Open assembles a kernel on a store, publishing what it needs to work.
//
// Everything it publishes goes through Define, which is idempotent
// against the shape — so this runs at every start and does nothing on all
// but the first. There is no separate "have I been here before" flag to
// get wrong.
func Open(ctx context.Context, boot Bootstrap) (*App, error) {
	config, err := ReadConfig(boot.Config)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(config.Store()), 0o700); err != nil {
		return nil, fmt.Errorf("prepare %s: %w", filepath.Dir(config.Store()), err)
	}

	bytes, err := bbolt.Open(config.Store())
	if err != nil {
		return nil, err
	}

	// The cache is a Store wrapping a Store and closing it closes what it
	// wraps, so from here on there is one thing to shut.
	cached := cache.New(bytes, cache.WithSize(config.Cache()))

	app := &App{
		bootstrap: boot,
		bytes:     cached,
		kernel:    kernel.New(cached),
		config:    config,
		changed:   make(chan struct{}),
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

	return a.report(ctx)
}

// report writes what this kernel is running with into its own record.
//
// The record exists to be READ — by a person asking how a kernel is
// configured, and by another kernel asking what platform to build a
// controller for. Nothing writes to it but this, which is why its spec is
// empty: there is nothing here anybody could tell it.
//
// It runs at every start and again whenever the configuration changes,
// because what it says is what is running now.
func (a *App) report(ctx context.Context) error {
	config := a.Config()

	id, err := KernelId(config.Name())
	if err != nil {
		return err
	}

	stored, err := a.kernel.Get(ctx, id)

	switch {
	case errors.Is(err, store.ErrNotFound):
		intent, err := resource.NewIntent(id, schemapb.MustStructFromGo(map[string]any{}))
		if err != nil {
			return err
		}

		if _, err := a.kernel.Put(ctx, intent, revision.Absent); err != nil {
			return fmt.Errorf("create %s: %w", id, err)
		}

		stored, err = a.kernel.Get(ctx, id)
		if err != nil {
			return err
		}

	case err != nil:
		return err
	}

	status := schemapb.MustStructFromGo(runningOn(config, a.bootstrap.Version))

	if _, err := a.kernel.Report(ctx, id, status, stored.Revision); err != nil {
		return fmt.Errorf("report %s: %w", id, err)
	}

	return nil
}

// Changed is closed the next time the configuration changes.
//
// Taken BEFORE reading the configuration and waited on after, which is
// the only order that cannot miss one: a change between the read and the
// wait closes the channel already in hand.
//
//	changed := app.Changed()
//	at := app.Config().Listen()
//	select { case <-changed: ...; case <-ctx.Done(): }
func (a *App) Changed() <-chan struct{} {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.changed
}

// Apply takes a configuration directly, without waiting for the watch to
// deliver it.
//
// It exists for whoever has just written one and needs the kernel to be
// working from it now — a test, or a local edit that would rather not
// race its own notification. The watch would arrive at the same answer a
// moment later, and applying the same configuration twice is nothing.
func (a *App) Apply(config Config) { a.hold(config) }

// hold replaces the configuration the kernel is working from, and tells
// whoever is waiting.
func (a *App) hold(config Config) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.config = config

	close(a.changed)
	a.changed = make(chan struct{})
}
