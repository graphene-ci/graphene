// Package app assembles a kernel out of the pieces below it and runs it.
//
// This is the composition root, and the only place that knows which
// implementation is behind which port: bbolt under the byte layer, a
// cache in front of it, a guard in front of the kernel — or, for a
// subordinate, none of those and a connection instead. Everything else in
// the program was written against an interface and does not know what is
// on the other side.
//
// It is also where every goroutine starts. Nothing below spawns one — a
// watch is pulled rather than pushed for exactly that reason — so the
// concurrency of a running kernel is the list in Run, and the list is
// short enough to read.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/app/api"
	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/app/health"
	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/app/upstream"
	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	blobcache "github.com/graphene-ci/graphene/internal/infrastructure/blob/cache"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob/remote"
	"github.com/graphene-ci/graphene/internal/infrastructure/gateway"
	"github.com/graphene-ci/graphene/internal/infrastructure/kv/bbolt"
	"github.com/graphene-ci/graphene/internal/infrastructure/runner/rawexec"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store/kv/cache"
)

// Bootstrap is the one thing that cannot come from the configuration:
// where the configuration is.
type Bootstrap struct {
	// Config is the file the kernel is configured by.
	Config string
	// Version is what the build calls itself, reported in the status.
	Version string
}

// App is a running kernel and everything under it.
//
// The same four things whichever kind of kernel it is, and that is what
// the rest of the program is built on: something that answers, something
// that records this kernel, something that says whether it is well, and
// something to close. What is behind each of them — a store or a
// connection — is decided once, in Open, and nowhere else.
type App struct {
	version string
	live    *config.Live

	service graphenepbv1.KernelServiceServer
	bytes   blobpb.BlobServiceServer
	agent   *process.Agent
	record  report.Recorder
	source  health.Source
	release func() error

	kept  kernel.Kernel
	keeps bool
}

// Open assembles a kernel, publishing what it needs to work.
//
// WHICH KERNEL IT IS is the file's answer and is read once. A kernel that
// could become subordinate while running would have to migrate a store it
// no longer keeps, and there is no such operation — so this is decided at
// start, like every other line but the address.
func Open(ctx context.Context, boot Bootstrap, log *xlog.Logger) (*App, error) {
	live, err := config.Open(boot.Config)
	if err != nil {
		return nil, err
	}

	running := live.Config()

	app := &App{version: boot.Version, live: live}

	if to, forwards := running.Upstream(); forwards {
		err = app.subordinate(to, log)
	} else {
		local, _ := running.Local()
		err = app.own(ctx, local, log)
	}

	if err != nil {
		return nil, err
	}

	// Recording itself is the one thing BOTH kinds of kernel do, and they
	// do it the same way: a subordinate writes its record up there, under
	// its own name, so a fleet is one list however it is arranged.
	if err := report.Write(ctx, app.record, app.Config(), app.version); err != nil {
		_ = app.Close()

		return nil, err
	}

	return app, nil
}

// own builds a kernel that keeps everything itself, and publishes what it
// needs to work.
//
// Everything it publishes goes through Define, which is idempotent
// against the shape — so this runs at every start and does nothing on all
// but the first. There is no separate "have I been here before" flag to
// get wrong.
func (a *App) own(ctx context.Context, local config.Local, log *xlog.Logger) error {
	if err := os.MkdirAll(filepath.Dir(local.Store()), 0o700); err != nil {
		return fmt.Errorf("prepare %s: %w", filepath.Dir(local.Store()), err)
	}

	bytes, err := bbolt.Open(local.Store())
	if err != nil {
		return err
	}

	// The cache is a Store wrapping a Store, and closing it closes what
	// it wraps — so from here on there is one thing to shut.
	cached := cache.New(bytes, cache.WithSize(local.Cache()))
	own := kernel.New(cached)

	guard := auth.New(own)

	a.service = api.New(guard, own, log)
	a.record = own
	a.source = own
	a.release = cached.Close
	a.kept, a.keeps = own, true

	if err := auth.Bootstrap(ctx, own); err != nil {
		return err
	}

	if err := report.Publish(ctx, own); err != nil {
		return err
	}

	if err := a.execute(ctx, local, guard, own, log); err != nil {
		return err
	}

	// After the kinds and not before: the first caller's role names every
	// kind there is, and at this moment that is exactly the builtin ones.
	return begin(ctx, own, a.live, local, log)
}

// execute gives this kernel what it needs to RUN things: somewhere to
// keep bytes, somewhere to answer for them, and the agent that turns a
// record into a process.
//
// Both kinds of kernel run things, and they differ in one place: the
// door. A process holds no credential, so the kernel it talks to has to
// be able to say who it is — which a kernel with a store reads out of the
// record, and a subordinate says up the link instead, signing as itself
// and naming the process it is speaking for.
func (a *App) execute(
	ctx context.Context,
	local config.Local,
	guard auth.Guard,
	own kernel.Kernel,
	log *xlog.Logger,
) error {
	if _, err := own.Define(ctx, process.Definition()); err != nil {
		return fmt.Errorf("define %s: %w", process.Kind, err)
	}

	// Beside the store, for the reason the token already lives there: a
	// kernel's things are about one place, and the configuration already
	// names that place. Three more settings would be three more ways to
	// point half of a kernel somewhere else.
	beside := filepath.Dir(local.Store())

	bytes, err := fs.Open(filepath.Join(beside, "blobs"))
	if err != nil {
		return err
	}

	closing := a.release
	a.release = func() error { return errors.Join(bytes.Close(), closing()) }

	a.bytes = api.NewBlobs(api.Guarded(bytes, guard, api.ByCredential(guard, own, log)), log)

	a.agent = a.running(beside, process.Here(own), bytes,
		gateway.Here(filepath.Join(beside, "doors"), guard, own, log), log)

	return nil
}

// forward gives a subordinate the same execution layer, pointed up.
//
// The bytes come from above and the records come from above, both asked
// for with THIS kernel's credential — a kernel fetching what it was told
// to run is acting on its own account. The door is the one piece that
// cannot be: a process gets its own, and what it does there is done as
// the identity its record names, which only the kernel above can say.
func (a *App) forward(link config.Upstream, above *upstream.Upstream, log *xlog.Logger) {
	work := link.Work()

	a.agent = a.running(work, above.Watching(), remote.Over(above.Fetching()),
		gateway.Above(filepath.Join(work, "doors"), above, log), log)
}

// running is the half that is the same on either kind of kernel.
func (a *App) running(
	work string, k process.Kernel, bytes blob.Reader, doors process.Gateway, log *xlog.Logger,
) *process.Agent {
	return &process.Agent{
		Name:   a.live.Config().Name(),
		Kernel: k,
		Fetch:  blobcache.New(filepath.Join(work, "cache"), bytes),
		Runner: rawexec.New(filepath.Join(work, "run")),
		Doors:  doors,
		Log:    log,
	}
}

// subordinate builds a kernel that keeps nothing.
//
// It publishes NOTHING either. The kinds belong to the kernel above,
// which has them already, and a proxy declaring them would be a proxy
// with an opinion — worse, one that fails to start when it is not allowed
// to define, which is a permission a subordinate has no reason to hold.
//
// There is no store here, no guard and no kernel — and that is not a
// simplification, it is the whole meaning of the mode. A subordinate that
// authorized anything would be a second opinion about permissions, and
// two opinions is one more than a system can have.
func (a *App) subordinate(link config.Upstream, log *xlog.Logger) error {
	above, err := upstream.Open(link)
	if err != nil {
		return err
	}

	a.service = above.Serving()
	a.record = above.Recording()
	a.source = above.Recording()
	a.release = above.Close

	a.forward(link, above, log)

	return nil
}

// Own is the store this kernel keeps, and whether it keeps one.
//
// The bool is the whole point of the signature: a subordinate has no
// kernel of its own, and a caller that did not ask cannot be handed a
// zero one that answers every read with an empty store.
func (a *App) Own() (kernel.Kernel, bool) { return a.kept, a.keeps }

// Service is what this kernel answers with, whatever is behind it.
func (a *App) Service() graphenepbv1.KernelServiceServer { return a.service }

// Bytes is what answers for blobs, or nil on a kernel that keeps none.
func (a *App) Bytes() blobpb.BlobServiceServer { return a.bytes }

// Agent is what runs the processes placed on this kernel, or nil when
// there is nothing here able to run them.
func (a *App) Agent() *process.Agent { return a.agent }

// Source is what its health is asked of.
func (a *App) Source() health.Source { return a.source }

// Config is what the kernel is configured to do, as of now.
func (a *App) Config() config.Config { return a.live.Config() }

// Listen is where to serve. It is on App rather than reached through
// Config because an endpoint wants the address and nothing else, and a
// narrower thing to depend on is a narrower thing to get wrong.
func (a *App) Listen() string { return a.live.Listen() }

// Changed is closed the next time the configuration changes. Take it
// BEFORE reading, or lose the change that answers what was just read.
func (a *App) Changed() <-chan struct{} { return a.live.Changed() }

// Close shuts whatever this kernel was holding: a file, or a connection.
func (a *App) Close() error {
	if a.release == nil {
		return nil
	}

	return a.release()
}
