package app

import (
	"context"
	"fmt"
	"time"

	"github.com/gopherex/xlog"
	"github.com/gopherex/xshutdown"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/app/health"
	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/app/server"
)

// How long a kernel is given to wind down before it is taken down.
//
// A shutdown that could hang forever is a service that has to be killed,
// and a service that is killed loses whatever it was in the middle of.
const (
	drain = 15 * time.Second
	// forced is the exit code when the drain did not finish. It is
	// distinct from a plain failure so that whoever reads it knows the
	// difference between "it stopped badly" and "it would not stop".
	forced = 2
)

// Run is the whole of a kernel's life, and every goroutine in it.
//
// The rule is that nothing below the composition root starts a goroutine
// — a watch is pulled rather than pushed for exactly that reason — so the
// concurrency of a running kernel is the four lines below, and four lines
// can be read.
//
// The manager is what makes that more than a convention. A goroutine it
// starts is tracked, told to stop through a context that cascades, and
// waited for; one started any other way is none of those things, and the
// difference shows up as a shutdown that does not finish.
func Run(ctx context.Context, boot Bootstrap, log *xlog.Logger) error {
	stop := xshutdown.New(ctx,
		xshutdown.WithTimeout(drain),
		xshutdown.WithForceExit(forced),
		xshutdown.WithErrorHandler(func(err error) { log.Error("shutdown", xlog.Err(err)) }),
	)

	// stop.Context() is the run's own: the parent ends it, and so does a
	// signal. Everything below is started from it, so a shutdown reaches
	// all of it without anyone passing a second cancel around.
	//nolint:contextcheck // the shutdown's context IS the run's context
	kernel, err := Open(stop.Context(), boot, log)
	if err != nil {
		return err
	}

	// Closing the store is cleanup and not work: it runs after the last
	// worker has wound down, which is the only order in which closing a
	// file out from under one of them cannot happen.
	stop.RegisterFnErr(func(context.Context) error { return kernel.Close() })

	checks := health.New(kernel.Source(), log)
	endpoint := server.New(kernel, kernel.As(), kernel.Service(), kernel.Bytes(), checks.Server(), log)

	log.Info("kernel",
		xlog.String("config", boot.Config),
		xlog.String("running", kernel.Config().String()))

	//  1. Serve, which stands a server up and stands it up again
	//  2. Rebind, which stops the one that is up when it should not be
	//  3. Watch, which keeps the configuration up to date with the file
	//  4. Beat, which keeps saying this kernel is still here
	//  5. Poll, which keeps the health it answers with true
	//
	// The second is what makes the first return, so it cannot be cleanup
	// instead: cleanup runs after the drain, and the drain is waiting for
	// the call the second one ends.
	stop.Go(func(ctx context.Context) {
		if err := endpoint.Serve(ctx); err != nil {
			log.Error("serve", xlog.Err(err))
		}
	})

	stop.Go(func(ctx context.Context) {
		if err := endpoint.Rebind(ctx); err != nil {
			log.Error("rebind", xlog.Err(err))
		}
	})

	stop.Go(func(ctx context.Context) {
		if err := Watch(ctx, kernel, log); err != nil {
			log.Error("config watch", xlog.Err(err))
		}
	})

	stop.Go(func(ctx context.Context) { Beat(ctx, kernel, log) })

	// The agent. Both kinds of kernel have one — a subordinate watches
	// the kernel above for what to run — so the nil check is for a kernel
	// that failed to build one, not for a kind that has none.
	if agent := kernel.Agent(); agent != nil {
		stop.Go(func(ctx context.Context) {
			if err := agent.Run(ctx); err != nil {
				log.Error("agent", xlog.Err(err))
			}
		})
	}

	stop.Go(checks.Poll)

	// Run installs the signals, blocks, and then drains.
	if err := stop.Run(); err != nil {
		return fmt.Errorf("run: %w", err)
	}

	log.Info("stopped")

	return nil
}

// Watch keeps the configuration up to date, and records what changed.
//
// The report is written again on every change, because what it says is
// what is running NOW — and after a change, what is running is different.
// Beat keeps saying this kernel is here, until it is not.
//
// A failed beat is logged and the next one is tried. It is a write to a
// kernel that may be a link away, so a failure is ordinary — and a kernel
// that stopped beating because one write failed would report itself dead
// while running perfectly.
//
// There is nothing to stop: the loop ends with the context, and whoever
// reads the record afterwards works out from the last beat that this
// kernel is gone. That is the shutdown a kernel that was switched off and
// one whose machine caught fire have in common, and having one path for
// both is the point.
func Beat(ctx context.Context, a *App, log *xlog.Logger) {
	ticker := time.NewTicker(report.Beat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := report.Write(ctx, a.record, a.Config(), a.version); err != nil {
			log.Warn("beat", xlog.Err(err))
		}
	}
}

func Watch(ctx context.Context, a *App, log *xlog.Logger) error {
	return a.live.Watch(ctx, log, func(running config.Config) {
		if err := report.Write(ctx, a.record, running, a.version); err != nil {
			log.Error("report", xlog.Err(err))
		}
	})
}
