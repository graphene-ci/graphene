package app

import (
	"context"
	"log/slog"

	"github.com/knadh/koanf/providers/file"
)

// Watch keeps the kernel working from what the file says, until ctx is
// done.
//
// The watching itself is the library's, and it runs a goroutine of its
// own. That is the same boundary grpc's per-call goroutines are: it
// belongs to a package, it lives inside the lifetime of this loop, and it
// is stopped where the loop ends. What the rule is about is code of
// ours, and this file starts nothing.
//
// The callback does as little as it can — read the file, take what it
// says — because it runs on somebody else's goroutine and everything it
// touches is guarded.
//
// A file that stops parsing is LOGGED AND IGNORED, not applied. An editor
// saving halfway through, an invalid line, a truncated write: none of
// them are a reason to take a running kernel's configuration away, and
// what it already has is the last thing anybody successfully said.
func (a *App) Watch(ctx context.Context, log *slog.Logger) error {
	provider := file.Provider(a.bootstrap.Config)

	if err := provider.Watch(func(_ any, err error) {
		if err != nil {
			log.Error("config watch", "err", err)

			return
		}

		a.reread(ctx, log)
	}); err != nil {
		return err
	}

	defer func() { _ = provider.Unwatch() }()

	// Read once more, now that the watch is on. Between Open reading the
	// file and this line there is a gap, and an edit that lands in it
	// would otherwise wait for the NEXT edit to be noticed.
	//
	// Subscribe first, read second: the same order the store's watch is
	// used in, and for the same reason. It costs one read and it makes a
	// missed change impossible rather than unlikely.
	a.reread(ctx, log)

	<-ctx.Done()

	return nil
}

// reread takes what the file says now.
//
// A file that will not parse is LOGGED AND IGNORED rather than applied.
// An editor saving halfway, an invalid line, a truncated write: none of
// them are a reason to take a running kernel's configuration away, and
// what it already has is the last thing anybody successfully said.
func (a *App) reread(ctx context.Context, log *slog.Logger) {
	config, err := ReadConfig(a.bootstrap.Config)
	if err != nil {
		log.Error("keeping the configuration that parsed",
			"file", a.bootstrap.Config, "err", err)

		return
	}

	// Two of these cannot be acted on while a kernel is running: a store
	// cannot move under one using it, and a rename would leave its record
	// behind. They are kept from the start rather than refused, so an edit
	// that changes them describes the next start rather than being lost.
	running := a.Config()
	config = NewConfig(running.Store(), running.Name(), config.Listen(), config.Cache())

	if config.Eq(running) {
		return
	}

	a.hold(config)

	log.Info("configuration changed", "now", config)

	if err := a.report(ctx); err != nil {
		log.Error("report", "err", err)
	}
}
