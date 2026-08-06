// Package daemon is a kernel as the machine sees it: something that is
// installed, started at boot, stopped, and asked how it is doing.
//
// It is the boundary between a program and an operating system, and
// nothing else. What a kernel DOES is app's; when the machine runs it is
// this. The library underneath speaks systemd, launchd and the Windows
// service manager through one interface, which is the only reason this
// package can be as short as it is.
package daemon

import (
	"context"
	"fmt"
	"sync"

	"github.com/kardianos/service"

	"github.com/gopherex/xlog"

	"github.com/graphene-ci/graphene/internal/app"
)

// What the machine calls a kernel.
const (
	name        = "graphened"
	displayName = "Graphene kernel"
	description = "Runs a graphene kernel: a store, an API, and what is watching it."
)

// Daemon is one installable kernel.
type Daemon struct {
	service service.Service
	program *program
}

// New describes this kernel to the service manager.
//
// It is installed as a USER service, and that follows from where a kernel
// keeps things: the store and the configuration default to the user's own
// directories, so a system service running as root would come up looking
// at an empty store in somebody else's home. One answer, everywhere,
// rather than a flag that is wrong half the time.
//
// The arguments it is installed with carry the configuration path, so a
// kernel installed against one file cannot be started against another by
// a service manager that has forgotten which.
func New(boot app.Bootstrap, log *xlog.Logger) (*Daemon, error) {
	running := &program{boot: boot, log: log, stopped: make(chan struct{})}

	made, err := service.New(running, &service.Config{
		Name:        name,
		DisplayName: displayName,
		Description: description,
		Arguments:   []string{"run", "--config", boot.Config},
		Option: service.KeyValue{
			"UserService": true,
			// WAIT FOR THE KERNEL, not only for a signal.
			//
			// The library's own wait blocks on SIGTERM and nothing else,
			// so a kernel that could not start — a store it may not open,
			// an upstream that refuses its credential — would leave a
			// process sitting there having done nothing, with the failure
			// on a goroutine nobody is reading. Waiting for either means
			// the process ends when the kernel does, whichever of them
			// decided it was over.
			//
			// The signal is still handled, one layer in: the kernel
			// installs its own handler and drains on the way out, which
			// is the behaviour a service manager is asking for when it
			// sends one.
			"RunWait": func() { <-running.stopped },
		},
	})
	if err != nil {
		return nil, fmt.Errorf("describe the service: %w", err)
	}

	return &Daemon{service: made, program: running}, nil
}

// Run hands control to the service manager, or keeps it if there is none.
//
// The same call either way, which is the library's whole point: run from
// a terminal it blocks until interrupted, run from systemd it reports
// itself started and waits to be told to stop. A kernel that behaved
// differently under a manager would be a kernel nobody could reproduce a
// problem with by hand.
func (d *Daemon) Run() error {
	if err := d.service.Run(); err != nil {
		return err
	}

	return d.program.err
}

// Install puts the kernel in the service manager, and Uninstall takes it
// out. Neither starts it: installing something that immediately runs
// takes the decision away from whoever installed it.
func (d *Daemon) Install() error   { return d.service.Install() }
func (d *Daemon) Uninstall() error { return d.service.Uninstall() }

// Start, Stop and Restart ask the manager to act. They are not the same
// as running: these return as soon as the manager has been told.
func (d *Daemon) Start() error   { return d.service.Start() }
func (d *Daemon) Stop() error    { return d.service.Stop() }
func (d *Daemon) Restart() error { return d.service.Restart() }

// Status is what the manager thinks the kernel is doing.
func (d *Daemon) Status() (service.Status, error) { return d.service.Status() }

// program is the kernel, adapted to the shape a service manager expects:
// a Start that returns at once and a Stop that finishes quickly.
//
// The kernel itself is neither — it blocks for years and drains on the
// way out — so it runs on a goroutine of its own, started HERE, which is
// as close to the composition root as this program has. It is joined in
// Stop rather than abandoned: a Stop that returned while the store was
// still being written would let the process exit mid-write.
type program struct {
	boot app.Bootstrap
	log  *xlog.Logger

	once    sync.Once
	cancel  context.CancelFunc
	stopped chan struct{}
	err     error
}

// Start gets the kernel going and returns, which is what the manager
// waits for before calling the service started.
func (p *program) Start(service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go func() {
		defer close(p.stopped)

		p.err = app.Run(ctx, p.boot, p.log)
	}()

	return nil
}

// Stop asks the kernel to wind down and waits for it to finish.
//
// Once, because a manager stopping a service that has already stopped
// itself is ordinary — a kernel that failed to bind and gave up, a
// signal that reached the process directly — and cancelling a cancelled
// context twice is fine while closing a closed channel is not.
func (p *program) Stop(service.Service) error {
	p.once.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
	})

	<-p.stopped

	return p.err
}
