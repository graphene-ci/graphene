// Package probes wires the installation's health: cached states fed by
// periodic runners over the infra dependencies, served over
// grpc.health.v1 on the gRPC door and as HTTP liveness/readiness on the
// HTTP door. Slow is not down: the taxonomy keeps Timeout distinct.
package probes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gopherex/xlog"
	"github.com/gopherex/xprobe"
	"github.com/gopherex/xprobe/pkg/probe"
	"github.com/gopherex/xprobe/pkg/runner"
	"github.com/gopherex/xprobe/pkg/state"
	"go.temporal.io/sdk/client"
)

// DockerPinger is the slice of the managed runner the probes need.
type DockerPinger interface {
	// Ping reports the docker daemon's reachability; ErrDisabled when
	// the managed contour is off.
	Ping(ctx context.Context) error
}

// ErrDisabled marks an infra dependency that is configured off — not a
// failure.
var ErrDisabled = errors.New("disabled")

// Deps are the dependencies under watch.
type Deps struct {
	Temporal         client.Client
	Docker           DockerPinger
	RegistryUpstream string
	Log              *xlog.Logger
}

// Probes is the assembled health of the installation.
type Probes struct {
	// Registry backs the grpc.health.v1 server: "" is the overall
	// status, named entries are the dependencies.
	Registry *state.Registry

	live    *probe.Bool
	runners []*runner.Runner
}

// New assembles states, probes, and runners.
func New(deps Deps) *Probes {
	p := &Probes{Registry: state.NewRegistry(), live: xprobe.NewBool()}
	p.live.Set(true)

	var all []probe.Probe
	add := func(name string, pr probe.Probe) {
		st := p.Registry.Get(name)
		p.runners = append(p.runners, runner.New(pr, st,
			runner.WithName(name),
			runner.WithInterval(10*time.Second),
			runner.WithTimeout(3*time.Second),
			runner.RunImmediately(),
		))
		all = append(all, pr)
	}

	add("temporal", xprobe.FromError(func(ctx context.Context) error {
		_, err := deps.Temporal.CheckHealth(ctx, &client.CheckHealthRequest{})
		return err
	}))
	if deps.Docker != nil {
		add("docker", xprobe.FromError(func(ctx context.Context) error {
			err := deps.Docker.Ping(ctx)
			if errors.Is(err, ErrDisabled) {
				return nil
			}
			return err
		}))
	}
	if deps.RegistryUpstream != "" {
		upstream := deps.RegistryUpstream
		add("registry", xprobe.FromError(func(ctx context.Context) error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream+"/v2/", nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			_ = resp.Body.Close()
			// 401 is a healthy registry demanding auth.
			if resp.StatusCode >= 500 {
				return fmt.Errorf("registry: %s", resp.Status)
			}
			return nil
		}))
	}

	// "" — the overall readiness: every dependency up.
	overall := p.Registry.Get("")
	p.runners = append(p.runners, runner.New(xprobe.All(all...), overall,
		runner.WithName("overall"),
		runner.WithInterval(10*time.Second),
		runner.WithTimeout(5*time.Second),
		runner.RunImmediately(),
	))
	return p
}

// Run drives every runner until ctx ends. Blocks.
func (p *Probes) Run(ctx context.Context) {
	done := make(chan struct{})
	for _, r := range p.runners {
		r := r
		go func() { r.Run(ctx); done <- struct{}{} }()
	}
	for range p.runners {
		<-done
	}
}

// HTTPMux serves the outside probes: liveness is the process itself,
// readiness reads the cached overall state — cheap under any traffic.
func (p *Probes) HTTPMux() *http.ServeMux {
	ready := xprobe.FromError(func(context.Context) error {
		if st := p.Registry.Get("").Get(); st != xprobe.StatusUp {
			return fmt.Errorf("installation is %s", st)
		}
		return nil
	})
	return xprobe.Mux(
		xprobe.Liveness(p.live),
		xprobe.Readiness(ready),
	)
}
