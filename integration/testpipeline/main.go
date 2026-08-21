// Command testpipeline is the user code of the integration contour on
// surface v2: one binary, every role. It exercises the whole system —
// an agent with labels, inline activities on its machine (at-least-once
// and at-most-once), a capability published and required, a selection
// fan-out, an artifact from computed bytes, a foreign attach, and a
// stand transfer with a TTL.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/api/types/volume"

	dockerlib "github.com/graphene-ci/library/docker"
	pipelineactivity "github.com/graphene-ci/pipeline/pkg/activity"
	"github.com/graphene-ci/pipeline/pkg/artifact"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Params is the typed input of the run.
type Params struct {
	AgentId   string        `json:"agentId"`
	MarkerDir string        `json:"markerDir"`
	Keep      time.Duration `json:"keep"`
	// VolumeName, when set, declares a docker volume on the agent's
	// machine as a LIBRARY KIND entity and hands it to the stand — the
	// library-kind leg of the contour.
	VolumeName string `json:"volumeName"`
}

// Result is the run's output.
type Result struct {
	Report         string `json:"report"`
	FanOut         int    `json:"fanOut"`
	BaselineDigest string `json:"baselineDigest"`
}

func main() {
	pipeline.Main("e2e", func(ctx pipeline.Context, params Params) (Result, error) {
		agent := pipeline.NewAgent(ctx, params.AgentId,
			pipeline.WithLabels(map[string]string{"role": "e2e"}))
		if state := agent.Ready(ctx); !state.AgentConnected {
			return Result{}, fmt.Errorf("agent ready but not connected")
		}

		// Converging code on the machine: proves it runs in the
		// agent-hosted container by writing where the workflow cannot.
		report, err := pipelineactivity.Activity(ctx, agent,
			pipelineactivity.ActivityFn("write-marker",
				func(_ context.Context, dir string) (string, error) {
					msg := fmt.Sprintf("pid=%d", os.Getpid())
					return msg, os.WriteFile(dir+"/on-machine", []byte(msg), 0o600)
				},
				params.MarkerDir,
			),
		)
		if err != nil {
			return Result{}, fmt.Errorf("on machine: %w", err)
		}

		// One-shot: at most once, never a silent second execution.
		if _, err := pipelineactivity.Activity(ctx, agent,
			pipelineactivity.ActivityFn("one-shot",
				func(_ context.Context, dir string) (string, error) {
					return "once", os.WriteFile(dir+"/action", []byte("once"), 0o600)
				},
				params.MarkerDir,
			),
			pipelineactivity.WithGuarantee(pipelineactivity.AtMostOnce),
		); err != nil {
			return Result{}, fmt.Errorf("one shot: %w", err)
		}

		// A capability, published onto the record and then REQUIRED: the
		// attach below refuses to be ready before it is there.
		if err := pipeline.PublishCapability(ctx, agent, pipeline.Capability{
			Name: "marker", Version: "1", BroughtBy: "testpipeline", Ready: true,
		}); err != nil {
			return Result{}, fmt.Errorf("publish: %w", err)
		}
		foreign := pipeline.AttachAgent(ctx, params.AgentId, pipeline.Need("marker"))
		if state := foreign.Ready(ctx); !state.AgentConnected {
			return Result{}, fmt.Errorf("attached agent not connected")
		}

		// "Run it on all who are marked": selection + fan-out.
		marked, err := pipeline.SelectAgents(ctx,
			pipeline.WithLabels(map[string]string{"role": "e2e"}),
			pipeline.Need("marker"),
		)
		if err != nil {
			return Result{}, fmt.Errorf("select: %w", err)
		}
		fanReports, err := pipelineactivity.ActivityAll(ctx, marked,
			pipelineactivity.ActivityFn("fan-out",
				func(_ context.Context, dir string) (string, error) {
					return "fan", os.WriteFile(dir+"/fan-out", []byte("fan"), 0o600)
				},
				params.MarkerDir,
			),
		)
		if err != nil {
			return Result{}, fmt.Errorf("fan out: %w", err)
		}

		// An artifact from bytes the run computed; then attached back the
		// foreign way and read.
		art := pipeline.NewArtifact(ctx, "e2e-report", artifact.FromBytes([]byte(report)))
		if state := art.Ready(ctx); !state.Verified {
			return Result{}, fmt.Errorf("artifact not verified")
		}
		baseline := pipeline.AttachArtifact(ctx, "e2e-report")
		digest := baseline.Ready(ctx).Blob.Digest

		// Long life is a transfer: the artifact goes to the pipeline's
		// Stand with a small TTL — the run ends, the record stays until
		// the sweeper collects it.
		pipeline.ToStand(ctx, art, pipeline.KeepFor(params.Keep))

		// The library-kind leg: a docker volume declared as an entity on
		// the agent's machine, owned in the tree, then handed to the
		// stand with the same TTL — the record must outlive the run and
		// its cascade must remove the real volume. Declared
		// unconditionally: the recording pass walks the zero path.
		vol := dockerlib.Volume(ctx, agent, volume.CreateOptions{Name: params.VolumeName})
		if info := vol.Ready(ctx); info.Name != params.VolumeName {
			return Result{}, fmt.Errorf("volume ready: want %q, got %q", params.VolumeName, info.Name)
		}
		pipeline.ToStand(ctx, vol, pipeline.KeepFor(params.Keep))

		return Result{Report: report, FanOut: len(fanReports), BaselineDigest: digest}, nil
	})
}
