// Command testpipeline is the user code of the integration contour: one
// binary, every role. The pipeline declares a machine, runs a converging
// function and a one-shot action on it, and finishes — exercising the
// whole code → server → agent → container path.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
)

// Params is the typed parameter set of the pipeline.
type Params struct {
	MachineId string `json:"machineId"`
	MarkerDir string `json:"markerDir"`
}

// E2EPipeline is the run workflow.
func E2EPipeline(ctx workflow.Context, params Params) error {
	machine := pipeline.Machine(ctx, id.MachineId(params.MachineId), pipeline.MachineSpec{})
	state, err := machine.Ready(ctx)
	if err != nil {
		return fmt.Errorf("machine: %w", err)
	}
	if !state.AgentConnected {
		return fmt.Errorf("machine ready but agent not connected")
	}

	opts := pipeline.ExecOptions{Timeout: time.Minute, HeartbeatTimeout: 20 * time.Second}
	if err := pipeline.OnMachine(ctx, id.MachineId(params.MachineId), opts,
		WriteMarker, params.MarkerDir+"/on-machine").Get(ctx, nil); err != nil {
		return fmt.Errorf("on machine: %w", err)
	}
	var pid int
	if err := pipeline.Action(ctx, id.MachineId(params.MachineId), opts,
		OneShot, params.MarkerDir+"/action").Get(ctx, &pid); err != nil {
		return fmt.Errorf("action: %w", err)
	}
	if pid == 0 {
		return fmt.Errorf("action reported no pid")
	}
	return nil
}

// WriteMarker is a machine function: it runs inside the agent-hosted
// container and proves it by writing where the workflow cannot.
func WriteMarker(_ context.Context, path string) error {
	return os.WriteFile(path, []byte(fmt.Sprintf("pid=%d", os.Getpid())), 0o600)
}

// OneShot is the at-most-once machine function.
func OneShot(_ context.Context, path string) (int, error) {
	if err := os.WriteFile(path, []byte("once"), 0o600); err != nil {
		return 0, err
	}
	return os.Getpid(), nil
}

func main() {
	pipeline.Main("e2e", E2EPipeline,
		pipeline.WithMachineFunctions(WriteMarker, OneShot),
	)
}
