package agents

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gopherex/xlog"
	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/graphene/internal/auth"
	"google.golang.org/grpc"
)

// The stream can return EOF while its context is still live. Waiting only on
// the activity context leaves a command stranded after the agent reconnects.
func TestCommandEndsWithSessionAndCanRetryAfterReconnect(t *testing.T) {
	for _, disconnect := range []string{"eof", "context-canceled"} {
		t.Run(disconnect, func(t *testing.T) {
			registry := New(time.Second, xlog.New(xlog.NopCore{}))
			stream, cancel, ended := connectTestAgent(t, registry)
			defer cancel()
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			result := make(chan error, 1)
			spec := &agentpb.ContainerSpec{AgentId: "machine-1", RunId: "run-1", Image: "worker:1"}
			go func() { result <- registry.EnsureContainer(ctx, "test", spec) }()
			command := receive(t, stream.out)
			if command.GetEnsureContainer() == nil {
				t.Fatal("expected ensure command")
			}
			if disconnect == "eof" {
				close(stream.in)
			} else {
				cancel()
			}
			if err := receive(t, ended); !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
				t.Fatalf("session: %v", err)
			}
			if err := receive(t, result); err == nil || !strings.Contains(err.Error(), "disconnected before command reply") {
				t.Fatalf("pending command: %v", err)
			}
			if ctx.Err() != nil {
				t.Fatal("caller context must still be live")
			}

			// A normal retry must bind to the new stream and await its own reply.
			fresh, cancelFresh, freshEnded := connectTestAgent(t, registry)
			defer cancelFresh()
			go func() { result <- registry.EnsureContainer(ctx, "test", spec) }()
			next := receive(t, fresh.out).GetEnsureContainer()
			if next == nil || next.GetCommandId() == command.GetEnsureContainer().GetCommandId() {
				t.Fatal("retry did not get a fresh command")
			}
			fresh.in <- &agentpb.SessionRequest{Body: &agentpb.SessionRequest_CommandResult{CommandResult: &agentpb.CommandResult{CommandId: next.GetCommandId()}}}
			if err := receive(t, result); err != nil {
				t.Fatalf("retry: %v", err)
			}
			cancelFresh()
			_ = receive(t, freshEnded)
		})
	}
}

type testAgentStream struct {
	grpc.ServerStream
	ctx context.Context
	in  chan *agentpb.SessionRequest
	out chan *agentpb.SessionResponse
}

func (s *testAgentStream) Context() context.Context { return s.ctx }
func (s *testAgentStream) Send(msg *agentpb.SessionResponse) error {
	select {
	case s.out <- msg:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
func (s *testAgentStream) Recv() (*agentpb.SessionRequest, error) {
	select {
	case msg, ok := <-s.in:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func connectTestAgent(t *testing.T, registry *Registry) (*testAgentStream, context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(auth.WithPrincipal(context.Background(), auth.Principal{Role: auth.RoleAgent, Namespace: "test", AgentId: "machine-1"}))
	s := &testAgentStream{ctx: ctx, in: make(chan *agentpb.SessionRequest, 1), out: make(chan *agentpb.SessionResponse, 1)}
	ended := make(chan error, 1)
	go func() { ended <- registry.Session(s) }()
	s.in <- &agentpb.SessionRequest{Body: &agentpb.SessionRequest_Hello{Hello: &agentpb.Hello{AgentId: "machine-1", Facts: &agentpb.Facts{Hostname: "machine-1"}}}}
	if receive(t, s.out).GetHelloAck() == nil {
		cancel()
		t.Fatal("missing hello ack")
	}
	return s, cancel, ended
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not finish after session change")
		var zero T
		return zero
	}
}

// Stopping a run's executors asks every machine at once and is bounded by
// the caller: one agent that never answers — its VM deleted, its session
// kept alive by a proxy — costs the timeout once, not once per machine.
func TestStopRunContainersIsParallelAndBounded(t *testing.T) {
	registry := New(time.Second, xlog.New(xlog.NopCore{}))
	stream, cancel, _ := connectTestAgent(t, registry)
	defer cancel()
	for _, run := range []string{"run-1"} {
		stream.in <- &agentpb.SessionRequest{Body: &agentpb.SessionRequest_ContainerReport{ContainerReport: &agentpb.ContainerReport{AgentId: "machine-1", RunId: run, State: agentpb.ContainerState_CONTAINER_STATE_RUNNING}}}
	}
	time.Sleep(100 * time.Millisecond)
	ctx, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	start := time.Now()
	err := registry.StopRunContainers(ctx, "test", "run-1", nil)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a silent agent: %v, want the caller's deadline", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("stop took %s past its 300ms bound", took)
	}
}
