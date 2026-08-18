// Package agents is the server side of the agent wire: it serves the
// bidirectional session streams, tracks which machines have a connected
// agent, and sends container commands, waiting for their results.
package agents

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/gopherex/xlog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/graphene/internal/auth"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Registry implements agentpb.AgentAPIServer and answers "is the agent
// of machine X here" for the machine flow's Ops.
type Registry struct {
	agentpb.UnimplementedAgentAPIServer

	heartbeatSeconds uint32
	log              *xlog.Logger

	mu     sync.Mutex
	agents map[agentKey]*session
}

// agentKey isolates agents per namespace: the same agent id in two
// namespaces is two different agents.
type agentKey struct {
	namespace string
	agentId   id.AgentId
}

// session is one connected agent.
type session struct {
	namespace   string
	agentId     id.AgentId
	facts       *agentpb.Facts
	factsDigest string
	lastSeen    time.Time

	sendMu sync.Mutex
	stream agentpb.AgentAPI_SessionServer

	pendingMu sync.Mutex
	pending   map[string]chan string // command id -> error text ("" = ok)

	containersMu sync.Mutex
	containers   map[id.RunId]map[id.AgentId]agentpb.ContainerState
}

// New builds the registry.
func New(heartbeat time.Duration, log *xlog.Logger) *Registry {
	return &Registry{
		heartbeatSeconds: uint32(max(1, int(heartbeat/time.Second))), //nolint:gosec // small positive
		log:              log,
		agents:           map[agentKey]*session{},
	}
}

// Session serves one agent stream for its whole life.
func (r *Registry) Session(stream agentpb.AgentAPI_SessionServer) error {
	principal, ok := auth.FromContext(stream.Context())
	if !ok || principal.Role != auth.RoleAgent {
		return status.Error(codes.PermissionDenied, "agent token required")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first message must be hello")
	}
	agentId, err := id.ParseAgentId(hello.GetAgentId())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	// The token is scoped to one record: an agent cannot embody another.
	if principal.AgentId != agentId {
		return status.Error(codes.PermissionDenied, "token is not for this agent")
	}
	key := agentKey{namespace: principal.Namespace, agentId: agentId}

	s := &session{
		namespace:   principal.Namespace,
		agentId:     agentId,
		facts:       hello.GetFacts(),
		factsDigest: digest(hello.GetFacts()),
		lastSeen:    time.Now(),
		stream:      stream,
		pending:     map[string]chan string{},
		containers:  map[id.RunId]map[id.AgentId]agentpb.ContainerState{},
	}
	r.mu.Lock()
	r.agents[key] = s
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		if r.agents[key] == s {
			delete(r.agents, key)
		}
		r.mu.Unlock()
		r.log.Info("agent disconnected", xlog.Any("agent", agentId), xlog.String("namespace", key.namespace))
	}()
	r.log.Info("agent connected", xlog.Any("agent", agentId), xlog.String("version", hello.GetAgentVersion()))

	if err := s.send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_HelloAck{
		HelloAck: &agentpb.HelloAck{HeartbeatSeconds: r.heartbeatSeconds},
	}}); err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		s.lastSeen = time.Now()
		switch body := msg.GetBody().(type) {
		case *agentpb.SessionRequest_Heartbeat:
			for _, report := range body.Heartbeat.GetContainers() {
				s.track(report)
			}
		case *agentpb.SessionRequest_ContainerReport:
			s.track(body.ContainerReport)
			r.log.Info("container report",
				xlog.String("agent", body.ContainerReport.GetAgentId()),
				xlog.String("run", body.ContainerReport.GetRunId()),
				xlog.String("state", body.ContainerReport.GetState().String()),
				xlog.String("message", body.ContainerReport.GetMessage()))
		case *agentpb.SessionRequest_CommandResult:
			s.deliver(body.CommandResult)
		}
	}
}

// EnsureContainer asks the machine's agent to bring the container up and
// waits for the command result.
func (r *Registry) EnsureContainer(ctx context.Context, namespace string, spec *agentpb.ContainerSpec) error {
	return r.command(ctx, namespace, id.AgentId(spec.GetAgentId()), func(commandId string) *agentpb.SessionResponse {
		return &agentpb.SessionResponse{Body: &agentpb.SessionResponse_EnsureContainer{
			EnsureContainer: &agentpb.EnsureContainer{CommandId: commandId, Spec: spec},
		}}
	})
}

// StopContainer asks the machine's agent to stop the (machine × run)
// container and waits for the result.
func (r *Registry) StopContainer(ctx context.Context, namespace string, agentId id.AgentId, runId id.RunId) error {
	return r.command(ctx, namespace, agentId, func(commandId string) *agentpb.SessionResponse {
		return &agentpb.SessionResponse{Body: &agentpb.SessionResponse_StopContainer{
			StopContainer: &agentpb.StopContainer{
				CommandId: commandId,
				AgentId:   string(agentId),
				RunId:     string(runId),
			},
		}}
	})
}

// Status reports the agent's presence for the machine flow.
func (r *Registry) Status(namespace string, agentId id.AgentId) agentflow.AgentStatus {
	r.mu.Lock()
	s, ok := r.agents[agentKey{namespace: namespace, agentId: agentId}]
	r.mu.Unlock()
	if !ok {
		return agentflow.AgentStatus{}
	}
	return agentflow.AgentStatus{
		Connected:   true,
		Addresses:   s.facts.GetAddresses(),
		FactsDigest: s.factsDigest,
	}
}

func (r *Registry) command(ctx context.Context, namespace string, agentId id.AgentId, build func(commandId string) *agentpb.SessionResponse) error {
	r.mu.Lock()
	s, ok := r.agents[agentKey{namespace: namespace, agentId: agentId}]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("agent of machine %q is not connected", agentId)
	}
	commandId := uuid.NewString()
	ch := make(chan string, 1)
	s.pendingMu.Lock()
	s.pending[commandId] = ch
	s.pendingMu.Unlock()
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, commandId)
		s.pendingMu.Unlock()
	}()
	if err := s.send(build(commandId)); err != nil {
		return err
	}
	select {
	case errText := <-ch:
		if errText != "" {
			return fmt.Errorf("agent: %s", errText)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *session) send(msg *agentpb.SessionResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(msg)
}

// StopRunContainers stops the run's container on every machine that has
// one — the teardown half of the run cleanup.
func (r *Registry) StopRunContainers(ctx context.Context, namespace string, runId id.RunId) error {
	r.mu.Lock()
	sessions := make([]*session, 0, len(r.agents))
	for key, s := range r.agents {
		if key.namespace == namespace {
			sessions = append(sessions, s)
		}
	}
	r.mu.Unlock()
	var errs []error
	for _, s := range sessions {
		s.containersMu.Lock()
		_, has := s.containers[runId]
		s.containersMu.Unlock()
		if !has {
			continue
		}
		if err := r.StopContainer(ctx, namespace, s.agentId, runId); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// track records the latest reported state of one container; stopped
// containers leave the set.
func (s *session) track(report *agentpb.ContainerReport) {
	runId := id.RunId(report.GetRunId())
	agentId := id.AgentId(report.GetAgentId())
	s.containersMu.Lock()
	defer s.containersMu.Unlock()
	if report.GetState() == agentpb.ContainerState_CONTAINER_STATE_STOPPED {
		if byMachine, ok := s.containers[runId]; ok {
			delete(byMachine, agentId)
			if len(byMachine) == 0 {
				delete(s.containers, runId)
			}
		}
		return
	}
	if s.containers[runId] == nil {
		s.containers[runId] = map[id.AgentId]agentpb.ContainerState{}
	}
	s.containers[runId][agentId] = report.GetState()
}

func (s *session) deliver(res *agentpb.CommandResult) {
	s.pendingMu.Lock()
	ch, ok := s.pending[res.GetCommandId()]
	s.pendingMu.Unlock()
	if ok {
		ch <- res.GetError()
	}
}

// digest fingerprints the facts; the machine record carries only this.
func digest(f *agentpb.Facts) string {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(f)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
