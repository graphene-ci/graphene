package services

// The machine-shell door: AgentsAPI.Pty streams a shell that runs on
// the machine, AgentsAPI.PtyInput lands the operator's keystrokes.
// Authorization is invoke on the agent kind — opening a hand on a
// machine is a command, not a read — and the opening lands in the
// agent record's history as an audit note.

import (
	"context"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/obs"
)

// Pty opens the shell and streams its output; the first chunk names
// the session every PtyInput addresses. Returning closes the shell.
func (m *Management) Pty(ctx context.Context, creq *connect.Request[managementv1.PtyRequest], stream *connect.ServerStream[managementv1.PtyChunk]) error {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbInvoke, authz.KindAgent)
	if err != nil {
		return asConnectError(err)
	}
	if m.Agents == nil {
		return asConnectError(status.Error(codes.Unimplemented, "this installation serves no agents"))
	}
	agentId, err := id.ParseAgentId(req.GetAgent())
	if err != nil {
		return asConnectError(status.Error(codes.InvalidArgument, err.Error()))
	}
	ptyId, events, closeFn, err := m.Agents.OpenPty(b.Namespace, agentId, req.GetCols(), req.GetRows())
	if err != nil {
		return asConnectError(status.Error(codes.FailedPrecondition, err.Error()))
	}
	defer closeFn()
	ref := "agent/" + string(agentId)
	// A shell on the machine is as audited as any mutation: who opened
	// it lands in the agent record's own history.
	m.audit(ctx, b, ref, authz.VerbInvoke)
	octx := obs.WithEntity(ctx, ref)
	obs.Info(octx, "machine shell opened")
	obs.Count(octx, MetricCommand, 1, obs.Str("command", "pty"))
	m.Log.Info("machine shell opened",
		xlog.String("namespace", b.Namespace), xlog.Any("agent", agentId))

	if err := stream.Send(&managementv1.PtyChunk{Body: &managementv1.PtyChunk_Opened_{
		Opened: &managementv1.PtyChunk_Opened{SessionId: ptyId},
	}}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil // the viewer left; defer buries the shell
		case ev, ok := <-events:
			if !ok {
				return nil // the agent's side is gone
			}
			if ev.Closed {
				obs.Info(octx, "machine shell closed", obs.Int("exit", int(ev.Exit)))
				return stream.Send(&managementv1.PtyChunk{Body: &managementv1.PtyChunk_Closed_{
					Closed: &managementv1.PtyChunk_Closed{ExitCode: ev.Exit, Message: ev.Msg},
				}})
			}
			if err := stream.Send(&managementv1.PtyChunk{Body: &managementv1.PtyChunk_Data{Data: ev.Data}}); err != nil {
				return err
			}
		}
	}
}

// PtyInput lands one input frame on a live session.
func (m *Management) PtyInput(ctx context.Context, creq *connect.Request[managementv1.PtyInputRequest]) (*connect.Response[managementv1.PtyInputResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbInvoke, authz.KindAgent)
	if err != nil {
		return nil, err
	}
	if m.Agents == nil {
		return nil, status.Error(codes.Unimplemented, "this installation serves no agents")
	}
	switch body := req.GetBody().(type) {
	case *managementv1.PtyInputRequest_Data:
		err = m.Agents.PtyInput(b.Namespace, req.GetSessionId(), body.Data)
	case *managementv1.PtyInputRequest_Resize_:
		err = m.Agents.PtyResize(b.Namespace, req.GetSessionId(), body.Resize.GetCols(), body.Resize.GetRows())
	case *managementv1.PtyInputRequest_Close:
		err = m.Agents.PtyClose(b.Namespace, req.GetSessionId())
	default:
		return nil, status.Error(codes.InvalidArgument, "an input frame carries data, a resize or a close")
	}
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return connect.NewResponse(&managementv1.PtyInputResponse{}), nil
}
