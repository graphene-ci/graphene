package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// Observe serves the five dimensions of any entity. State and Events
// read the plane of truth (Temporal); Logs, Metrics, and Trace proxy
// the telemetry backend once one is configured.
type Observe struct {
	Bundles *nsbundle.Manager
	// Management supplies the entity describe path State reuses.
	Management *Management
	Log        *xlog.Logger
}

// State returns dimension 1: execution status plus, for entity refs,
// the full record.
func (o *Observe) State(ctx context.Context, creq *connect.Request[managementv1.ObserveStateRequest]) (*connect.Response[managementv1.ObserveStateResponse], error) {
	req := creq.Msg
	b, err := bundleFor(ctx, o.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return nil, asConnectError(err)
	}
	desc, err := b.Client.DescribeWorkflowExecution(ctx, req.GetRef(), "")
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}
	out := &managementv1.ObserveStateResponse{
		Status: desc.GetWorkflowExecutionInfo().GetStatus().String(),
	}
	if !strings.HasPrefix(req.GetRef(), "run/") {
		if res, err := o.Management.describe(ctx, b, req.GetRef()); err == nil {
			out.Resource = res
		}
	}
	return connect.NewResponse(out), nil
}

// Events streams dimension 2: the workflow history of the ref,
// translated event by event. Nothing is omitted — unclassified events
// pass through as internal-*; raw always carries the whole event.
func (o *Observe) Events(ctx context.Context, creq *connect.Request[managementv1.EventsRequest], stream *connect.ServerStream[managementv1.Event]) error {
	req := creq.Msg
	b, err := bundleFor(ctx, o.Bundles, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return asConnectError(err)
	}
	iter := b.Client.GetWorkflowHistory(ctx, req.GetRef(), "", req.GetFollow(), enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	// The activity an event belongs to is named only on its Scheduled
	// event; the rest reference it by scheduled_event_id.
	sched := map[int64]*historypb.ActivityTaskScheduledEventAttributes{}
	for iter.HasNext() {
		he, err := iter.Next()
		if err != nil {
			return asConnectError(status.Error(codes.Internal, err.Error()))
		}
		if a := he.GetActivityTaskScheduledEventAttributes(); a != nil {
			sched[he.GetEventId()] = a
		}
		ev := translate(he, sched)
		if he.GetEventId() <= req.GetAfterEventId() {
			continue
		}
		if req.GetActivityId() != "" && !belongsTo(he, sched, req.GetActivityId()) {
			continue
		}
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	return nil
}

// Logs, Metrics, and Trace await the telemetry backend decision; the
// collector fan-out is already in place.
func (o *Observe) Logs(context.Context, *connect.Request[managementv1.LogsRequest], *connect.ServerStream[managementv1.LogRecord]) error {
	return connect.NewError(connect.CodeUnimplemented, errNoBackend)
}

// Metrics — see Logs.
func (o *Observe) Metrics(context.Context, *connect.Request[managementv1.MetricsRequest]) (*connect.Response[managementv1.MetricsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errNoBackend)
}

// Trace — see Logs.
func (o *Observe) Trace(context.Context, *connect.Request[managementv1.TraceRequest]) (*connect.Response[managementv1.TraceResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errNoBackend)
}

var errNoBackend = fmt.Errorf("no telemetry backend is configured behind the collector yet")

// asConnectError translates the shared helpers' gRPC statuses on the
// STREAMING paths, where the unary interceptor cannot.
func asConnectError(err error) error {
	if s, ok := status.FromError(err); ok {
		return connect.NewError(connect.Code(s.Code()), fmt.Errorf("%s", s.Message()))
	}
	return err
}

// translate renders one history event: classification without
// omission.
func translate(he *historypb.HistoryEvent, sched map[int64]*historypb.ActivityTaskScheduledEventAttributes) *managementv1.Event {
	ev := &managementv1.Event{
		EventId:      he.GetEventId(),
		TimeUnixNano: he.GetEventTime().AsTime().UnixNano(),
	}
	if raw, err := protojson.Marshal(he); err == nil {
		ev.Raw = raw
	}
	subjectOf := func(scheduledId int64) {
		if a := sched[scheduledId]; a != nil {
			ev.Subject = a.GetActivityType().GetName()
			ev.Agent = agentOfQueue(a.GetTaskQueue().GetName())
		}
	}
	switch {
	case he.GetWorkflowExecutionStartedEventAttributes() != nil:
		ev.Kind = "run-started"
		ev.Input = payloadsJSON(he.GetWorkflowExecutionStartedEventAttributes().GetInput())
	case he.GetWorkflowExecutionCompletedEventAttributes() != nil:
		ev.Kind = "run-completed"
		ev.Status = "Completed"
		ev.Result = payloadsJSON(he.GetWorkflowExecutionCompletedEventAttributes().GetResult())
	case he.GetWorkflowExecutionFailedEventAttributes() != nil:
		ev.Kind = "run-failed"
		ev.Status = "Failed"
		ev.Error = he.GetWorkflowExecutionFailedEventAttributes().GetFailure().GetMessage()
	case he.GetWorkflowExecutionCanceledEventAttributes() != nil:
		ev.Kind = "run-canceled"
		ev.Status = "Canceled"
	case he.GetWorkflowExecutionTerminatedEventAttributes() != nil:
		ev.Kind = "run-terminated"
		ev.Status = "Terminated"
	case he.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil:
		ev.Kind = "run-continued-as-new"
	case he.GetActivityTaskScheduledEventAttributes() != nil:
		a := he.GetActivityTaskScheduledEventAttributes()
		ev.Kind = "activity-scheduled"
		ev.Subject = a.GetActivityType().GetName()
		ev.Agent = agentOfQueue(a.GetTaskQueue().GetName())
		ev.Input = payloadsJSON(a.GetInput())
	case he.GetActivityTaskStartedEventAttributes() != nil:
		a := he.GetActivityTaskStartedEventAttributes()
		ev.Kind = "activity-started"
		ev.Attempt = a.GetAttempt()
		if f := a.GetLastFailure(); f != nil {
			ev.Error = f.GetMessage()
		}
		subjectOf(a.GetScheduledEventId())
	case he.GetActivityTaskCompletedEventAttributes() != nil:
		a := he.GetActivityTaskCompletedEventAttributes()
		ev.Kind = "activity-completed"
		ev.Result = payloadsJSON(a.GetResult())
		subjectOf(a.GetScheduledEventId())
	case he.GetActivityTaskFailedEventAttributes() != nil:
		a := he.GetActivityTaskFailedEventAttributes()
		ev.Kind = "activity-failed"
		ev.Error = a.GetFailure().GetMessage()
		subjectOf(a.GetScheduledEventId())
	case he.GetActivityTaskTimedOutEventAttributes() != nil:
		a := he.GetActivityTaskTimedOutEventAttributes()
		ev.Kind = "activity-timed-out"
		ev.Error = a.GetFailure().GetMessage()
		subjectOf(a.GetScheduledEventId())
	case he.GetWorkflowExecutionUpdateAcceptedEventAttributes() != nil:
		a := he.GetWorkflowExecutionUpdateAcceptedEventAttributes()
		ev.Kind = "command-received"
		ev.Subject = a.GetAcceptedRequest().GetInput().GetName()
		ev.Input = payloadsJSON(a.GetAcceptedRequest().GetInput().GetArgs())
	case he.GetWorkflowExecutionUpdateCompletedEventAttributes() != nil:
		a := he.GetWorkflowExecutionUpdateCompletedEventAttributes()
		ev.Kind = "command-completed"
		if f := a.GetOutcome().GetFailure(); f != nil {
			ev.Error = f.GetMessage()
		} else {
			ev.Result = payloadsJSON(a.GetOutcome().GetSuccess())
		}
	case he.GetWorkflowExecutionSignaledEventAttributes() != nil:
		a := he.GetWorkflowExecutionSignaledEventAttributes()
		ev.Kind = "signal-received"
		ev.Subject = a.GetSignalName()
		ev.Input = payloadsJSON(a.GetInput())
	default:
		ev.Kind = "internal-" + strings.TrimPrefix(he.GetEventType().String(), "EVENT_TYPE_")
	}
	return ev
}

// belongsTo slices by activity id.
func belongsTo(he *historypb.HistoryEvent, sched map[int64]*historypb.ActivityTaskScheduledEventAttributes, activityId string) bool {
	if a := he.GetActivityTaskScheduledEventAttributes(); a != nil {
		return a.GetActivityId() == activityId
	}
	var scheduledId int64
	switch {
	case he.GetActivityTaskStartedEventAttributes() != nil:
		scheduledId = he.GetActivityTaskStartedEventAttributes().GetScheduledEventId()
	case he.GetActivityTaskCompletedEventAttributes() != nil:
		scheduledId = he.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
	case he.GetActivityTaskFailedEventAttributes() != nil:
		scheduledId = he.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
	case he.GetActivityTaskTimedOutEventAttributes() != nil:
		scheduledId = he.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
	default:
		return false
	}
	a := sched[scheduledId]
	return a != nil && a.GetActivityId() == activityId
}

// agentOfQueue extracts the agent from an "agent/<id>/run/<run>" queue.
func agentOfQueue(queue string) string {
	if rest, ok := strings.CutPrefix(queue, "agent/"); ok {
		if agentId, _, ok := strings.Cut(rest, "/run/"); ok {
			return agentId
		}
	}
	return ""
}

// payloadsJSON renders payloads as a JSON array: json-encoded payloads
// verbatim, everything else base64-quoted. History carries no secret
// values by construction, so nothing is stripped.
func payloadsJSON(p *commonpb.Payloads) []byte {
	items := p.GetPayloads()
	if len(items) == 0 {
		return nil
	}
	parts := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if string(item.GetMetadata()["encoding"]) == "json/plain" && json.Valid(item.GetData()) {
			parts = append(parts, item.GetData())
			continue
		}
		quoted, _ := json.Marshal(base64.StdEncoding.EncodeToString(item.GetData()))
		parts = append(parts, quoted)
	}
	if len(parts) == 1 {
		return parts[0]
	}
	out, _ := json.Marshal(parts)
	return out
}
