package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/telemetry"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// Observe serves the five dimensions of any entity. State and Events
// read the plane of truth (Temporal); Logs, Metrics, and Trace proxy
// the telemetry backend once one is configured.
type Observe struct {
	Bundles *nsbundle.Manager
	// Management supplies the entity describe path State reuses.
	Management *Management
	// The telemetry read drivers; nil means that dimension has no
	// backend configured.
	LogsBackend    telemetry.Logs
	MetricsBackend telemetry.Metrics
	TracesBackend  telemetry.Traces
	// Hub is the LIVE half: follow streams are pushed from the
	// collector, never polled from a backend.
	Hub *telemetry.Hub
	Log *xlog.Logger
}

// State returns dimension 1: execution status plus, for entity refs,
// the full record.
func (o *Observe) State(ctx context.Context, creq *connect.Request[managementv1.ObserveStateRequest]) (*connect.Response[managementv1.ObserveStateResponse], error) {
	req := creq.Msg
	b, err := o.Management.allow(ctx, authz.VerbWatch, authz.KindOf(req.GetRef()))
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
	b, err := o.Management.allow(ctx, authz.VerbWatch, authz.KindOf(req.GetRef()))
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

// Logs streams dimension 3: the backend's history first, then — with
// follow — the live push straight from the collector. The
// subscription opens BEFORE the history is read and the seam is
// deduplicated by time, so the moment between "read the past" and
// "listen to the present" cannot lose a line.
func (o *Observe) Logs(ctx context.Context, creq *connect.Request[managementv1.LogsRequest], stream *connect.ServerStream[managementv1.LogChunk]) error {
	req := creq.Msg
	// The raw view: the whole store, the backend's own language.
	if req.GetQuery() != "" {
		if _, err := scope(ctx, auth.RoleAdmin); err != nil {
			return asConnectError(err)
		}
		backend, ok := o.LogsBackend.(*telemetry.LogsQL)
		if !ok {
			return connect.NewError(connect.CodeUnimplemented, errNoBackend)
		}
		records, err := backend.RawLogs(ctx, req.GetQuery(), 1000)
		if err != nil {
			return asConnectError(status.Error(codes.InvalidArgument, err.Error()))
		}
		for _, rec := range records {
			if err := sendLog(stream, rec); err != nil {
				return err
			}
		}
		return nil
	}
	namespace, err := scope(ctx, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return asConnectError(err)
	}
	sel := telemetry.SelectorFor(namespace, req.GetRef())
	var sub *telemetry.Subscription
	if req.GetFollow() && o.Hub != nil {
		sub = o.Hub.Subscribe(sel, "log")
		defer sub.Close()
	}
	since := time.Time{}
	if req.GetSinceUnixNano() > 0 {
		since = time.Unix(0, req.GetSinceUnixNano())
	}
	last := since
	if o.LogsBackend != nil {
		records, err := o.LogsBackend.Query(ctx, sel, since, 1000)
		if err != nil && sub == nil {
			return asConnectError(status.Error(codes.Unavailable, err.Error()))
		}
		for _, rec := range records {
			if err := sendLog(stream, rec); err != nil {
				return err
			}
			if rec.Time.After(last) {
				last = rec.Time
			}
		}
	} else if sub == nil {
		return connect.NewError(connect.CodeUnimplemented, errNoBackend)
	}
	if sub == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case env, ok := <-sub.C():
			if !ok {
				return nil
			}
			if d := sub.Dropped(); d > 0 {
				if err := stream.Send(&managementv1.LogChunk{Chunk: &managementv1.LogChunk_Dropped{Dropped: d}}); err != nil {
					return err
				}
			}
			for _, rec := range telemetry.LogRecordsFrom(env) {
				// The seam: history already carried everything up to
				// `last`; the buffer may hold the same lines again.
				if !rec.Time.After(last) {
					continue
				}
				if err := sendLog(stream, rec); err != nil {
					return err
				}
			}
		}
	}
}

// sendLog renders one record chunk.
func sendLog(stream *connect.ServerStream[managementv1.LogChunk], rec telemetry.LogRecord) error {
	return stream.Send(&managementv1.LogChunk{Chunk: &managementv1.LogChunk_Record{Record: &managementv1.LogRecord{
		TimeUnixNano: rec.Time.UnixNano(),
		Severity:     rec.Severity,
		Body:         rec.Body,
		Attributes:   rec.Attributes,
	}}})
}

// Metrics streams dimension 4: one snapshot chunk — the backend's own
// PromQL range JSON — then, with follow, live OTLP metric batches of
// this subject, decodable by any standard OTel library.
func (o *Observe) Metrics(ctx context.Context, creq *connect.Request[managementv1.MetricsRequest], stream *connect.ServerStream[managementv1.MetricsChunk]) error {
	req := creq.Msg
	if req.GetQuery() != "" {
		if _, err := scope(ctx, auth.RoleAdmin); err != nil {
			return asConnectError(err)
		}
		backend, ok := o.MetricsBackend.(*telemetry.PromQL)
		if !ok {
			return connect.NewError(connect.CodeUnimplemented, errNoBackend)
		}
		end := time.Now()
		if req.GetEndUnixNano() > 0 {
			end = time.Unix(0, req.GetEndUnixNano())
		}
		start := end.Add(-time.Hour)
		if req.GetStartUnixNano() > 0 {
			start = time.Unix(0, req.GetStartUnixNano())
		}
		raw, err := backend.RawMetrics(ctx, req.GetQuery(), start, end)
		if err != nil {
			return asConnectError(status.Error(codes.InvalidArgument, err.Error()))
		}
		return stream.Send(&managementv1.MetricsChunk{Chunk: &managementv1.MetricsChunk_Snapshot{Snapshot: raw}})
	}
	namespace, err := scope(ctx, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return asConnectError(err)
	}
	sel := telemetry.SelectorFor(namespace, req.GetRef())
	var sub *telemetry.Subscription
	if req.GetFollow() && o.Hub != nil {
		sub = o.Hub.Subscribe(sel, "metric")
		defer sub.Close()
	}
	if o.MetricsBackend != nil {
		end := time.Now()
		if req.GetEndUnixNano() > 0 {
			end = time.Unix(0, req.GetEndUnixNano())
		}
		start := end.Add(-time.Hour)
		if req.GetStartUnixNano() > 0 {
			start = time.Unix(0, req.GetStartUnixNano())
		}
		series, serr := o.MetricsBackend.Series(ctx, sel, start, end)
		if serr != nil && sub == nil {
			return asConnectError(status.Error(codes.Unavailable, serr.Error()))
		}
		if serr == nil {
			if err := stream.Send(&managementv1.MetricsChunk{Chunk: &managementv1.MetricsChunk_Snapshot{Snapshot: series}}); err != nil {
				return err
			}
		}
	} else if sub == nil {
		return connect.NewError(connect.CodeUnimplemented, errNoBackend)
	}
	return followOtlp(ctx, sub,
		func(raw []byte) error {
			return stream.Send(&managementv1.MetricsChunk{Chunk: &managementv1.MetricsChunk_Otlp{Otlp: raw}})
		},
		func(d int64) error {
			return stream.Send(&managementv1.MetricsChunk{Chunk: &managementv1.MetricsChunk_Dropped{Dropped: d}})
		})
}

// Trace streams dimension 5: one snapshot chunk — Jaeger JSON of the
// subject's traces — then, with follow, live OTLP span batches.
func (o *Observe) Trace(ctx context.Context, creq *connect.Request[managementv1.TraceRequest], stream *connect.ServerStream[managementv1.TraceChunk]) error {
	req := creq.Msg
	if req.GetQuery() != "" {
		if _, err := scope(ctx, auth.RoleAdmin); err != nil {
			return asConnectError(err)
		}
		backend, ok := o.TracesBackend.(*telemetry.Jaeger)
		if !ok {
			return connect.NewError(connect.CodeUnimplemented, errNoBackend)
		}
		raw, err := backend.RawTraces(ctx, req.GetQuery())
		if err != nil {
			return asConnectError(status.Error(codes.InvalidArgument, err.Error()))
		}
		return stream.Send(&managementv1.TraceChunk{Chunk: &managementv1.TraceChunk_Snapshot{Snapshot: raw}})
	}
	namespace, err := scope(ctx, auth.RoleAdmin, auth.RoleRun)
	if err != nil {
		return asConnectError(err)
	}
	sel := telemetry.SelectorFor(namespace, req.GetRef())
	var sub *telemetry.Subscription
	if req.GetFollow() && o.Hub != nil {
		sub = o.Hub.Subscribe(sel, "span")
		defer sub.Close()
	}
	if o.TracesBackend != nil {
		trace, serr := o.TracesBackend.Search(ctx, sel, 20)
		if serr != nil && sub == nil {
			return asConnectError(status.Error(codes.Unavailable, serr.Error()))
		}
		if serr == nil {
			if err := stream.Send(&managementv1.TraceChunk{Chunk: &managementv1.TraceChunk_Snapshot{Snapshot: trace}}); err != nil {
				return err
			}
		}
	} else if sub == nil {
		return connect.NewError(connect.CodeUnimplemented, errNoBackend)
	}
	return followOtlp(ctx, sub,
		func(raw []byte) error {
			return stream.Send(&managementv1.TraceChunk{Chunk: &managementv1.TraceChunk_Otlp{Otlp: raw}})
		},
		func(d int64) error {
			return stream.Send(&managementv1.TraceChunk{Chunk: &managementv1.TraceChunk_Dropped{Dropped: d}})
		})
}

// followOtlp drains a live subscription into OTLP chunks; a nil
// subscription ends the stream after the snapshot.
func followOtlp(ctx context.Context, sub *telemetry.Subscription, send func([]byte) error, sendDropped func(int64) error) error {
	if sub == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case env, ok := <-sub.C():
			if !ok {
				return nil
			}
			if d := sub.Dropped(); d > 0 {
				if err := sendDropped(d); err != nil {
					return err
				}
			}
			raw, err := proto.Marshal(env.Payload)
			if err != nil {
				continue
			}
			if err := send(raw); err != nil {
				return err
			}
		}
	}
}

var errNoBackend = fmt.Errorf("no telemetry backend is configured behind the collector yet")

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
