package services

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/graphene-ci/temporal-entity/pkg/entity"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/selector"
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

// birthSlack widens the record's lower bound: a signal's timestamp is set
// on the machine that emitted it, the record's birth on the server, and
// the two clocks are not one.
const birthSlack = 5 * time.Second

// subject is the authorized selector of a record's dimensions 3-5. The
// read is authorized the way every other door verb is — through the
// namespace's roles and bindings, so a service account's token watches
// exactly what it may get. The selector is bounded by the record's birth,
// so a reused name does not inherit the signals of its previous bearer.
func (o *Observe) subject(ctx context.Context, ref string) (telemetry.Selector, error) {
	b, err := o.Management.allow(ctx, authz.VerbWatch, authz.KindOf(ref))
	if err != nil {
		return telemetry.Selector{}, err
	}
	sel := telemetry.SelectorFor(b.Namespace, ref)
	// A run's id is unique — its signals cannot be another run's.
	if !strings.HasPrefix(ref, "run/") {
		if born := recordBirth(ctx, b.Client, ref); !born.IsZero() {
			sel.Since = born.Add(-birthSlack)
		}
	}
	return sel, nil
}

// recordBirth is when the record behind ref came to be: the start of the
// FIRST run of its workflow chain — a record continues-as-new through its
// life, and the latest run's start is only the last turn of it. Unknown
// (no such workflow, the first run already past retention) is the zero
// time: showing too much beats hiding a long-lived record's own history.
func recordBirth(ctx context.Context, cl workflowDescriber, ref string) time.Time {
	latest, err := cl.DescribeWorkflowExecution(ctx, ref, "")
	if err != nil {
		return time.Time{}
	}
	info := latest.GetWorkflowExecutionInfo()
	first := info.GetFirstRunId()
	if first == "" || first == info.GetExecution().GetRunId() {
		return info.GetStartTime().AsTime()
	}
	origin, err := cl.DescribeWorkflowExecution(ctx, ref, first)
	if err != nil {
		return time.Time{}
	}
	return origin.GetWorkflowExecutionInfo().GetStartTime().AsTime()
}

// workflowDescriber is the one Temporal call recordBirth needs.
type workflowDescriber interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

// watchRaw gates the RAW query surface — the whole store in the
// backend's own language, no subject filter. A static admin token
// passes; any other principal must hold the narrowest admin-only
// right the resolver knows (invoke on service accounts).
func (o *Observe) watchRaw(ctx context.Context) error {
	if _, err := scope(ctx, auth.RoleAdmin); err == nil {
		return nil
	}
	if _, err := o.Management.allow(ctx, authz.VerbInvoke, authz.KindServiceAccount); err != nil {
		return status.Error(codes.PermissionDenied, "the raw query surface is an administrator's")
	}
	return nil
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
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				return asConnectError(status.Errorf(codes.NotFound, "no record %s", req.GetRef()))
			}
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
		if len(req.GetKinds()) > 0 && !slices.Contains(req.GetKinds(), ev.GetKind()) {
			continue
		}
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	return nil
}

// scopeOf resolves the two query forms of a dimension read. RAW — a query
// without a ref: the whole store, an administrator's. SCOPED and plain —
// a ref: the record's own selector, authorized like any read of the
// record; a query, if any, is evaluated inside that scope by the backend.
func (o *Observe) scopeOf(ctx context.Context, ref, query string) (sel telemetry.Selector, raw bool, err error) {
	if ref == "" {
		if query == "" {
			return telemetry.Selector{}, false, status.Error(codes.InvalidArgument, "name a record (ref) or, as an administrator, a raw query")
		}
		if err := o.watchRaw(ctx); err != nil {
			return telemetry.Selector{}, false, err
		}
		return telemetry.Selector{}, true, nil
	}
	sel, err = o.subject(ctx, ref)
	return sel, false, err
}

// backendError maps a telemetry backend's failure onto the door's codes:
// the query's fault is InvalidArgument, the backend's Unavailable, a form
// the backend cannot serve Unimplemented.
func backendError(err error) error {
	var be *telemetry.BackendError
	var ce *telemetry.ClientError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, telemetry.ErrScopedQueryUnsupported):
		return connect.NewError(connect.CodeUnimplemented, err)
	case errors.As(err, &ce):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.As(err, &be) && be.ClientFault():
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return connect.NewError(connect.CodeUnavailable, err)
}

// logQueryOf reads a log selection off the wire; the cursor is the opaque
// token of a previous page.
func logQueryOf(req *managementv1.LogsRequest) (telemetry.LogQuery, error) {
	q := telemetry.LogQuery{
		Limit:      int(req.GetLimit()),
		Filter:     req.GetQuery(),
		Severities: req.GetSeverities(),
		Text:       req.GetText(),
		Attributes: map[string]string{},
	}
	if req.GetSinceUnixNano() > 0 {
		q.Since = time.Unix(0, req.GetSinceUnixNano())
	}
	if req.GetUntilUnixNano() > 0 {
		q.Until = time.Unix(0, req.GetUntilUnixNano())
	}
	switch req.GetOrder() {
	case "", "asc":
	case "desc":
		q.Desc = true
	default:
		return q, status.Errorf(codes.InvalidArgument, "order %q: want asc or desc", req.GetOrder())
	}
	if q.Limit > telemetry.MaxLogLimit {
		return q, status.Errorf(codes.InvalidArgument, "limit %d is above %d", q.Limit, telemetry.MaxLogLimit)
	}
	for attr, value := range map[string]string{
		"stream": req.GetStream(), "graphene.agent": req.GetAgent(), "graphene.entity": req.GetEntity(),
	} {
		if value != "" {
			q.Attributes[attr] = value
		}
	}
	if tok := req.GetPageToken(); tok != "" {
		cursor, err := decodeCursor(tok)
		if err != nil {
			return q, status.Error(codes.InvalidArgument, "page token: "+err.Error())
		}
		q.Cursor = cursor
	}
	return q, nil
}

// followable says whether a selection can be followed: follow reads
// forward from the present, so it takes neither desc nor a page token;
// and the door cannot evaluate the backend's own language on live
// records, so a follow takes the selection's fields but no query.
func followable(follow bool, q telemetry.LogQuery) error {
	switch {
	case !follow:
		return nil
	case q.Desc || !q.Cursor.IsZero():
		return errors.New("follow reads forward from the present: it takes neither desc nor a page token")
	case strings.TrimSpace(q.Filter) != "":
		return errors.New("follow takes the selection's fields (severities, stream, agent, entity, text) but no query: live records cannot be filtered in the backend's language")
	}
	return nil
}

// The page token is the cursor, base64 of "<unix nanos>:<skip>".
func encodeCursor(c telemetry.LogCursor) string {
	if c.IsZero() {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(c.Time.UnixNano(), 10) + ":" + strconv.Itoa(c.Skip)))
}

func decodeCursor(tok string) (telemetry.LogCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return telemetry.LogCursor{}, err
	}
	at, skip, ok := strings.Cut(string(raw), ":")
	nanos, err1 := strconv.ParseInt(at, 10, 64)
	n, err2 := strconv.Atoi(skip)
	if !ok || err1 != nil || err2 != nil || n < 0 {
		return telemetry.LogCursor{}, errors.New("not a page token of this door")
	}
	return telemetry.LogCursor{Time: time.Unix(0, nanos), Skip: n}, nil
}

// Logs streams dimension 3: the selection's history first — one page,
// closed by a page chunk that says how much came and whether more is
// there — then, with follow, the live push straight from the collector.
// The subscription opens BEFORE the history is read and the seam is
// deduplicated by time, so the moment between "read the past" and "listen
// to the present" cannot lose a line.
func (o *Observe) Logs(ctx context.Context, creq *connect.Request[managementv1.LogsRequest], stream *connect.ServerStream[managementv1.LogChunk]) error {
	req := creq.Msg
	sel, raw, err := o.scopeOf(ctx, req.GetRef(), req.GetQuery())
	if err != nil {
		return asConnectError(err)
	}
	if raw {
		backend, ok := o.LogsBackend.(*telemetry.LogsQL)
		if !ok {
			return connect.NewError(connect.CodeUnimplemented, errNoBackend)
		}
		records, err := backend.RawLogs(ctx, req.GetQuery(), int(req.GetLimit()))
		if err != nil {
			return backendError(err)
		}
		for _, rec := range records {
			if err := sendLog(stream, rec); err != nil {
				return err
			}
		}
		return stream.Send(&managementv1.LogChunk{Chunk: &managementv1.LogChunk_Page{Page: &managementv1.LogPage{Returned: int32(len(records))}}}) //nolint:gosec // bounded by the limit
	}
	q, err := logQueryOf(req)
	if err != nil {
		return asConnectError(err)
	}
	if err := followable(req.GetFollow(), q); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	var sub *telemetry.Subscription
	if req.GetFollow() && o.Hub != nil {
		sub = o.Hub.Subscribe(sel, "log")
		defer sub.Close()
	}
	last := q.Since
	if o.LogsBackend != nil {
		page, err := o.LogsBackend.Query(ctx, sel, q)
		if err != nil {
			// The past is part of the answer; a live tail is no substitute
			// for it.
			return backendError(err)
		}
		for _, rec := range page.Records {
			if err := sendLog(stream, rec); err != nil {
				return err
			}
			if rec.Time.After(last) {
				last = rec.Time
			}
		}
		closing := &managementv1.LogPage{Returned: int32(len(page.Records)), Truncated: page.Truncated} //nolint:gosec // bounded by the limit
		if page.Truncated {
			closing.NextPageToken = encodeCursor(page.Next)
		}
		if err := stream.Send(&managementv1.LogChunk{Chunk: &managementv1.LogChunk_Page{Page: closing}}); err != nil {
			return err
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
				// `last`; the buffer may hold the same lines again. And the
				// selection holds for the present as it held for the past.
				if !rec.Time.After(last) || !q.Admits(rec) {
					continue
				}
				if err := sendLog(stream, rec); err != nil {
					return err
				}
			}
		}
	}
}

// LogFacets counts the values of fields within the same selection Logs
// would return — a UI's filter menu with its numbers.
func (o *Observe) LogFacets(ctx context.Context, creq *connect.Request[managementv1.LogFacetsRequest]) (*connect.Response[managementv1.LogFacetsResponse], error) {
	req := creq.Msg
	selection := req.GetSelection()
	if selection.GetRef() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("facets are counted within one record: name its ref"))
	}
	if len(req.GetFields()) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name the fields to count"))
	}
	sel, _, err := o.scopeOf(ctx, selection.GetRef(), selection.GetQuery())
	if err != nil {
		return nil, asConnectError(err)
	}
	q, err := logQueryOf(selection)
	if err != nil {
		return nil, asConnectError(err)
	}
	if o.LogsBackend == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errNoBackend)
	}
	facets, err := o.LogsBackend.Facets(ctx, sel, q, req.GetFields(), int(req.GetLimit()))
	if err != nil {
		return nil, backendError(err)
	}
	out := &managementv1.LogFacetsResponse{}
	for _, f := range facets {
		facet := &managementv1.LogFacetsResponse_Facet{Field: f.Field}
		for _, v := range f.Values {
			facet.Values = append(facet.Values, &managementv1.LogFacetsResponse_Value{Value: v.Value, Hits: v.Hits})
		}
		out.Facets = append(out.Facets, facet)
	}
	return connect.NewResponse(out), nil
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

// metricsQueryOf reads the window and resolution off the wire: end now,
// start an hour before, unless said otherwise.
func metricsQueryOf(req *managementv1.MetricsRequest) telemetry.MetricsQuery {
	q := telemetry.MetricsQuery{End: time.Now(), Step: time.Duration(req.GetStepSeconds()) * time.Second}
	if req.GetEndUnixNano() > 0 {
		q.End = time.Unix(0, req.GetEndUnixNano())
	}
	q.Start = q.End.Add(-time.Hour)
	if req.GetStartUnixNano() > 0 {
		q.Start = time.Unix(0, req.GetStartUnixNano())
	}
	return q
}

// Metrics streams dimension 4: one snapshot chunk — the backend's own
// PromQL range JSON of the record's series, or of the caller's expression
// evaluated inside the record's scope — then, with follow, live OTLP
// metric batches of the record.
func (o *Observe) Metrics(ctx context.Context, creq *connect.Request[managementv1.MetricsRequest], stream *connect.ServerStream[managementv1.MetricsChunk]) error {
	req := creq.Msg
	sel, raw, err := o.scopeOf(ctx, req.GetRef(), req.GetQuery())
	if err != nil {
		return asConnectError(err)
	}
	q := metricsQueryOf(req)
	if raw {
		backend, ok := o.MetricsBackend.(*telemetry.PromQL)
		if !ok {
			return connect.NewError(connect.CodeUnimplemented, errNoBackend)
		}
		snapshot, err := backend.RawMetrics(ctx, req.GetQuery(), q)
		if err != nil {
			return backendError(err)
		}
		return stream.Send(&managementv1.MetricsChunk{Chunk: &managementv1.MetricsChunk_Snapshot{Snapshot: snapshot}})
	}
	q.Expr = req.GetQuery()
	var sub *telemetry.Subscription
	if req.GetFollow() && o.Hub != nil {
		sub = o.Hub.Subscribe(sel, "metric")
		defer sub.Close()
	}
	if o.MetricsBackend != nil {
		series, serr := o.MetricsBackend.Series(ctx, sel, q)
		if serr != nil && sub == nil {
			return backendError(serr)
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
// record's traces, narrowed by the caller's search parameters inside the
// record's scope — then, with follow, live OTLP span batches.
func (o *Observe) Trace(ctx context.Context, creq *connect.Request[managementv1.TraceRequest], stream *connect.ServerStream[managementv1.TraceChunk]) error {
	req := creq.Msg
	sel, raw, err := o.scopeOf(ctx, req.GetRef(), req.GetQuery())
	if err != nil {
		return asConnectError(err)
	}
	if raw {
		backend, ok := o.TracesBackend.(*telemetry.Jaeger)
		if !ok {
			return connect.NewError(connect.CodeUnimplemented, errNoBackend)
		}
		snapshot, err := backend.RawTraces(ctx, req.GetQuery())
		if err != nil {
			return backendError(err)
		}
		return stream.Send(&managementv1.TraceChunk{Chunk: &managementv1.TraceChunk_Snapshot{Snapshot: snapshot}})
	}
	var sub *telemetry.Subscription
	if req.GetFollow() && o.Hub != nil {
		sub = o.Hub.Subscribe(sel, "span")
		defer sub.Close()
	}
	if o.TracesBackend != nil {
		trace, serr := o.TracesBackend.Search(ctx, sel, req.GetQuery(), int(req.GetLimit()))
		if serr != nil && sub == nil {
			return backendError(serr)
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
		ev.ActivityId = strconv.FormatInt(scheduledId, 10)
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
		ev.Status = selector.PhaseCompleted
		ev.Result = payloadsJSON(he.GetWorkflowExecutionCompletedEventAttributes().GetResult())
	case he.GetWorkflowExecutionFailedEventAttributes() != nil:
		ev.Kind = "run-failed"
		ev.Status = selector.PhaseFailed
		ev.Error = he.GetWorkflowExecutionFailedEventAttributes().GetFailure().GetMessage()
	case he.GetWorkflowExecutionCanceledEventAttributes() != nil:
		ev.Kind = "run-canceled"
		ev.Status = selector.PhaseCanceled
	case he.GetWorkflowExecutionTerminatedEventAttributes() != nil:
		ev.Kind = "run-terminated"
		ev.Status = selector.PhaseTerminated
	case he.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil:
		ev.Kind = "run-continued-as-new"
	case he.GetActivityTaskScheduledEventAttributes() != nil:
		a := he.GetActivityTaskScheduledEventAttributes()
		ev.Kind = "activity-scheduled"
		ev.ActivityId = strconv.FormatInt(he.GetEventId(), 10)
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
		if a.GetSignalName() == entity.NoteSignalName {
			// A milestone the pipeline emitted: its own kind, its own
			// name as the subject, its payload as the input — not the
			// envelope the signal carried it in.
			ev.Kind = "note"
			var note struct {
				Name    string          `json:"name"`
				Payload json.RawMessage `json:"payload"`
			}
			if json.Unmarshal(ev.Input, &note) == nil && note.Name != "" {
				ev.Subject, ev.Input = note.Name, note.Payload
			}
		}
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
