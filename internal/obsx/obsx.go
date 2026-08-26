// Package obsx makes the SERVER's own records observable. Dimensions
// 1 and 2 (state, events) a record answers by itself; 3, 4 and 5 —
// logs, metrics and traces — have to be EMITTED, and until they are,
// asking for them returns an honest emptiness that looks like a
// working feature.
//
// Everything here hangs off ONE interceptor rather than off calls
// scattered through the activities: an activity added tomorrow is
// observable the day it is written, without anybody remembering to
// instrument it. That is the difference between "we have telemetry"
// and "every entity has five dimensions".
package obsx

import (
	"context"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"

	"github.com/graphene-ci/pipeline/pkg/obs"
)

// Metric names of the server's own contour. Names are stable: a
// dashboard outlives a refactor.
const (
	// MetricActivity counts activity executions by outcome.
	MetricActivity = "graphene.server.activity"
	// MetricActivityDuration measures how long they take, in seconds.
	MetricActivityDuration = "graphene.server.activity.seconds"
	// MetricRetry counts RETRIED attempts — the signal behind the
	// silent backoff that costs a day of debugging when invisible.
	MetricRetry = "graphene.server.activity.retry"
)

// Interceptor stamps every server activity with the entity it works
// on and emits its telemetry.
type Interceptor struct {
	interceptor.WorkerInterceptorBase
}

// InterceptActivity wraps the activity chain.
func (i *Interceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityInbound{root: next}
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	root interceptor.ActivityInboundInterceptor
}

func (a *activityInbound) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	a.ActivityInboundInterceptorBase.Next = a.root
	return a.root.Init(outbound)
}

// ExecuteActivity is where a server activity becomes observable: the
// entity it belongs to, a span, a duration, and — when the attempt is
// not the first — the retry that would otherwise be invisible.
func (a *activityInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	info := activity.GetInfo(ctx)
	entityRef := EntityOf(info.WorkflowExecution.ID)
	name := info.ActivityType.Name

	ctx = obs.WithEntity(ctx, entityRef)
	ctx, end := obs.Span(ctx, name)

	if info.Attempt > 1 {
		// A retry is a fact about the ENTITY, not only about the
		// worker: it is why a record sits in "creating" with nothing
		// apparently happening.
		obs.Count(ctx, MetricRetry, 1, obs.Str("activity", name))
		obs.Warn(ctx, "activity retrying",
			obs.Str("activity", name), obs.Int("attempt", int(info.Attempt)))
	}

	started := time.Now()
	res, err := a.root.ExecuteActivity(ctx, in)
	elapsed := time.Since(started).Seconds()

	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	obs.Count(ctx, MetricActivity, 1, obs.Str("activity", name), obs.Str("outcome", outcome))
	obs.Measure(ctx, MetricActivityDuration, elapsed, obs.Str("activity", name), obs.Str("outcome", outcome))
	if err != nil {
		obs.Error(ctx, "activity failed", obs.Str("activity", name), obs.Err(err))
	}
	end(err)
	return res, err
}

// EntityOf renders the entity reference a workflow id belongs to. A
// record's workflow id IS its reference ("pipeline/x"); a run's is
// "run/<id>", which is a record too.
func EntityOf(workflowID string) string {
	if workflowID == "" {
		return ""
	}
	if _, _, ok := strings.Cut(workflowID, "/"); ok {
		return workflowID
	}
	return workflowID
}
