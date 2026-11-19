package standflow

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

func TestSpecValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec Spec
		ok   bool
	}{
		{name: "pipeline stand", spec: Spec{PipelineId: "stroppy"}, ok: true},
		{name: "named, namespace-owned, no lifetime", spec: Spec{}, ok: true},
		{name: "named with owner and lifetime", spec: Spec{Owner: "pipeline/stroppy-stand", TTL: "168h", IdleTimeout: "72h"}, ok: true},
		{name: "pipeline stand with owner", spec: Spec{PipelineId: "p", Owner: "pipeline/q"}},
		{name: "bad ttl", spec: Spec{TTL: "a week"}},
		{name: "negative idle", spec: Spec{IdleTimeout: "-1h"}},
		{name: "bad owner", spec: Spec{Owner: "nokind"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := c.spec.Validate() == nil; got != c.ok {
				t.Fatalf("Validate ok=%v, want %v", got, c.ok)
			}
		})
	}
}

func TestOutlived(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	past, future := now.Add(-time.Minute), now.Add(time.Minute)
	cases := []struct {
		name string
		st   State
		want bool
	}{
		{name: "no lifetime", st: State{LastActive: now.Add(-1000 * time.Hour)}},
		{name: "ttl passed", st: State{ExpiresAt: &past}, want: true},
		{name: "ttl ahead", st: State{ExpiresAt: &future}},
		{name: "idle out", st: State{IdleTimeout: time.Hour, LastActive: now.Add(-2 * time.Hour)}, want: true},
		{name: "recently used", st: State{IdleTimeout: time.Hour, LastActive: now.Add(-time.Minute)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := outlived(&c.st, now); got != c.want {
				t.Fatalf("outlived = %v, want %v", got, c.want)
			}
		})
	}
}

func BenchmarkOutlived(b *testing.B) {
	now := time.Now()
	st := State{IdleTimeout: time.Hour, LastActive: now.Add(-time.Minute)}
	for b.Loop() {
		outlived(&st, now)
	}
}

// update sends one command to the stand under test.
func update(t *testing.T, env *testsuite.TestWorkflowEnvironment, name string, payload any, at time.Duration) {
	t.Helper()
	env.RegisterDelayedCallback(func() {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		env.UpdateWorkflow(name, fmt.Sprint(at), &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("%s rejected: %v", name, err) },
			OnComplete: func(_ any, err error) {
				if err != nil {
					t.Errorf("%s: %v", name, err)
				}
			},
		}, map[string]any{"requestId": fmt.Sprint(at), "payload": json.RawMessage(raw)})
	}, at)
}

// A stand whose own TTL or idle timeout runs out asks for its deletion
// once; a TTL removed in time keeps it.
func TestStandLifetime(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		spec    Spec
		steps   func(*testing.T, *testsuite.TestWorkflowEnvironment)
		expires bool
	}{
		{name: "ttl runs out", spec: Spec{TTL: "1h"}, expires: true},
		{name: "idle runs out", spec: Spec{IdleTimeout: "30m"}, expires: true},
		{name: "use keeps an idle stand", spec: Spec{IdleTimeout: "30m"}, steps: func(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
			for at := 20 * time.Minute; at < 3*time.Hour; at += 20 * time.Minute {
				update(t, env, "extend", ExtendCmd{}, at)
			}
		}},
		{name: "ttl removed in time", spec: Spec{TTL: "1h"}, steps: func(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
			update(t, env, "set-ttl", SetTTLCmd{}, 10*time.Minute)
		}},
		{name: "ttl set later", spec: Spec{}, expires: true, steps: func(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
			update(t, env, "set-ttl", SetTTLCmd{TTL: "30m"}, 10*time.Minute)
		}},
		{name: "no lifetime", spec: Spec{Owner: ref.OwnerRef("pipeline/stroppy-stand")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			if err := New(time.Minute).Register(env); err != nil {
				t.Fatal(err)
			}
			env.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, activity.RegisterOptions{Name: CascadeActivity})
			env.RegisterActivityWithOptions(func(context.Context, string) (bool, error) { return false, nil }, activity.RegisterOptions{Name: RunActiveActivity})
			expired := 0
			env.RegisterActivityWithOptions(func(context.Context) error { expired++; return nil }, activity.RegisterOptions{Name: ExpireActivity})
			if c.steps != nil {
				c.steps(t, env)
			}
			env.RegisterDelayedCallback(env.CancelWorkflow, 3*time.Hour)
			env.ExecuteWorkflow(string(Kind), map[string]any{"spec": c.spec})
			want := 0
			if c.expires {
				want = 1
			}
			if expired != want {
				t.Fatalf("expire asked %d times, want %d", expired, want)
			}
		})
	}
}
