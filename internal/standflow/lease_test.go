package standflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/graphene-ci/pipeline/pkg/ref"
)

// leaseStep is one command sent to the stand and the code it must
// answer with ("" — success).
type leaseStep struct {
	at      time.Duration
	name    string
	payload any
	code    string
}

// world is what the server activities answer for the stand under test.
type world struct {
	dirty    []string
	runState func(run string, elapsed time.Duration) string
}

// runLease drives a stand through steps and returns its state at end.
func runLease(t *testing.T, spec Spec, w world, steps []leaseStep, end time.Duration) State {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if err := New(time.Minute).Register(env); err != nil {
		t.Fatal(err)
	}
	start := env.Now()
	env.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, activity.RegisterOptions{Name: CascadeActivity})
	env.RegisterActivityWithOptions(func(context.Context, string) (bool, error) { return false, nil }, activity.RegisterOptions{Name: RunActiveActivity})
	env.RegisterActivityWithOptions(func(context.Context) error { return nil }, activity.RegisterOptions{Name: ExpireActivity})
	env.RegisterActivityWithOptions(func(context.Context) ([]string, error) { return w.dirty, nil }, activity.RegisterOptions{Name: DirtyActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, run string) (string, error) {
		if w.runState == nil {
			return RunRunning, nil
		}
		return w.runState(run, env.Now().Sub(start)), nil
	}, activity.RegisterOptions{Name: RunStateActivity})
	for i, s := range steps {
		env.RegisterDelayedCallback(func() {
			raw, err := json.Marshal(s.payload)
			if err != nil {
				t.Fatal(err)
			}
			check := func(err error) {
				var app *temporal.ApplicationError
				switch {
				case s.code == "" && err != nil:
					t.Errorf("step %d %s: %v", i, s.name, err)
				case s.code != "" && (!errors.As(err, &app) || app.Type() != s.code):
					t.Errorf("step %d %s: err %v, want code %s", i, s.name, err, s.code)
				}
			}
			env.UpdateWorkflow(s.name, fmt.Sprint(i), &testsuite.TestUpdateCallback{
				OnReject:   check,
				OnComplete: func(_ any, err error) { check(err) },
			}, map[string]any{"requestId": fmt.Sprint(i), "payload": json.RawMessage(raw)})
		}, s.at)
	}
	var st State
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow("describe")
		if err != nil {
			t.Fatal(err)
		}
		var described struct{ State State }
		if err := value.Get(&described); err != nil {
			t.Fatal(err)
		}
		st = described.State
		env.CancelWorkflow()
	}, end)
	env.ExecuteWorkflow(string(Kind), map[string]any{"spec": spec})
	return st
}

func acquire(at time.Duration, run, code string) leaseStep {
	return leaseStep{at: at, name: "acquire", payload: AcquireCmd{Run: run}, code: code}
}

func TestLease(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		world  world
		steps  []leaseStep
		end    time.Duration
		holder string
	}{
		{
			name:   "a free stand is taken, again by the same run",
			steps:  []leaseStep{acquire(time.Second, "r1", ""), acquire(2*time.Second, "r1", "")},
			end:    time.Minute / 2,
			holder: "r1",
		},
		{
			name:   "another run is told busy",
			steps:  []leaseStep{acquire(time.Second, "r1", ""), acquire(2*time.Second, "r2", CodeBusy)},
			end:    time.Minute / 2,
			holder: "r1",
		},
		{
			name:  "a dirty stand is refused",
			world: world{dirty: []string{"agent/db-1"}},
			steps: []leaseStep{acquire(time.Second, "r1", CodeDirty)},
			end:   time.Minute / 2,
		},
		{
			name: "the holder gives it back; a stranger cannot",
			steps: []leaseStep{
				acquire(time.Second, "r1", ""),
				{at: 2 * time.Second, name: "release-lease", payload: ReleaseLeaseCmd{Run: "r2"}, code: CodeNotHolder},
				{at: 3 * time.Second, name: "release-lease", payload: ReleaseLeaseCmd{Run: "r1"}},
				acquire(4*time.Second, "r2", ""),
			},
			end:    time.Minute / 2,
			holder: "r2",
		},
		{
			name: "holdings stay while leased",
			steps: []leaseStep{
				{at: time.Second, name: "accept", payload: AcceptCmd{Ref: ref.OwnerRef("agent/db-1")}},
				acquire(2*time.Second, "r1", ""),
				{at: 3 * time.Second, name: "release", payload: ReleaseCmd{Ref: ref.OwnerRef("agent/db-1")}, code: CodeLeased},
			},
			end:    time.Minute / 2,
			holder: "r1",
		},
		{
			name:  "the lease ends with its run",
			world: world{runState: func(string, time.Duration) string { return RunClosed }},
			steps: []leaseStep{acquire(time.Second, "r1", "")},
			end:   3 * time.Minute,
		},
		{
			name:   "a run not started yet keeps its lease within the grace",
			world:  world{runState: func(string, time.Duration) string { return RunUnknown }},
			steps:  []leaseStep{acquire(time.Second, "r1", "")},
			end:    LeaseGrace / 2,
			holder: "r1",
		},
		{
			name:  "a run that never starts loses the lease after the grace",
			world: world{runState: func(string, time.Duration) string { return RunUnknown }},
			steps: []leaseStep{acquire(time.Second, "r1", "")},
			end:   LeaseGrace + 5*time.Minute,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			st := runLease(t, Spec{}, c.world, c.steps, c.end)
			holder := ""
			if st.Lease != nil {
				holder = st.Lease.Run
			}
			if holder != c.holder {
				t.Fatalf("holder %q, want %q", holder, c.holder)
			}
		})
	}
}

// A leased stand neither idles out nor drops TTL'd holdings under the run.
func TestLeasedStandDoesNotExpire(t *testing.T) {
	t.Parallel()
	st := runLease(t, Spec{IdleTimeout: "10m"}, world{}, []leaseStep{
		{at: time.Second, name: "accept", payload: AcceptCmd{Ref: ref.OwnerRef("agent/db-1"), Keep: 5 * time.Minute}},
		acquire(2*time.Second, "r1", ""),
	}, time.Hour)
	if _, held := st.Holdings["agent/db-1"]; !held || st.Expiring {
		t.Fatalf("a leased stand expired: holdings %v, expiring %v", st.Holdings, st.Expiring)
	}
}
