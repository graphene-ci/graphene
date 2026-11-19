//go:build record

package standflow

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/pipeline/pkg/ref"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Records a stand history with the CURRENT code into testdata, for the
// golden replay that guards the next change. Run it on the released tag
// before changing the flow:
//
//	GRAPHENE_STAND_RECORD_AS=stand-vX.Y.Z.json go test -tags record -run TestRecordStandHistory ./internal/standflow/
func TestRecordStandHistory(t *testing.T) {
	name := os.Getenv("GRAPHENE_STAND_RECORD_AS")
	if name == "" {
		t.Skip("set GRAPHENE_STAND_RECORD_AS to the testdata file name")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  os.Getenv("TEMPORAL_CLI"),
		ClientOptions: &client.Options{},
		LogLevel:      "warn",
		SearchAttributes: temporal.NewSearchAttributes(
			entdefine.SearchAttrKind.ValueSet("seed"),
			entdefine.SearchAttrPhase.ValueSet("seed"),
			entdefine.SearchAttrLabels.ValueSet([]string{"seed=seed"}),
			wire.SearchAttrOwner.ValueSet("seed"),
			wire.SearchAttrKeepUntil.ValueSet(time.Now()),
			wire.SearchAttrFlows.ValueSet([]string{"seed"}),
		),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()
	cl := srv.Client()
	def := New(time.Second)
	w := worker.New(cl, wire.ServerQueue, worker.Options{})
	if err := def.Register(w); err != nil {
		t.Fatal(err)
	}
	w.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, activity.RegisterOptions{Name: CascadeActivity})
	w.RegisterActivityWithOptions(func(context.Context, string) (bool, error) { return false, nil }, activity.RegisterOptions{Name: RunActiveActivity})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	stands := entclient.Bind(def, cl, wire.ServerQueue)
	rid := entity.ResourceID("recorded")
	steps := []any{
		AcceptCmd{Ref: ref.OwnerRef("artifact/kept"), From: "run/one"},
		AcceptCmd{Ref: ref.OwnerRef("artifact/short"), Keep: 2 * time.Second, From: "run/one"},
		ExtendCmd{Ref: ref.OwnerRef("artifact/kept"), Keep: time.Hour},
		AcceptCmd{Ref: ref.OwnerRef("artifact/released"), From: "run/two"},
		ReleaseCmd{Ref: ref.OwnerRef("artifact/released")},
	}
	for i, step := range steps {
		var err error
		switch cmd := step.(type) {
		case AcceptCmd:
			if i == 0 {
				_, err = entclient.ExecWithStart(ctx, stands, rid, Spec{PipelineId: "recorded"}, cmd)
			} else {
				_, err = entclient.Exec(ctx, stands, rid, cmd)
			}
		case ExtendCmd:
			_, err = entclient.Exec(ctx, stands, rid, cmd)
		case ReleaseCmd:
			_, err = entclient.Exec(ctx, stands, rid, cmd)
		}
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	// Ticks past the short keep: the expiry cascades it.
	time.Sleep(5 * time.Second)
	if err := stands.Delete(ctx, rid); err != nil {
		t.Fatal(err)
	}
	wid := stands.WorkflowID(rid)
	if err := cl.GetWorkflow(ctx, wid, "").Get(ctx, nil); err != nil {
		t.Logf("stand ended: %v", err)
	}
	history := &historypb.History{}
	iter := cl.GetWorkflowHistory(ctx, wid, "", false, 0)
	for iter.HasNext() {
		ev, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		history.Events = append(history.Events, ev)
	}
	b, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("testdata", 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("testdata/"+name, b, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("recorded %d events", len(history.Events))
}
