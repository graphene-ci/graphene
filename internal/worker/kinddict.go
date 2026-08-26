package worker

// The dictionary as RECORDS: every kind the installation serves has a
// kind/<name> record. The system's own kinds are declared here when
// the namespace's worker starts; the kinds a pipeline brings are
// reconciled in when a manifest lands, next to its triggers. The
// entries police themselves — kindflow's audit tick calls back into
// the activities below.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/graphene/internal/kindflow"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// DeclareSystemKinds writes the dictionary entries of everything this
// server serves. Re-declaring is how an upgrade refreshes them: the
// change lands through the record's own command, like a role's rules.
func (s *Worker) DeclareSystemKinds(ctx context.Context) error {
	entries := entclient.Bind(s.kindDef, s.deps.Client, wire.ServerQueue)
	for _, info := range s.KindInfos() {
		decl := kindflow.DeclareCmd{
			Declarable:  info.Declarable,
			Description: info.Description,
		}
		for _, d := range info.Dimensions {
			decl.Dimensions = append(decl.Dimensions, string(d))
		}
		if info.Spec != nil {
			if raw, err := protojson.Marshal(info.Spec); err == nil {
				decl.SpecSchema = raw
			}
		}
		for _, c := range info.Commands {
			kc := kindflow.Command{Name: string(c.Name)}
			if c.Payload != nil {
				if raw, err := protojson.Marshal(c.Payload); err == nil {
					kc.PayloadSchema = raw
				}
			}
			decl.Commands = append(decl.Commands, kc)
		}
		rid := entity.ResourceID(info.Name)
		if _, err := entries.CreateOrAttach(ctx, rid, kindflow.Spec{Origin: kindflow.OriginSystem}); err != nil {
			return fmt.Errorf("kind %s: %w", info.Name, err)
		}
		if _, err := entclient.Exec(ctx, entries, rid, decl); err != nil {
			return fmt.Errorf("kind %s: %w", info.Name, err)
		}
	}
	return nil
}

// KindInfos lists what the server itself serves — the bootstrap's
// source, from the same Go types the door validates with.
func (s *Worker) KindInfos() []KindInfo {
	out := make([]KindInfo, 0, len(s.kinds))
	for _, e := range s.kinds {
		out = append(out, e.info)
	}
	return out
}

// reconcileKindRecords brings a manifest's kinds into the dictionary —
// called next to the trigger reconcile whenever a manifest lands. Only
// the fast half lives here; the slow half (pruning, self-removal) is
// the entries' own audit tick.
func (s *Worker) reconcileKindRecords(ctx context.Context, pipelineId string, names []string) error {
	entries := entclient.Bind(s.kindDef, s.deps.Client, wire.ServerQueue)
	for _, name := range names {
		// A name the server already serves is not "brought" — the
		// dictionary entry exists and says more than the manifest can.
		if _, system := s.kinds[name]; system {
			continue
		}
		rid := entity.ResourceID(name)
		if _, err := entries.CreateOrAttach(ctx, rid, kindflow.Spec{Origin: kindflow.OriginBrought}); err != nil {
			return fmt.Errorf("kind %s: %w", name, err)
		}
		if _, err := entclient.Exec(ctx, entries, rid, kindflow.BringCmd{
			PipelineId: pipelineId,
			// The chassis command belongs to every record; the kind's
			// OWN commands arrive when the SDK exports them.
			Commands: []kindflow.Command{{Name: string(entity.SetLabelsCommandName)}},
		}); err != nil {
			return fmt.Errorf("kind %s: %w", name, err)
		}
	}
	return nil
}

// auditKind is the activity behind an entry's tick: count the kind's
// live records, and keep only the bringers whose active manifest still
// names it.
func (s *Worker) auditKind(ctx context.Context, req kindflow.AuditReq) (kindflow.AuditRes, error) {
	var res kindflow.AuditRes
	count, err := s.deps.Client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Query: fmt.Sprintf("%s = %q AND ExecutionStatus = 'Running'",
			entdefine.SearchAttrKind.GetName(), req.KindName),
	})
	if err != nil {
		return res, err
	}
	res.Records = int(count.GetCount())
	for _, pipelineId := range req.BroughtBy {
		st, err := s.GetPipeline(ctx, pipelineId)
		if err != nil {
			continue // the pipeline is gone — pruned
		}
		if manifestNamesKind(st.Manifest, req.KindName) {
			res.BroughtBy = append(res.BroughtBy, pipelineId)
		}
	}
	return res, nil
}

// retireKind deletes an orphaned entry — the record asked for its own
// removal, and a workflow cannot remove itself.
func (s *Worker) retireKind(ctx context.Context, kindName string) error {
	return s.DeleteOne(ctx, string(kindflow.Kind)+"/"+kindName)
}

// manifestNamesKind reports whether a manifest's kinds include name.
func manifestNamesKind(manifestJSON json.RawMessage, name string) bool {
	if len(manifestJSON) == 0 {
		return false
	}
	var m struct {
		Kinds []string `json:"kinds"`
	}
	if json.Unmarshal(manifestJSON, &m) != nil {
		return false
	}
	for _, k := range m.Kinds {
		if k == name {
			return true
		}
	}
	return false
}
