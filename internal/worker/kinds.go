package worker

// The kind registry: what this installation knows how to declare, and
// what each kind can be asked to do. Without it a generic Apply has
// nowhere to look, and a UI has to hard-code the vocabulary it should
// be discovering.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/gopherex/schemapb/go/schemapb"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/kindflow"
	syslabels "github.com/graphene-ci/graphene/internal/labels"
	"github.com/graphene-ci/graphene/internal/nsflow"
	"github.com/graphene-ci/graphene/internal/pipelineflow"
	"github.com/graphene-ci/graphene/internal/rbacflow"
	"github.com/graphene-ci/graphene/internal/revisionflow"
	"github.com/graphene-ci/graphene/internal/sourceflow"
	"github.com/graphene-ci/graphene/internal/standflow"
	"github.com/graphene-ci/graphene/internal/triggerflow"
	"github.com/graphene-ci/graphene/internal/valueflow"
	agentflow "github.com/graphene-ci/pipeline/pkg/flow/agent"
	"github.com/graphene-ci/pipeline/pkg/flow/artifact"
	"github.com/graphene-ci/pipeline/pkg/manifest"
	"github.com/graphene-ci/pipeline/pkg/pipeline"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// Dimension is one of the five ways an entity is observed.
type Dimension string

// The five dimensions of any entity.
const (
	DimensionState   Dimension = "state"
	DimensionEvents  Dimension = "events"
	DimensionLogs    Dimension = "logs"
	DimensionMetrics Dimension = "metrics"
	DimensionTraces  Dimension = "traces"
)

// AllDimensions is what a system record answers — all five: the server
// emits logs, metrics and spans under every record's own reference.
var AllDimensions = []Dimension{
	DimensionState, DimensionEvents, DimensionLogs, DimensionMetrics, DimensionTraces,
}

// RecordDimensions is what a record answers with no telemetry of its
// own: the two it IS, by being a workflow with a history.
var RecordDimensions = []Dimension{DimensionState, DimensionEvents}

// KindInfo describes one kind to whoever asks: what its spec looks
// like and which commands it answers. The schemas come from the Go
// types themselves, so they cannot drift from the code.
type KindInfo struct {
	Name string `json:"name"`
	// Declarable says whether a caller may create this kind directly.
	// A revision is declared, a run is fired, an artifact is produced
	// by a pipeline — not everything is a thing you make on request.
	Declarable bool `json:"declarable"`
	// Spec is the schema of the declaration.
	Spec *schemapb.Schema `json:"-"`
	// Commands are what the kind can be asked to do, with the schema
	// of each payload.
	Commands []CommandInfo `json:"commands"`
	// Description is one line for a human.
	Description string `json:"description"`
	// Dimensions this kind answers. Every RECORD answers state and
	// events by construction — it is a workflow with a describe query
	// and a history. Logs, metrics and traces are answered when
	// something emits under the entity's reference: for system records
	// the server's own interceptor does, which is why they are listed
	// here rather than left to a console's guess.
	Dimensions []Dimension `json:"dimensions"`
}

// CommandInfo is one command of a kind.
type CommandInfo struct {
	Name    string           `json:"name"`
	Payload *schemapb.Schema `json:"-"`
}

// kindEntry is the registry's own record of a kind.
type kindEntry struct {
	info KindInfo
	// specType turns a JSON spec into the kind's own Go type — the
	// validation a generic Apply would otherwise skip.
	specType reflect.Type
}

// Kinds lists what this installation can declare and command.
func (s *Worker) Kinds() []KindInfo {
	out := make([]KindInfo, 0, len(s.kinds))
	for _, e := range s.kinds {
		out = append(out, e.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Apply declares a record of any registered kind. The spec is checked
// against the kind's Go type here — a typo fails at the door instead
// of inside a workflow that then sits in create-failed.
func (s *Worker) Apply(ctx context.Context, kind, id string, spec json.RawMessage, labels map[string]string) (string, error) {
	entry, ok := s.kinds[kind]
	if !ok {
		return "", fmt.Errorf("unknown kind %q; this installation knows %s", kind, s.kindNames())
	}
	if !entry.info.Declarable {
		return "", fmt.Errorf("a %s is not declared directly: %s", kind, entry.info.Description)
	}
	// The installation's own records live in ONE place: declaring them
	// anywhere else would leave copies nobody reads.
	if authz.IsSystem(authz.KindOf(kind+"/")) && s.deps.Namespace != nsflow.SystemNamespace {
		return "", fmt.Errorf("a %s belongs to the %s namespace", kind, nsflow.SystemNamespace)
	}
	if len(spec) > 0 && entry.specType != nil {
		probe := reflect.New(entry.specType).Interface()
		dec := json.NewDecoder(bytes.NewReader(spec))
		dec.DisallowUnknownFields()
		if err := dec.Decode(probe); err != nil {
			return "", fmt.Errorf("spec does not fit kind %q: %w", kind, err)
		}
	}
	// System markers go on at the ONE place records are declared, so
	// completeness does not depend on who declares them.
	labels = syslabels.Merge(labels, s.marksFor(kind, spec))
	return entclient.ApplyRaw(ctx, s.deps.Client, entity.KindName(kind), entity.ResourceID(id), wire.ServerQueue, spec, labels)
}

// marksFor derives a record's system markers from what it declares.
// The ownership tree already answers ancestry; these carry the CROSS
// references a tree has no edge for — which pipeline a source serves,
// where a copy came from, which source a revision was built from.
func (s *Worker) marksFor(kind string, spec json.RawMessage) map[string]string {
	var decl struct {
		PipelineId   string `json:"pipelineId"`
		From         string `json:"from"`
		SourceRef    string `json:"sourceRef"`
		SourceDigest string `json:"sourceDigest"`
		Commit       string `json:"commit"`
	}
	if len(spec) > 0 {
		_ = json.Unmarshal(spec, &decl)
	}
	marks := map[string]string{
		syslabels.Pipeline: decl.PipelineId,
		syslabels.Origin:   decl.From,
		syslabels.Source:   decl.SourceRef,
		syslabels.Commit:   decl.Commit,
	}
	if decl.SourceDigest != "" {
		marks[syslabels.SourceDigest] = strings.TrimPrefix(decl.SourceDigest, "sha256:")[:min(16, len(strings.TrimPrefix(decl.SourceDigest, "sha256:")))]
	}
	return marks
}

func (s *Worker) kindNames() string {
	names := make([]string, 0, len(s.kinds))
	for name := range s.kinds {
		names = append(names, name)
	}
	sort.Strings(names)
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

// buildKinds assembles the registry. Every entry names its spec type
// and its commands; the schemas are reflected from those types, the
// same way a pipeline's params schema is.
func buildKinds() map[string]*kindEntry {
	reg := map[string]*kindEntry{}
	add := func(name, description string, declarable bool, specType reflect.Type, commands ...commandDef) {
		e := &kindEntry{
			info: KindInfo{
				Name: name, Declarable: declarable, Description: description,
				// Every system record is observable end to end: the
				// server emits its logs, metrics and spans under the
				// record's own reference.
				Dimensions: AllDimensions,
			},
			specType: specType,
		}
		if specType != nil {
			if schema, err := manifest.SchemaOf(specType, schemapb.ID("graphene", schemapb.SchemaName(name+"-spec"), schemapb.Ver(0, 1, 0))); err == nil {
				e.info.Spec = schema
			}
		}
		for _, c := range commands {
			ci := CommandInfo{Name: c.name}
			if c.payload != nil {
				if schema, err := manifest.SchemaOf(c.payload, schemapb.ID("graphene", schemapb.SchemaName(name+"-"+c.name), schemapb.Ver(0, 1, 0))); err == nil {
					ci.Payload = schema
				}
			}
			e.info.Commands = append(e.info.Commands, ci)
		}
		reg[name] = e
	}

	// The pipeline IS the project: its source, its working tree, the
	// version its automatic starts use, and the arbiter of those
	// starts — one record, one history.
	add("pipeline", "a project: the arbiter of its runs and the version they use", true,
		reflect.TypeFor[pipelineflow.Spec](),
		cmd("fire", reflect.TypeFor[pipelineflow.FireCmd]()),
		cmd("publish-manifest", reflect.TypeFor[pipelineflow.PublishCmd]()),
		cmd("activate", reflect.TypeFor[pipelineflow.ActivateCmd]()))

	// A namespace record lives in the DEFAULT namespace: a container
	// cannot hold its own declaration.
	add("namespace", "an isolation unit: its own records, queues and visibility", true,
		reflect.TypeFor[nsflow.Spec]())

	add("var", "visible configuration a pipeline's params may reference", true,
		reflect.TypeFor[valueflow.VarSpec](),
		cmd("set", reflect.TypeFor[valueflow.SetVarCmd]()))

	// A secret's VALUE never rides a command — `secret set` is its one
	// channel — so the record declares only that the name exists.
	add("secret", "a name whose value lives sealed beside it", true,
		reflect.TypeFor[valueflow.SecretSpec]())

	// The two source kinds differ in what may be DONE to them, which is
	// why they are two kinds and not one with a mode field.
	add("gitsource", "a checkout of a ref: read-only, moves by fetching again", true,
		reflect.TypeFor[sourceflow.GitSpec](),
		cmd("sync", nil))

	add("managedsource", "the project's own tree: every file editable in place", true,
		reflect.TypeFor[sourceflow.ManagedSpec](),
		cmd("write", reflect.TypeFor[sourceflow.WriteCmd]()),
		cmd("revert", reflect.TypeFor[sourceflow.RevertCmd]()))

	add("revision", "one immutable build of a source tree", true,
		reflect.TypeFor[revisionflow.Spec]())

	add("trigger", "what starts runs besides a human", true,
		reflect.TypeFor[triggerflow.Spec](),
		cmd("pause", nil), cmd("resume", nil),
		cmd("hook", reflect.TypeFor[triggerflow.HookCmd]()))

	add("stand", "the project's standing ground: what outlives a run", true,
		reflect.TypeFor[standflow.Spec](),
		cmd("accept", reflect.TypeFor[standflow.AcceptCmd]()),
		cmd("extend", reflect.TypeFor[standflow.ExtendCmd]()),
		cmd("release", reflect.TypeFor[standflow.ReleaseCmd]()))

	add("agent", "a machine's identity and the process on it", true,
		reflect.TypeFor[pipeline.AgentSpec]())

	add("artifact", "bytes a run produced, kept with their record", true,
		reflect.TypeFor[pipeline.ArtifactSpec]())

	add("role", "a set of rules: what may be done", true,
		reflect.TypeFor[rbacflow.RoleSpec](),
		cmd("set-rules", reflect.TypeFor[rbacflow.SetRulesCmd]()))

	add("rolebinding", "who gets a role, and where", true,
		reflect.TypeFor[rbacflow.BindingSpec](),
		cmd("set-subjects", reflect.TypeFor[rbacflow.SetSubjectsCmd]()))

	add("serviceaccount", "a machine of this installation and its tokens", true,
		reflect.TypeFor[rbacflow.AccountSpec](),
		cmd("issue-token", reflect.TypeFor[rbacflow.IssueTokenCmd]()),
		cmd("revoke-token", reflect.TypeFor[rbacflow.RevokeTokenCmd]()))

	add("run", "the execution of a pipeline — fired, not declared", false, nil)

	// The dictionary contains itself: kind records are managed by the
	// server and the manifests, never declared by hand.
	add("kind", "a dictionary entry: what a kind is and what it answers", false,
		reflect.TypeFor[kindflow.Spec](),
		cmd("declare", reflect.TypeFor[kindflow.DeclareCmd]()),
		cmd("bring", reflect.TypeFor[kindflow.BringCmd]()))

	// Every kind serves the built-in label patch.
	for _, e := range reg {
		e.info.Commands = append(e.info.Commands, CommandInfo{Name: entity.SetLabelsCommandName})
	}
	return reg
}

type commandDef struct {
	name    string
	payload reflect.Type
}

func cmd(name string, payload reflect.Type) commandDef {
	return commandDef{name: name, payload: payload}
}

var _ = agentflow.Kind
var _ = artifact.Kind
