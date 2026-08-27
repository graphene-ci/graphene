// Package nsbundle runs one runtime bundle per NAMESPACE: a Temporal
// client bound to the namespace, the server worker with the system
// flows, and the managed-run reaper. A graphene
// namespace is symmetric to a Temporal namespace — creating one
// registers it in Temporal with the graphene search attributes; a
// bundle starts lazily on first use.
package nsbundle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gopherex/xlog"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/graphene-ci/graphene/internal/agents"
	"github.com/graphene-ci/graphene/internal/infrastructure/blob"
	"github.com/graphene-ci/graphene/internal/managed"
	"github.com/graphene-ci/graphene/internal/materialize"
	"github.com/graphene-ci/graphene/internal/nsflow"
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Deps is what every bundle is built from.
type Deps struct {
	TemporalHostPort string
	TemporalLogger   log.Logger
	Registry         *agents.Registry
	Secrets          *secrets.Namespaced
	// Materializer builds source revisions; nil disables the
	// source-first contour.
	Materializer *materialize.Materializer
	Blobs        blob.Store
	External     string
	// RunTokenFor returns the run token of a namespace — the fallback
	// for an installation without a signing key.
	RunTokenFor func(namespace string) string
	// MintRunToken issues a token scoped to ONE run, living as long as
	// the run may; empty when the installation cannot mint.
	MintRunToken func(namespace, runId string) string
	// UserDataFor renders the agent install script.
	UserDataFor func(namespace string, agentId id.AgentId) (string, error)
	SweepEvery  time.Duration
	ReapEvery   time.Duration
	// LogSink receives tailed run-container output (the server's OTLP
	// collector); nil disables orchestrator log tailing.
	LogSink managed.LogSink
	// MakeRunStarter builds the run-start path for trigger firings —
	// wired by the server so the worker shares the management door's
	// start logic (validation, labels, the managed contour).
	MakeRunStarter func(*Bundle) worker.RunStarter
	Log            *xlog.Logger
}

// Bundle is one namespace's runtime.
type Bundle struct {
	Namespace string
	// MintRunToken issues the token a starting run carries.
	MintRunToken func(namespace, runId string) string
	Client       client.Client
	Worker       *worker.Worker
	Runner       *managed.Runner
	// Secrets and Vars are the namespace-bound value stores: on run
	// start the door substitutes ${var:...} params and checks that
	// secret-typed params name existing secrets.
	Secrets secrets.Store
	Vars    secrets.Store
	// stop ends this bundle's goroutines when the namespace is retired.
	stop context.CancelFunc
}

// Manager builds and runs bundles.
type Manager struct {
	deps Deps
	ctx  context.Context // the server's lifetime: bundle goroutines live under it

	mu      sync.Mutex
	bundles map[string]*Bundle
	// declared answers whether a namespace still has its record. Nil
	// until the default bundle exists — the installation's own
	// bootstrap predates the records.
	declared func(ctx context.Context, name string) (bool, error)
}

// SetDeclaredCheck wires the question "does this namespace still have
// a record?". A namespace whose record is gone stops being served: the
// record IS the namespace's existence, so an ordinary call must not
// resurrect it.
func (m *Manager) SetDeclaredCheck(fn func(ctx context.Context, name string) (bool, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.declared = fn
}

// New builds the manager; ctx bounds every bundle's goroutines.
func New(ctx context.Context, deps Deps) *Manager {
	return &Manager{deps: deps, ctx: ctx, bundles: map[string]*Bundle{}}
}

// Get returns the namespace's bundle, starting it on first use.
func (m *Manager) Get(namespace string) (*Bundle, error) {
	if namespace == "" || namespace == "*" {
		return nil, fmt.Errorf("a concrete namespace is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.bundles[namespace]; ok {
		return b, nil
	}
	// Starting a bundle is how a namespace comes back after a server
	// restart — so it must answer to the records, or a retired
	// namespace would return on the next call that names it.
	if namespace != nsflow.SystemNamespace && m.declared != nil {
		ok, err := m.declared(m.ctx, namespace)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("namespace %s is not declared", namespace)
		}
	}
	b, err := m.build(namespace)
	if err != nil {
		return nil, err
	}
	m.bundles[namespace] = b
	return b, nil
}

// Retire stops serving one namespace: its worker and runner end, its
// connection closes. What the namespace HOLDS is untouched — it ages
// out under its own retention, because deleting a container must not
// silently destroy what a person put inside it.
func (m *Manager) Retire(namespace string) error {
	if namespace == nsflow.SystemNamespace {
		return fmt.Errorf("the %s namespace cannot be retired", namespace)
	}
	m.mu.Lock()
	b, ok := m.bundles[namespace]
	delete(m.bundles, namespace)
	m.mu.Unlock()
	if !ok {
		return nil
	}
	if b.stop != nil {
		b.stop()
	}
	b.Client.Close()
	m.deps.Log.Info("namespace retired", xlog.String("namespace", namespace))
	return nil
}

// List names the running bundles.
func (m *Manager) List() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.bundles))
	for name := range m.bundles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (m *Manager) build(namespace string) (*Bundle, error) {
	// The bundle's own lifetime, under the server's: retiring one
	// namespace must not touch the others.
	ctx, stop := context.WithCancel(m.ctx)
	c, err := client.Dial(client.Options{
		HostPort:  m.deps.TemporalHostPort,
		Namespace: namespace,
		Logger:    m.deps.TemporalLogger,
	})
	if err != nil {
		stop()
		return nil, fmt.Errorf("namespace %s: %w", namespace, err)
	}
	log := m.deps.Log.With(xlog.String("namespace", namespace))
	agentOps := ops.NewAgentOps(namespace, m.deps.Registry, m.deps.Secrets.In(namespace),
		func(agentId id.AgentId) (string, error) { return m.deps.UserDataFor(namespace, agentId) })
	artifactOps := ops.NewArtifactOps(namespace, m.deps.Blobs)
	w, err := worker.New(worker.Deps{
		Namespace:    namespace,
		Client:       c,
		Registry:     m.deps.Registry,
		AgentOps:     agentOps,
		ArtifactOps:  artifactOps,
		External:     m.deps.External,
		StandTick:    m.deps.SweepEvery,
		RunToken:     m.deps.RunTokenFor(namespace),
		Materializer: m.deps.Materializer,
		Blobs:        m.deps.Blobs,
		Secrets:      m.deps.Secrets.In(namespace),
		SecretWriter: m.deps.Secrets,
		Log:          log.With(xlog.String("component", "worker")),
	})
	if err != nil {
		stop()
		c.Close()
		return nil, err
	}
	runner := managed.New(namespace, c, m.deps.External, m.deps.RunTokenFor(namespace),
		m.deps.LogSink, log.With(xlog.String("component", "managed")))

	// Variables ARE records now: the door reads them the way it reads
	// everything else, through the namespace's own worker.
	b := &Bundle{
		Namespace: namespace, Client: c, Worker: w, Runner: runner,
		MintRunToken: m.deps.MintRunToken,
		Secrets:      m.deps.Secrets.In(namespace),
		Vars:         w.VarStore(),
		stop:         stop,
	}
	if m.deps.MakeRunStarter != nil {
		// Trigger-driven starts go through the same door logic as the
		// management plane; the starter needs the whole bundle.
		w.SetRunStarter(m.deps.MakeRunStarter(b))
	}
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("namespace worker died", xlog.Err(err))
		}
	}()
	// The namespace's dictionary: every kind the server serves gets its
	// record. Async — the worker must be polling before the entries'
	// workflows can run.
	go func() {
		if err := w.DeclareSystemKinds(ctx); err != nil && ctx.Err() == nil {
			log.Error("kind dictionary bootstrap failed", xlog.Err(err))
		}
	}()
	// Stand TTLs are the stands' OWN timers now — no sweeper loop.
	go runner.Tick(ctx, m.deps.ReapEvery)
	// Machine executors live while their records do; this collects them
	// when the last record dies after the run is gone.
	go w.ReapExecutors(ctx, m.deps.ReapEvery)
	// Orphan bytes: a terminated record runs no finalizer; the janitor
	// answers for what deletion could not.
	go w.RunJanitor(ctx)
	log.Info("namespace bundle started")
	return b, nil
}

// CreateNamespace registers the namespace in Temporal with the graphene
// search attributes and starts its bundle. Idempotent.
func (m *Manager) CreateNamespace(ctx context.Context, base client.Client, name string, retentionDays int32) error {
	if name == "" || name == "*" {
		return fmt.Errorf("a concrete namespace name is required")
	}
	if retentionDays <= 0 {
		retentionDays = 30
	}
	retention := durationpb.New(time.Duration(retentionDays) * 24 * time.Hour)
	_, err := base.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        name,
		WorkflowExecutionRetentionPeriod: retention,
	})
	var already *serviceerror.NamespaceAlreadyExists
	if err != nil && !errors.As(err, &already) {
		return fmt.Errorf("register namespace: %w", err)
	}
	// The graphene search attributes are PER NAMESPACE — symmetry again.
	_, err = base.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: name,
		SearchAttributes: map[string]enums.IndexedValueType{
			"EntityKind":      enums.INDEXED_VALUE_TYPE_KEYWORD,
			"EntityPhase":     enums.INDEXED_VALUE_TYPE_KEYWORD,
			"EntityOwner":     enums.INDEXED_VALUE_TYPE_KEYWORD,
			"EntityLabels":    enums.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			"EntityKeepUntil": enums.INDEXED_VALUE_TYPE_DATETIME,
		},
	})
	var alreadySA *serviceerror.AlreadyExists
	if err != nil && !errors.As(err, &alreadySA) {
		return fmt.Errorf("register search attributes: %w", err)
	}
	_, err = m.Get(name)
	return err
}

// ListNamespaces asks Temporal for every registered namespace.
func (m *Manager) ListNamespaces(ctx context.Context, base client.Client) ([]string, error) {
	var out []string
	var token []byte
	for {
		resp, err := base.WorkflowService().ListNamespaces(ctx, &workflowservice.ListNamespacesRequest{
			NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, ns := range resp.GetNamespaces() {
			name := ns.GetNamespaceInfo().GetName()
			if name == "temporal-system" {
				continue
			}
			out = append(out, name)
		}
		token = resp.GetNextPageToken()
		if len(token) == 0 {
			sort.Strings(out)
			return out, nil
		}
	}
}
