// Package nsbundle runs one runtime bundle per NAMESPACE: a Temporal
// client bound to the namespace, the server worker with the system
// flows, the managed-run reaper, and the stand sweeper. A graphene
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
	"github.com/graphene-ci/graphene/internal/ops"
	"github.com/graphene-ci/graphene/internal/secrets"
	"github.com/graphene-ci/graphene/internal/sweeper"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Deps is what every bundle is built from.
type Deps struct {
	TemporalHostPort string
	TemporalLogger   log.Logger
	Registry         *agents.Registry
	Secrets          *secrets.Namespaced
	Blobs            blob.Store
	ExternalGRPC     string
	// RunTokenFor returns the run token of a namespace.
	RunTokenFor func(namespace string) string
	// UserDataFor renders the agent install script.
	UserDataFor func(namespace string, agentId id.AgentId) (string, error)
	SweepEvery  time.Duration
	ReapEvery   time.Duration
	Log         *xlog.Logger
}

// Bundle is one namespace's runtime.
type Bundle struct {
	Namespace string
	Client    client.Client
	Worker    *worker.Worker
	Runner    *managed.Runner
}

// Manager builds and runs bundles.
type Manager struct {
	deps Deps
	ctx  context.Context // the server's lifetime: bundle goroutines live under it

	mu      sync.Mutex
	bundles map[string]*Bundle
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
	b, err := m.build(namespace)
	if err != nil {
		return nil, err
	}
	m.bundles[namespace] = b
	return b, nil
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
	c, err := client.Dial(client.Options{
		HostPort:  m.deps.TemporalHostPort,
		Namespace: namespace,
		Logger:    m.deps.TemporalLogger,
	})
	if err != nil {
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
		ExternalGRPC: m.deps.ExternalGRPC,
		RunToken:     m.deps.RunTokenFor(namespace),
		Log:          log.With(xlog.String("component", "worker")),
	})
	if err != nil {
		c.Close()
		return nil, err
	}
	runner := managed.New(namespace, c, m.deps.ExternalGRPC, m.deps.RunTokenFor(namespace),
		log.With(xlog.String("component", "managed")))
	sweep := sweeper.New(c, w, log.With(xlog.String("component", "sweeper")))

	b := &Bundle{Namespace: namespace, Client: c, Worker: w, Runner: runner}
	go func() {
		if err := w.Run(m.ctx); err != nil && m.ctx.Err() == nil {
			log.Error("namespace worker died", xlog.Err(err))
		}
	}()
	go sweep.Tick(m.ctx, m.deps.SweepEvery)
	go runner.Tick(m.ctx, m.deps.ReapEvery)
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
