package worker

// The value planes as records: a variable IS its record, a secret is a
// record beside a sealed value. The door reads variables through the
// same ownership tree it reads everything else, so a variable can be
// owned, labelled, audited and deleted like any other record.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"

	"github.com/graphene-ci/graphene/internal/nsflow"
	"github.com/graphene-ci/graphene/internal/valueflow"
	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// DeclareVar writes one variable: a new name is created, an existing
// one takes the value through its own command so the change lands in
// its history.
func (s *Worker) DeclareVar(ctx context.Context, name, value string) error {
	vars := entclient.Bind(s.varDef, s.deps.Client, wire.ServerQueue)
	if _, err := vars.CreateOrAttach(ctx, entity.ResourceID(name), valueflow.VarSpec{Value: value}); err != nil {
		return err
	}
	// An existing variable takes the value through its own command, so
	// the change lands in its history.
	if _, err := entclient.Exec(ctx, vars, entity.ResourceID(name), valueflow.SetVarCmd{Value: value}); err != nil {
		return err
	}
	s.varCache.forget(name)
	return nil
}

// Var reads one variable's value.
func (s *Worker) Var(ctx context.Context, name string) (string, error) {
	if v, ok := s.varCache.get(name); ok {
		return v, nil
	}
	vars := entclient.Bind(s.varDef, s.deps.Client, wire.ServerQueue)
	desc, err := vars.Describe(ctx, entity.ResourceID(name))
	if err != nil {
		return "", fmt.Errorf("variable %q is not configured", name)
	}
	s.varCache.put(name, desc.State.Value)
	return desc.State.Value, nil
}

// Vars returns every variable of this namespace, name to value.
func (s *Worker) Vars(ctx context.Context) (map[string]string, error) {
	names, err := s.listKind(ctx, string(valueflow.VarKind))
	if err != nil {
		return nil, err
	}
	vars := entclient.Bind(s.varDef, s.deps.Client, wire.ServerQueue)
	out := make(map[string]string, len(names))
	for _, n := range names {
		desc, err := vars.Describe(ctx, entity.ResourceID(n))
		if err != nil {
			continue
		}
		out[n] = desc.State.Value
	}
	return out, nil
}

// SetSecretValue writes the VALUE into the sealed store and records
// the rotation on the secret's record. The order matters: the record
// must never claim a version the store cannot serve.
func (s *Worker) SetSecretValue(ctx context.Context, name, value string) error {
	if s.secretWriter == nil {
		return fmt.Errorf("this installation has no secret store")
	}
	s.secretWriter.Set(s.deps.Namespace, name, value)
	secrets := entclient.Bind(s.secretDef, s.deps.Client, wire.ServerQueue)
	if _, err := secrets.CreateOrAttach(ctx, entity.ResourceID(name), valueflow.SecretSpec{}); err != nil {
		return err
	}
	_, err := entclient.Exec(ctx, secrets, entity.ResourceID(name), valueflow.RotateCmd{})
	return err
}

// forgetSecret erases the value behind a deleted secret record.
func (s *Worker) forgetSecret(_ context.Context, req valueflow.ForgetReq) error {
	if s.secretWriter == nil {
		return nil
	}
	s.secretWriter.Delete(s.deps.Namespace, req.Name)
	return nil
}

// varStore reads variables the way the run planes expect a value
// store to read: by name, within this namespace.
type varStore struct {
	w *Worker
}

// VarStore hands the door a value store backed by the var records.
func (s *Worker) VarStore() *varStore { return &varStore{w: s} }

// Get resolves one variable name.
func (v *varStore) Get(name id.SecretId) (string, error) {
	return v.w.Var(context.Background(), string(name))
}

// valueCache keeps a variable's value for a moment: a submit resolves
// several names at once, and a record read is a network call.
type valueCache struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]cachedValue
	now func() time.Time
}

type cachedValue struct {
	value string
	until time.Time
}

func newValueCache(ttl time.Duration) *valueCache {
	return &valueCache{ttl: ttl, m: map[string]cachedValue{}, now: time.Now}
}

func (c *valueCache) get(name string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[name]
	if !ok || c.now().After(e.until) {
		return "", false
	}
	return e.value, true
}

func (c *valueCache) put(name, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[name] = cachedValue{value: value, until: c.now().Add(c.ttl)}
}

func (c *valueCache) forget(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, name)
}

// DescribeSecret reads one secret's record — its life, never its
// value.
func (s *Worker) DescribeSecret(ctx context.Context, name string) (entity.Phase, valueflow.SecretSpec, valueflow.SecretState, error) {
	secrets := entclient.Bind(s.secretDef, s.deps.Client, wire.ServerQueue)
	out, err := secrets.Describe(ctx, entity.ResourceID(name))
	if err != nil {
		return "", valueflow.SecretSpec{}, valueflow.SecretState{}, err
	}
	return out.Phase, out.Spec, out.State, nil
}

// --- the namespace record's side effects ---

// ensureNamespace registers a namespace and starts serving it.
func (s *Worker) ensureNamespace(ctx context.Context, req nsflow.EnsureReq) error {
	if s.ensureNs == nil {
		return fmt.Errorf("namespaces are declared in the default namespace only")
	}
	return s.ensureNs(ctx, req.Name, req.RetentionDays)
}

// retireNamespace stops serving a namespace whose record was deleted.
// What it holds is left to age out under its own retention — deleting
// a container must not silently destroy what a person put inside it.
func (s *Worker) retireNamespace(ctx context.Context, req nsflow.RetireReq) error {
	if s.retireNs == nil {
		return nil
	}
	return s.retireNs(ctx, req.Name)
}

// DeclareNamespace writes one namespace record — how the installation
// adopts a namespace that already exists (the default one) and how a
// new one is made.
func (s *Worker) DeclareNamespace(ctx context.Context, name string, spec nsflow.Spec) error {
	namespaces := entclient.Bind(s.namespaceDef, s.deps.Client, wire.ServerQueue)
	_, err := namespaces.CreateOrAttach(ctx, entity.ResourceID(name), spec)
	return err
}

// NamespaceDeclared answers whether a namespace still has its record —
// what the bundle manager asks before serving one.
func (s *Worker) NamespaceDeclared(ctx context.Context, name string) (bool, error) {
	namespaces := entclient.Bind(s.namespaceDef, s.deps.Client, wire.ServerQueue)
	desc, err := namespaces.Describe(ctx, entity.ResourceID(name))
	if err != nil {
		// A record that never existed and one whose worker is briefly
		// unreachable look alike here; refusing is the safe half, and
		// the caller retries.
		return false, nil
	}
	return desc.Phase != entity.PhaseDeleted && desc.Phase != entity.PhaseDeleting, nil
}
