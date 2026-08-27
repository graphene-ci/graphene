package authz

// Resolving a caller's rights means reading the namespace's roles and
// bindings. Those are records, so the read is a visibility query plus
// a describe — cheap, but not free, and it happens on EVERY call.
// Hence a short cache: permissions may lag a few seconds, which is the
// same promise every RBAC system makes.

import (
	"context"
	"sync"
	"time"
)

// Store reads the authorization records of one installation.
type Store interface {
	// Roles returns the namespace's roles by name, including the
	// built-ins.
	Roles(ctx context.Context, namespace string) (map[string]Rules, error)
	// Bindings returns the namespace's bindings (including those bound
	// to every namespace).
	Bindings(ctx context.Context, namespace string) ([]Binding, error)
}

// cacheTTL is how long a resolved permission set is reused. Short
// enough that revoking rights takes effect while someone is still
// looking at the screen.
const cacheTTL = 5 * time.Second

// Resolver answers authorization questions over a Store.
type Resolver struct {
	store Store

	mu     sync.Mutex
	cached map[string]snapshot
}

type snapshot struct {
	roles    map[string]Rules
	bindings []Binding
	at       time.Time
}

// NewResolver builds a resolver over the store.
func NewResolver(store Store) *Resolver {
	return &Resolver{store: store, cached: map[string]snapshot{}}
}

// Allow answers whether the caller may do verb on kind. A caller
// carrying a BOUND role (a minted token of a run or an agent) is
// decided by that role alone: its rights travel with it and no binding
// can widen them.
func (r *Resolver) Allow(ctx context.Context, id Identity, boundRole string, verb Verb, kind Kind) Decision {
	if boundRole != "" {
		roles, err := r.roles(ctx, id.Namespace)
		if err != nil {
			return Decision{Reason: "cannot read roles: " + err.Error()}
		}
		rules, ok := roles[boundRole]
		if !ok {
			return Decision{Reason: "token names role " + boundRole + ", which does not exist"}
		}
		if rules.Allows(verb, kind) {
			return Decision{Allowed: true}
		}
		return Decision{Reason: id.Subject.String() + " may not " + string(verb) + " " + string(kind)}
	}
	snap, err := r.snapshot(ctx, id.Namespace)
	if err != nil {
		return Decision{Reason: "cannot read permissions: " + err.Error()}
	}
	return Authorize(id, verb, kind, snap.bindings, snap.roles)
}

func (r *Resolver) roles(ctx context.Context, namespace string) (map[string]Rules, error) {
	snap, err := r.snapshot(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return snap.roles, nil
}

func (r *Resolver) snapshot(ctx context.Context, namespace string) (snapshot, error) {
	r.mu.Lock()
	snap, ok := r.cached[namespace]
	r.mu.Unlock()
	if ok && time.Since(snap.at) < cacheTTL {
		return snap, nil
	}
	roles, err := r.store.Roles(ctx, namespace)
	if err != nil {
		return snapshot{}, err
	}
	bindings, err := r.store.Bindings(ctx, namespace)
	if err != nil {
		return snapshot{}, err
	}
	snap = snapshot{roles: roles, bindings: bindings, at: time.Now()}
	r.mu.Lock()
	r.cached[namespace] = snap
	r.mu.Unlock()
	return snap, nil
}

// ClusterWide reports whether the caller's rights span every
// namespace: a binding on "*" names them. This is the one signal a
// client needs to offer namespace switching; the switch itself is just
// the header, and every call still authorizes in the target namespace.
func (r *Resolver) ClusterWide(ctx context.Context, id Identity) bool {
	snap, err := r.snapshot(ctx, id.Namespace)
	if err != nil {
		return false
	}
	for _, b := range snap.bindings {
		if b.Namespace == "*" && b.Matches(id) {
			return true
		}
	}
	return false
}

// Forget drops a namespace's cached permissions — called when a role
// or a binding changes, so a revocation does not wait out the TTL.
func (r *Resolver) Forget(namespace string) {
	r.mu.Lock()
	delete(r.cached, namespace)
	r.mu.Unlock()
}
