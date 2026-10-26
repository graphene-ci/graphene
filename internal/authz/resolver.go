package authz

// Resolving a caller's rights means reading the installation's roles
// and bindings. Those are records, so the read is a visibility query
// plus a describe per record — cheap, but not free, and every call
// asks. Hence one shared snapshot, refreshed at most every few seconds:
// permissions may lag that long, which is the promise every RBAC
// system makes.
//
// The records are installation-wide (a binding names its namespace and
// Authorize filters by it), so ONE snapshot serves every namespace —
// a snapshot per caller namespace multiplied the same reads by the
// number of namespaces. One refresh runs at a time; callers that
// arrive during it keep the snapshot they have. A failed refresh keeps
// the last good snapshot for up to a minute: a backend that sheds load
// for a moment must not turn into "permission denied" for every run and
// agent, and a store down for longer must not keep revoked rights alive.

import (
	"context"
	"sync"
	"time"
)

// Store reads the authorization records of one installation.
type Store interface {
	// Roles returns the roles by name, including the built-ins.
	Roles(ctx context.Context) (map[string]Rules, error)
	// Bindings returns every binding; each names its namespace.
	Bindings(ctx context.Context) ([]Binding, error)
}

// cacheTTL is how long a snapshot is reused. Short enough that revoking
// rights takes effect while someone is still looking at the screen.
const cacheTTL = 5 * time.Second

// maxStale bounds how long the last snapshot stands in for a store that
// cannot be read: long enough to ride out a burst of shed load, short
// enough that a revoked right does not live on while the store is down.
// Past it, the failure is the answer and every caller is refused.
const maxStale = time.Minute

// Resolver answers authorization questions over a Store.
type Resolver struct {
	store Store
	now   func() time.Time

	// refresh serializes reads of the store; mu guards the snapshot.
	refresh sync.Mutex
	mu      sync.Mutex
	snap    snapshot
}

type snapshot struct {
	roles    map[string]Rules
	bindings []Binding
	at       time.Time
}

// NewResolver builds a resolver over the store.
func NewResolver(store Store) *Resolver {
	return &Resolver{store: store, now: time.Now}
}

// Allow answers whether the caller may do verb on kind. A caller
// carrying a BOUND role (a minted token of a run or an agent) is
// decided by that role alone: its rights travel with it and no binding
// can widen them.
func (r *Resolver) Allow(ctx context.Context, id Identity, boundRole string, verb Verb, kind Kind) Decision {
	if boundRole != "" {
		roles, err := r.roles(ctx)
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
	snap, err := r.snapshot(ctx)
	if err != nil {
		return Decision{Reason: "cannot read permissions: " + err.Error()}
	}
	return Authorize(id, verb, kind, snap.bindings, snap.roles)
}

func (r *Resolver) roles(ctx context.Context) (map[string]Rules, error) {
	snap, err := r.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snap.roles, nil
}

// current is the snapshot held now, and whether it is still fresh.
func (r *Resolver) current() (snapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snap, !r.snap.at.IsZero() && r.now().Sub(r.snap.at) < cacheTTL
}

// usable reports whether a snapshot may still answer for a store that
// cannot be read right now.
func (r *Resolver) usable(snap snapshot) bool {
	return !snap.at.IsZero() && r.now().Sub(snap.at) < maxStale
}

// snapshot is a fresh snapshot, or — while the store cannot be read —
// the last good one for at most maxStale.
func (r *Resolver) snapshot(ctx context.Context) (snapshot, error) {
	snap, fresh := r.current()
	if fresh {
		return snap, nil
	}
	// A refresh already running answers for everyone: a caller with a
	// snapshot keeps it; only the very first callers wait.
	if !r.refresh.TryLock() {
		if r.usable(snap) {
			return snap, nil
		}
		r.refresh.Lock()
	}
	defer r.refresh.Unlock()
	// Whoever held the lock before may have refreshed already.
	snap, fresh = r.current()
	if fresh {
		return snap, nil
	}
	next, err := r.read(ctx)
	if err != nil {
		if r.usable(snap) {
			return snap, nil
		}
		return snapshot{}, err
	}
	r.mu.Lock()
	r.snap = next
	r.mu.Unlock()
	return next, nil
}

func (r *Resolver) read(ctx context.Context) (snapshot, error) {
	roles, err := r.store.Roles(ctx)
	if err != nil {
		return snapshot{}, err
	}
	bindings, err := r.store.Bindings(ctx)
	if err != nil {
		return snapshot{}, err
	}
	return snapshot{roles: roles, bindings: bindings, at: r.now()}, nil
}

// ClusterWide reports whether the caller's rights span every
// namespace: a binding on "*" names them. This is the one signal a
// client needs to offer namespace switching; the switch itself is just
// the header, and every call still authorizes in the target namespace.
func (r *Resolver) ClusterWide(ctx context.Context, id Identity) bool {
	snap, err := r.snapshot(ctx)
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
