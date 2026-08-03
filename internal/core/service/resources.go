// Package service implements the resource API semantics on top of the
// store and the registry: instance validation with definition pinning,
// CAS writes, graceful deletion via finalizers, selector filtering and
// watch mapping.
//
// This is the domain layer: it implements the generated gRPC server
// interfaces directly (the proto IS our contract), while transport
// concerns (listeners, uds, tokens) live in infrastructure.
//
// Storage layout: the store value is the marshaled Resource with the
// store-owned fields (revision, created_revision) zeroed; they are stamped
// back from store entries on every read. Definitions live under the
// reserved kind space (see registry).
package service

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/store"
)

// errUncloneable guards the proto.Clone type assertion; hitting it means a
// broken protobuf runtime, not a caller mistake.
var errUncloneable = errors.New("service: resource clone returned an unexpected type")

// Resources implements graphenepbv1.ResourceServiceServer.
type Resources struct {
	graphenepbv1.UnimplementedResourceServiceServer

	st  store.Store
	reg *registry.Registry
}

func NewResources(st store.Store, reg *registry.Registry) *Resources {
	return &Resources{st: st, reg: reg}
}

// --- instances ----------------------------------------------------------

func (r *Resources) Get(ctx context.Context, req *graphenepbv1.GetRequest) (*graphenepbv1.GetResponse, error) {
	storedKey, err := storeKey(req.GetKey())
	if err != nil {
		return nil, err
	}

	entry, err := r.st.Get(ctx, storedKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s not found", key.FromProto(req.GetKey()).String())
	}

	if err != nil {
		return nil, internal(err)
	}

	res, err := decodeResource(entry)
	if err != nil {
		return nil, internal(err)
	}

	if err := auth.CheckRead(ctx, req.GetKey().GetKind(), req.GetKey().GetPath(), res); err != nil {
		return nil, denied(err)
	}

	return &graphenepbv1.GetResponse{Resource: res}, nil
}

func (r *Resources) Put(ctx context.Context, req *graphenepbv1.PutRequest) (*graphenepbv1.PutResponse, error) {
	res := req.GetResource()

	storedKey, err := storeKey(res.GetKey())
	if err != nil {
		return nil, err
	}

	kind := res.GetKey().GetKind()
	if kind == registry.KindKind {
		return nil, status.Error(codes.InvalidArgument, "definitions are managed via Define, not Put")
	}

	// Validate against the kind's definition; version 0 resolves to latest
	// and is pinned into the stored record.
	pinned, err := r.reg.ValidateInstance(ctx, kind, res.GetKey().GetPath(),
		res.GetDefinitionVersion(), res.GetSpec(), res.GetStatus())
	if err != nil {
		return nil, mapRegistryErr(err)
	}

	// The deleting mark is system-owned: it appears only through Delete.
	// A Put may legitimately act on a deleting resource (finalizer removal)
	// but must not set or clear the mark itself — we carry it over from
	// the current record.
	current, err := r.currentRecord(ctx, storedKey)
	if err != nil {
		return nil, err
	}

	// Authorize by changed parts, constraints evaluated against the
	// CURRENT record for updates (a writer cannot move an object out of
	// its own scope) and the incoming one for creates.
	against := current
	if against == nil {
		against = res
	}

	if err := auth.CheckWrite(ctx, kind, res.GetKey().GetPath(), changedParts(current, res), against); err != nil {
		return nil, denied(err)
	}

	// Authorization data is written through the same API as everything
	// else — so the escalation rule guards it here: no writer may mint
	// grants it does not itself hold.
	if err := r.checkAuthority(ctx, kind, res, current); err != nil {
		return nil, denied(err)
	}

	stored, ok := proto.Clone(res).(*graphenepbv1.Resource)
	if !ok {
		return nil, internal(errUncloneable)
	}

	stored.Revision = 0
	stored.CreatedRevision = 0
	stored.DefinitionVersion = pinned
	stored.Deleting = current.GetDeleting()
	stored.Generation = nextGeneration(current, stored)

	// Finalize-commit path: the resource is deleting and the last
	// finalizer was just removed — the Put turns into the real removal.
	if stored.GetDeleting() && len(stored.GetFinalizers()) == 0 {
		rev, err := r.st.Delete(ctx, storedKey, req.GetExpectedRevision())
		if err != nil {
			return nil, mapStoreErr(err, res.GetKey())
		}

		return &graphenepbv1.PutResponse{Revision: rev, StoreRevision: rev}, nil
	}

	raw, err := proto.Marshal(stored)
	if err != nil {
		return nil, internal(err)
	}

	rev, err := r.st.Put(ctx, storedKey, raw, req.GetExpectedRevision())
	if err != nil {
		return nil, mapStoreErr(err, res.GetKey())
	}

	return &graphenepbv1.PutResponse{Revision: rev, StoreRevision: rev}, nil
}

func (r *Resources) Delete(ctx context.Context, req *graphenepbv1.DeleteRequest) (*graphenepbv1.DeleteResponse, error) {
	storedKey, err := storeKey(req.GetKey())
	if err != nil {
		return nil, err
	}

	entry, err := r.st.Get(ctx, storedKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s not found", key.FromProto(req.GetKey()).String())
	}

	if err != nil {
		return nil, internal(err)
	}

	current, err := decodeResource(entry)
	if err != nil {
		return nil, internal(err)
	}

	if err := auth.CheckDelete(ctx, req.GetKey().GetKind(), req.GetKey().GetPath(), current); err != nil {
		return nil, denied(err)
	}

	if err := r.checkAuthorityLoss(ctx, req.GetKey().GetKind(), current); err != nil {
		return nil, denied(err)
	}

	// No finalizers — remove immediately.
	if len(current.GetFinalizers()) == 0 {
		if _, err := r.st.Delete(ctx, storedKey, req.GetExpectedRevision()); err != nil {
			return nil, mapStoreErr(err, req.GetKey())
		}

		return &graphenepbv1.DeleteResponse{}, nil
	}

	// Graceful path: mark deleting (watchers see a PUT); controllers do
	// their teardown and remove finalizers via Put; the finalize-commit
	// happens there.
	if current.GetDeleting() {
		return &graphenepbv1.DeleteResponse{}, nil // already in progress
	}

	current.Deleting = true
	current.Revision = 0
	current.CreatedRevision = 0

	raw, err := proto.Marshal(current)
	if err != nil {
		return nil, internal(err)
	}

	if _, err := r.st.Put(ctx, storedKey, raw, req.GetExpectedRevision()); err != nil {
		return nil, mapStoreErr(err, req.GetKey())
	}

	return &graphenepbv1.DeleteResponse{}, nil
}

func (r *Resources) List(ctx context.Context, req *graphenepbv1.ListRequest) (*graphenepbv1.ListResponse, error) {
	if req.GetKind() == "" {
		return nil, status.Error(codes.InvalidArgument, "kind is required")
	}

	allowed, err := auth.Filter(ctx, auth.VerbList, req.GetKind(), req.GetPathPrefix())
	if err != nil {
		return nil, denied(err)
	}

	prefix := key.New(req.GetKind(), req.GetPathPrefix()...).Encode()

	limit := int(req.GetPageSize())

	var cursor []byte
	if req.GetPageToken() != "" {
		cursor = []byte(req.GetPageToken())
	}

	entries, next, err := r.st.Scan(ctx, prefix, limit, cursor)
	if err != nil {
		return nil, internal(err)
	}

	resp := &graphenepbv1.ListResponse{}

	for _, e := range entries {
		res, err := decodeResource(e)
		if err != nil {
			return nil, internal(err)
		}

		if allowed(res) && matchSelector(res, req.GetSelector()) {
			resp.Resources = append(resp.Resources, res)
		}
	}

	if next != nil {
		resp.NextPageToken = string(next)
	}

	return resp, nil
}

func (r *Resources) Watch(req *graphenepbv1.WatchRequest, srv graphenepbv1.ResourceService_WatchServer) error {
	if req.GetKind() == "" {
		return status.Error(codes.InvalidArgument, "kind is required")
	}

	ctx := srv.Context()

	allowed, err := auth.Filter(ctx, auth.VerbWatch, req.GetKind(), req.GetPathPrefix())
	if err != nil {
		return denied(err)
	}

	prefix := key.New(req.GetKind(), req.GetPathPrefix()...).Encode()

	events, err := r.st.Watch(ctx, prefix, req.GetFromStoreRevision())
	if err != nil {
		return internal(err)
	}

	for event := range events {
		out, err := mapEvent(&event)
		if err != nil {
			return internal(err)
		}

		// The catch-up boundary carries no resource: it is the client's
		// resume cursor and passes every filter untouched.
		if event.Type == store.EventSync {
			if err := srv.Send(out); err != nil {
				return fmt.Errorf("send watch event: %w", err)
			}

			continue
		}

		// The grant predicate is mandatory for every event. For deletes it
		// is evaluated against the record's final state (prev_kv), while
		// the client only ever receives the key: visibility is filtered,
		// content is not leaked.
		if !allowed(eventView(&event, out)) {
			continue
		}

		// Deletes pass the CLIENT selector always: the final state is gone
		// and the watcher must be told regardless of its filter.
		if event.Type == store.EventPut && !matchSelector(out.GetResource(), req.GetSelector()) {
			continue
		}

		if err := srv.Send(out); err != nil {
			return fmt.Errorf("send watch event: %w", err)
		}
	}
	// Channel closed: store shut down or the consumer was too slow —
	// the client re-watches from its last seen store revision.
	return status.Error(codes.Aborted, "watch stream reset; re-watch from the last seen store_revision")
}

// --- definitions --------------------------------------------------------

func (r *Resources) Define(ctx context.Context, req *graphenepbv1.DefineRequest) (*graphenepbv1.DefineResponse, error) {
	if err := auth.CheckDefine(ctx); err != nil {
		return nil, denied(err)
	}

	version, err := r.reg.Define(ctx, req.GetDefinition())
	if err != nil {
		return nil, mapRegistryErr(err)
	}

	return &graphenepbv1.DefineResponse{Version: version}, nil
}

func (r *Resources) GetDefinition(
	ctx context.Context,
	req *graphenepbv1.GetDefinitionRequest,
) (*graphenepbv1.GetDefinitionResponse, error) {
	if _, err := auth.Filter(ctx, auth.VerbGet, registry.KindKind, nil); err != nil {
		return nil, denied(err)
	}

	def, err := r.reg.Get(ctx, req.GetKind(), req.GetVersion())
	if err != nil {
		return nil, mapRegistryErr(err)
	}

	return &graphenepbv1.GetDefinitionResponse{Definition: def}, nil
}

func (r *Resources) ListDefinitions(
	ctx context.Context,
	_ *graphenepbv1.ListDefinitionsRequest,
) (*graphenepbv1.ListDefinitionsResponse, error) {
	if _, err := auth.Filter(ctx, auth.VerbList, registry.KindKind, nil); err != nil {
		return nil, denied(err)
	}

	defs, err := r.reg.List(ctx)
	if err != nil {
		return nil, mapRegistryErr(err)
	}

	return &graphenepbv1.ListDefinitionsResponse{Definitions: defs}, nil
}

func (r *Resources) WatchDefinitions(
	req *graphenepbv1.WatchDefinitionsRequest,
	srv graphenepbv1.ResourceService_WatchDefinitionsServer,
) error {
	if _, err := auth.Filter(srv.Context(), auth.VerbWatch, registry.KindKind, nil); err != nil {
		return denied(err)
	}

	events, err := r.st.Watch(srv.Context(), key.New(registry.KindKind).Encode(), req.GetFromStoreRevision())
	if err != nil {
		return internal(err)
	}

	for event := range events {
		if event.Type != store.EventPut {
			continue // definitions are never deleted; sync carries nothing
		}

		def := &graphenepbv1.ResourceDefinition{}
		if err := proto.Unmarshal(event.Entry.Value, def); err != nil {
			return internal(err)
		}

		if err := srv.Send(&graphenepbv1.WatchDefinitionsEvent{
			Definition:    def,
			StoreRevision: event.StoreRevision,
		}); err != nil {
			return fmt.Errorf("send definitions event: %w", err)
		}
	}

	return status.Error(codes.Aborted, "watch stream reset; re-watch from the last seen store_revision")
}

// --- helpers ------------------------------------------------------------

func storeKey(k *graphenepbv1.Key) ([]byte, error) {
	if k.GetKind() == "" {
		return nil, status.Error(codes.InvalidArgument, "key.kind is required")
	}

	if len(k.GetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key.path is required")
	}

	return key.FromProto(k).Encode(), nil
}

// DecodeEntry unmarshals a stored entry into the resource it holds —
// shared with in-process consumers of raw store events (controllers).
func DecodeEntry(e store.Entry) (*graphenepbv1.Resource, error) {
	return decodeResource(e)
}

// decodeResource unmarshals the stored payload and stamps the store-owned
// fields from the entry.
func decodeResource(e store.Entry) (*graphenepbv1.Resource, error) {
	res := &graphenepbv1.Resource{}
	if err := proto.Unmarshal(e.Value, res); err != nil {
		return nil, fmt.Errorf("decode resource: %w", err)
	}

	res.Revision = e.Revision
	res.CreatedRevision = e.CreatedRevision

	return res, nil
}

func mapEvent(event *store.Event) (*graphenepbv1.WatchEvent, error) {
	out := &graphenepbv1.WatchEvent{StoreRevision: event.StoreRevision}

	switch event.Type {
	case store.EventPut:
		out.Type = graphenepbv1.EventType_EVENT_TYPE_PUT

		res, err := decodeResource(event.Entry)
		if err != nil {
			return nil, err
		}

		out.Resource = res
	case store.EventDelete:
		out.Type = graphenepbv1.EventType_EVENT_TYPE_DELETE
		out.Resource = &graphenepbv1.Resource{
			Key:      key.Decode(event.Entry.Key).Proto(),
			Revision: event.Entry.Revision,
		}
	case store.EventSync:
		out.Type = graphenepbv1.EventType_EVENT_TYPE_SYNC
	}

	return out, nil
}

// matchSelector applies all FieldMatch terms (AND) to the resource.
// Paths are dotted, rooted at the envelope: "spec.placement", "status.phase".
func matchSelector(res *graphenepbv1.Resource, sel []*graphenepbv1.FieldMatch) bool {
	for _, m := range sel {
		if !auth.FieldEquals(res, m.GetPath(), m.GetValue()) {
			return false
		}
	}

	return true
}

// checkAuthority guards both directions at once: no writer may mint
// authority it does not hold, and none may destroy authority it does not
// hold either.
func (r *Resources) checkAuthority(ctx context.Context, kind string, res, current *graphenepbv1.Resource) error {
	if err := r.checkAuthorityWrite(ctx, kind, res); err != nil {
		return err
	}

	return r.checkAuthorityLoss(ctx, kind, current)
}

// checkAuthorityWrite applies the escalation guard to writes of the kinds
// that CARRY authority: defining a Role mints grants, and binding roles to
// an Identity hands those grants out. Either way the writer must already
// hold everything it gives away (auth.CheckEscalation).
func (r *Resources) checkAuthorityWrite(ctx context.Context, kind string, res *graphenepbv1.Resource) error {
	switch kind {
	case builtin.KindRole:
		grants, err := auth.GrantsFromSpec(res.GetSpec())
		if err != nil {
			return fmt.Errorf("decode role grants: %w", err)
		}

		if err := auth.CheckEscalation(ctx, grants); err != nil {
			return fmt.Errorf("role grants: %w", err)
		}

		return nil

	case builtin.KindIdentity:
		spec := auth.IdentityFromSpec(res.GetSpec())

		return r.checkRoles(ctx, spec.Roles, "binding role")

	case builtin.KindProcess:
		// A Process runs AS an identity: the kernel mints it a token, so
		// writing one hands out that identity's whole authority. Without
		// this check the escalation guard has an open door — you cannot
		// mint a powerful Role, but you could start a process running as
		// one and have it act for you.
		identity := processIdentity(res.GetSpec())
		if identity == "" {
			return nil // no credentials asked for, none given
		}

		roles, err := r.identityRoles(ctx, identity)
		if err != nil {
			return err
		}

		return r.checkRoles(ctx, roles, "running as "+identity+" via role")

	default:
		return nil
	}
}

// checkRoles applies the escalation guard to every named role: the writer
// must already hold everything the roles hand out.
func (r *Resources) checkRoles(ctx context.Context, roles []string, what string) error {
	for _, role := range roles {
		grants, err := r.roleGrants(ctx, role)
		if err != nil {
			return err
		}

		if err := auth.CheckEscalation(ctx, grants); err != nil {
			return fmt.Errorf("%s %q: %w", what, role, err)
		}
	}

	return nil
}

// identityRoles reads the roles an Identity carries. A process asking for
// an identity that does not exist is refused rather than deferred — the
// same rule as binding a missing role.
func (r *Resources) identityRoles(ctx context.Context, name string) ([]string, error) {
	entry, err := r.st.Get(ctx, key.New(builtin.KindIdentity, name).Encode())
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("%w: identity %s does not exist", auth.ErrDenied, name)
	}

	if err != nil {
		return nil, fmt.Errorf("read identity: %w", err)
	}

	res, err := decodeResource(entry)
	if err != nil {
		return nil, err
	}

	return auth.IdentityFromSpec(res.GetSpec()).Roles, nil
}

// processIdentity reads spec.identity, absent or non-string meaning none.
func processIdentity(spec *schemapb.StructValue) string {
	name, _ := spec.ToGo()["identity"].(string)

	return name
}

// checkAuthorityLoss guards the destruction of authority: removing or
// overwriting a Role, and removing or rebinding an Identity, must be done
// by someone who already holds the authority being taken away.
//
// Without it the escalation guard has a trivial bypass: a principal that
// cannot MINT the administrator's grants can still DELETE the role they
// come from, or overwrite the administrator's identity — disarming the
// system instead of escalating within it.
func (r *Resources) checkAuthorityLoss(ctx context.Context, kind string, current *graphenepbv1.Resource) error {
	if current == nil {
		return nil // nothing existed, nothing is lost
	}

	switch kind {
	case builtin.KindRole:
		grants, err := auth.GrantsFromSpec(current.GetSpec())
		if err != nil {
			// An undecodable role is authority nobody can account for:
			// only an unconfined holder may remove it.
			if err := auth.CheckEscalation(ctx, []auth.Grant{{Kind: "*"}}); err != nil {
				return fmt.Errorf("removing an undecodable role: %w", err)
			}

			return nil
		}

		if err := auth.CheckEscalation(ctx, grants); err != nil {
			return fmt.Errorf("removing role grants: %w", err)
		}

		return nil

	case builtin.KindIdentity:
		spec := auth.IdentityFromSpec(current.GetSpec())

		for _, role := range spec.Roles {
			grants, err := r.roleGrants(ctx, role)
			if err != nil {
				// The bound role is already gone: nothing to hold.
				continue
			}

			if err := auth.CheckEscalation(ctx, grants); err != nil {
				return fmt.Errorf("unbinding role %q: %w", role, err)
			}
		}

		return nil

	default:
		return nil
	}
}

// roleGrants reads a Role's grants; binding a role that does not exist is
// refused rather than deferred — an identity must never carry a name that
// silently gains meaning later.
func (r *Resources) roleGrants(ctx context.Context, role string) ([]auth.Grant, error) {
	entry, err := r.st.Get(ctx, key.New(builtin.KindRole, role).Encode())
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("%w: role %s does not exist", auth.ErrDenied, role)
	}

	if err != nil {
		return nil, fmt.Errorf("read role: %w", err)
	}

	res, err := decodeResource(entry)
	if err != nil {
		return nil, err
	}

	grants, err := auth.GrantsFromSpec(res.GetSpec())
	if err != nil {
		return nil, fmt.Errorf("decode role grants: %w", err)
	}

	return grants, nil
}

// currentRecord loads the existing record; nil (no error) when absent.
func (r *Resources) currentRecord(ctx context.Context, storedKey []byte) (*graphenepbv1.Resource, error) {
	entry, err := r.st.Get(ctx, storedKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil //nolint:nilnil // absence is a valid, non-error outcome here
	}

	if err != nil {
		return nil, internal(err)
	}

	current, err := decodeResource(entry)
	if err != nil {
		return nil, internal(err)
	}

	return current, nil
}

// nextGeneration counts INTENT, not writes. A create starts at 1; a spec
// change bumps; anything else — a status write, a finalizer removal —
// carries the current value over.
//
// This is what lets a controller ignore the echo of its own status write:
// the revision moved, the generation did not, so there is nothing new to
// act on. Without it "react to changes" is an infinite loop.
func nextGeneration(current, incoming *graphenepbv1.Resource) uint64 {
	if current == nil {
		return 1
	}

	if proto.Equal(current.GetSpec(), incoming.GetSpec()) {
		return current.GetGeneration()
	}

	return current.GetGeneration() + 1
}

// changedParts reports which writable sections a Put actually touches;
// nil current = create (every present part counts, spec always).
func changedParts(current, incoming *graphenepbv1.Resource) []auth.Part {
	if current == nil {
		parts := []auth.Part{auth.PartSpec}
		if len(incoming.GetStatus().GetFields()) > 0 {
			parts = append(parts, auth.PartStatus)
		}

		if len(incoming.GetFinalizers()) > 0 {
			parts = append(parts, auth.PartFinalizers)
		}

		return parts
	}

	var parts []auth.Part
	if !proto.Equal(current.GetSpec(), incoming.GetSpec()) {
		parts = append(parts, auth.PartSpec)
	}

	if !proto.Equal(current.GetStatus(), incoming.GetStatus()) {
		parts = append(parts, auth.PartStatus)
	}

	if !slices.Equal(current.GetFinalizers(), incoming.GetFinalizers()) {
		parts = append(parts, auth.PartFinalizers)
	}

	return parts
}

// eventView is what grant predicates evaluate: the resource for puts, the
// final (prev_kv) state for deletes.
func eventView(event *store.Event, out *graphenepbv1.WatchEvent) *graphenepbv1.Resource {
	if event.Type != store.EventDelete || len(event.Entry.Value) == 0 {
		return out.GetResource()
	}

	prev, err := decodeResource(event.Entry)
	if err != nil {
		return out.GetResource()
	}

	return prev
}

func denied(err error) error {
	if errors.Is(err, auth.ErrDenied) {
		return status.Error(codes.PermissionDenied, err.Error())
	}

	return internal(err)
}

func mapStoreErr(err error, target *graphenepbv1.Key) error {
	switch {
	case errors.Is(err, store.ErrRevisionMismatch):
		return status.Errorf(codes.Aborted, "%s: revision mismatch — re-read and retry", key.FromProto(target).String())
	case errors.Is(err, store.ErrNotFound):
		return status.Errorf(codes.NotFound, "%s not found", key.FromProto(target).String())
	case errors.Is(err, store.ErrCompacted):
		return status.Errorf(codes.OutOfRange, "%s: revision compacted", key.FromProto(target).String())
	default:
		return internal(err)
	}
}

func mapRegistryErr(err error) error {
	var verr *registry.ValidationError
	switch {
	case errors.As(err, &verr):
		return status.Error(codes.InvalidArgument, verr.Error())
	case errors.Is(err, registry.ErrUnknownKind),
		errors.Is(err, registry.ErrUnknownVersion):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, registry.ErrReservedKind):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return internal(err)
	}
}

func internal(err error) error {
	return status.Errorf(codes.Internal, "%v", err)
}
