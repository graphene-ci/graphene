// Package service implements the resource API semantics on top of the
// store and the registry: instance validation with definition pinning,
// CAS writes, graceful deletion via finalizers, selector filtering and
// watch mapping.
//
// This is the domain layer: it implements the generated gRPC server
// interfaces directly (the proto IS our contract), while transport
// concerns (listeners, uds, tokens) live in infrastructure.
//
// Storage layout: the store value is the marshalled Resource with the
// store-owned fields (revision, created_revision) zeroed; they are stamped
// back from store entries on every read. Definitions live under the
// reserved kind space (see registry).
package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/store"
)

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
	key, err := storeKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	entry, err := r.st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s not found", keyString(req.GetKey()))
	}
	if err != nil {
		return nil, internal(err)
	}
	res, err := decodeResource(entry)
	if err != nil {
		return nil, internal(err)
	}
	return &graphenepbv1.GetResponse{Resource: res}, nil
}

func (r *Resources) Put(ctx context.Context, req *graphenepbv1.PutRequest) (*graphenepbv1.PutResponse, error) {
	res := req.GetResource()
	key, err := storeKey(res.GetKey())
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
	var current *graphenepbv1.Resource
	if entry, err := r.st.Get(ctx, key); err == nil {
		if current, err = decodeResource(entry); err != nil {
			return nil, internal(err)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, internal(err)
	}

	stored := proto.Clone(res).(*graphenepbv1.Resource)
	stored.Revision = 0
	stored.CreatedRevision = 0
	stored.DefinitionVersion = pinned
	stored.Deleting = current.GetDeleting()

	// Finalize-commit path: the resource is deleting and the last
	// finalizer was just removed — the Put turns into the real removal.
	if stored.GetDeleting() && len(stored.GetFinalizers()) == 0 {
		rev, err := r.st.Delete(ctx, key, req.GetExpectedRevision())
		if err != nil {
			return nil, mapStoreErr(err, res.GetKey())
		}
		return &graphenepbv1.PutResponse{Revision: rev, StoreRevision: rev}, nil
	}

	raw, err := proto.Marshal(stored)
	if err != nil {
		return nil, internal(err)
	}
	rev, err := r.st.Put(ctx, key, raw, req.GetExpectedRevision())
	if err != nil {
		return nil, mapStoreErr(err, res.GetKey())
	}
	return &graphenepbv1.PutResponse{Revision: rev, StoreRevision: rev}, nil
}

func (r *Resources) Delete(ctx context.Context, req *graphenepbv1.DeleteRequest) (*graphenepbv1.DeleteResponse, error) {
	key, err := storeKey(req.GetKey())
	if err != nil {
		return nil, err
	}
	entry, err := r.st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "%s not found", keyString(req.GetKey()))
	}
	if err != nil {
		return nil, internal(err)
	}
	current, err := decodeResource(entry)
	if err != nil {
		return nil, internal(err)
	}

	// No finalizers — remove immediately.
	if len(current.GetFinalizers()) == 0 {
		if _, err := r.st.Delete(ctx, key, req.GetExpectedRevision()); err != nil {
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
	if _, err := r.st.Put(ctx, key, raw, req.GetExpectedRevision()); err != nil {
		return nil, mapStoreErr(err, req.GetKey())
	}
	return &graphenepbv1.DeleteResponse{}, nil
}

func (r *Resources) List(ctx context.Context, req *graphenepbv1.ListRequest) (*graphenepbv1.ListResponse, error) {
	if req.GetKind() == "" {
		return nil, status.Error(codes.InvalidArgument, "kind is required")
	}
	prefix := store.EncodePrefix(req.GetKind(), req.GetPathPrefix()...)

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
		if matchSelector(res, req.GetSelector()) {
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
	prefix := store.EncodePrefix(req.GetKind(), req.GetPathPrefix()...)

	ctx := srv.Context()
	events, err := r.st.Watch(ctx, prefix, req.GetFromStoreRevision())
	if err != nil {
		return internal(err)
	}
	for ev := range events {
		out, err := mapEvent(ev)
		if err != nil {
			return internal(err)
		}
		// Deletes pass the selector always: the final state is gone and
		// the watcher must be told regardless of its filter.
		if ev.Type == store.EventPut && !matchSelector(out.GetResource(), req.GetSelector()) {
			continue
		}
		if err := srv.Send(out); err != nil {
			return err
		}
	}
	// Channel closed: store shut down or the consumer was too slow —
	// the client re-watches from its last seen store revision.
	return status.Error(codes.Aborted, "watch stream reset; re-watch from the last seen store_revision")
}

// --- definitions --------------------------------------------------------

func (r *Resources) Define(ctx context.Context, req *graphenepbv1.DefineRequest) (*graphenepbv1.DefineResponse, error) {
	version, err := r.reg.Define(ctx, req.GetDefinition())
	if err != nil {
		return nil, mapRegistryErr(err)
	}
	return &graphenepbv1.DefineResponse{Version: version}, nil
}

func (r *Resources) GetDefinition(ctx context.Context, req *graphenepbv1.GetDefinitionRequest) (*graphenepbv1.GetDefinitionResponse, error) {
	def, err := r.reg.Get(ctx, req.GetKind(), req.GetVersion())
	if err != nil {
		return nil, mapRegistryErr(err)
	}
	return &graphenepbv1.GetDefinitionResponse{Definition: def}, nil
}

func (r *Resources) ListDefinitions(ctx context.Context, req *graphenepbv1.ListDefinitionsRequest) (*graphenepbv1.ListDefinitionsResponse, error) {
	defs, err := r.reg.List(ctx)
	if err != nil {
		return nil, mapRegistryErr(err)
	}
	return &graphenepbv1.ListDefinitionsResponse{Definitions: defs}, nil
}

func (r *Resources) WatchDefinitions(req *graphenepbv1.WatchDefinitionsRequest, srv graphenepbv1.ResourceService_WatchDefinitionsServer) error {
	events, err := r.st.Watch(srv.Context(), store.EncodePrefix(registry.KindKind), req.GetFromStoreRevision())
	if err != nil {
		return internal(err)
	}
	for ev := range events {
		if ev.Type != store.EventPut {
			continue // definitions are never deleted
		}
		def := &graphenepbv1.ResourceDefinition{}
		if err := proto.Unmarshal(ev.Entry.Value, def); err != nil {
			return internal(err)
		}
		if err := srv.Send(&graphenepbv1.WatchDefinitionsEvent{
			Definition:    def,
			StoreRevision: ev.StoreRevision,
		}); err != nil {
			return err
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
	return store.EncodeKey(k.GetKind(), k.GetPath()...), nil
}

func keyString(k *graphenepbv1.Key) string {
	return k.GetKind() + "/" + strings.Join(k.GetPath(), "/")
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

func mapEvent(ev store.Event) (*graphenepbv1.WatchEvent, error) {
	out := &graphenepbv1.WatchEvent{StoreRevision: ev.StoreRevision}
	switch ev.Type {
	case store.EventPut:
		out.Type = graphenepbv1.EventType_EVENT_TYPE_PUT
		res, err := decodeResource(ev.Entry)
		if err != nil {
			return nil, err
		}
		out.Resource = res
	case store.EventDelete:
		out.Type = graphenepbv1.EventType_EVENT_TYPE_DELETE
		kind, path := store.DecodeKey(ev.Entry.Key)
		out.Resource = &graphenepbv1.Resource{
			Key:      &graphenepbv1.Key{Kind: kind, Path: path},
			Revision: ev.Entry.Revision,
		}
	}
	return out, nil
}

// matchSelector applies all FieldMatch terms (AND) to the resource.
// Paths are dotted, rooted at the envelope: "spec.placement", "status.phase".
func matchSelector(res *graphenepbv1.Resource, sel []*graphenepbv1.FieldMatch) bool {
	for _, m := range sel {
		if !matchField(res, m.GetPath(), m.GetValue()) {
			return false
		}
	}
	return true
}

func matchField(res *graphenepbv1.Resource, path, want string) bool {
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return false
	}
	var root *schemapb.StructValue
	switch parts[0] {
	case "spec":
		root = res.GetSpec()
	case "status":
		root = res.GetStatus()
	default:
		return false
	}
	val, ok := lookup(root.ToGo(), parts[1:])
	if !ok {
		return false
	}
	return fmt.Sprintf("%v", val) == want
}

func lookup(m map[string]any, path []string) (any, bool) {
	var cur any = m
	for _, p := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = obj[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func mapStoreErr(err error, key *graphenepbv1.Key) error {
	switch {
	case errors.Is(err, store.ErrRevisionMismatch):
		return status.Errorf(codes.Aborted, "%s: revision mismatch — re-read and retry", keyString(key))
	case errors.Is(err, store.ErrNotFound):
		return status.Errorf(codes.NotFound, "%s not found", keyString(key))
	case errors.Is(err, store.ErrCompacted):
		return status.Errorf(codes.OutOfRange, "%s: revision compacted", keyString(key))
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
