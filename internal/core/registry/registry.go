// Package registry manages resource kind definitions — the CRD analog —
// and validates instances against them.
//
// Definitions are stored in the same truth store as everything else, under
// the reserved kind space "Kind": key = (Kind, <kind>, <version>), value =
// marshaled graphenepbv1.ResourceDefinition. Versions are monotonic per
// kind and assigned here on Define; nothing is ever overwritten — a new
// version is a new record (instances pin the version they were validated
// against).
package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/store"
)

// KindKind is the reserved kind space holding definitions themselves.
const (
	KindKind = "Kind"

	scanPage      = 512
	maxSegmentLen = 256
)

var (
	// ErrUnknownKind — no definition for the kind.
	ErrUnknownKind = errors.New("registry: unknown kind")
	// ErrUnknownVersion — the kind exists, this version does not.
	ErrUnknownVersion = errors.New("registry: unknown definition version")
	// ErrReservedKind — the kind name is system-reserved.
	ErrReservedKind = errors.New("registry: reserved kind")

	errUncloneable      = errors.New("registry: definition clone returned an unexpected type")
	errSegmentEmpty     = errors.New("empty")
	errSegmentTooLong   = errors.New("longer than 256 bytes")
	errSegmentSeparator = errors.New("contains a reserved separator character")
)

// ValidationError carries the reasons an instance was rejected.
type ValidationError struct {
	Reasons []string
}

func (e *ValidationError) Error() string {
	return "registry: invalid instance: " + strings.Join(e.Reasons, "; ")
}

// Registry is a thin domain layer over the store; safe for concurrent use.
type Registry struct {
	st store.Store
}

func New(st store.Store) *Registry {
	return &Registry{st: st}
}

// Define registers a new version of the kind and returns it. The version
// field of the input is ignored; the next monotonic version is assigned.
func (r *Registry) Define(ctx context.Context, def *graphenepbv1.ResourceDefinition) (uint32, error) {
	if err := validateDefinition(def); err != nil {
		return 0, err
	}

	// Assign version = latest+1 with a CAS-create; a concurrent Define of
	// the same kind loses the race and retries on the next number.
	for {
		latest, err := r.latest(ctx, def.GetKind())

		var version uint32

		switch {
		case err == nil:
			version = latest.GetVersion() + 1
		case errors.Is(err, ErrUnknownKind):
			version = 1
		default:
			return 0, err
		}

		next, ok := proto.Clone(def).(*graphenepbv1.ResourceDefinition)
		if !ok {
			return 0, errUncloneable
		}

		next.Version = version

		raw, err := proto.Marshal(next)
		if err != nil {
			return 0, fmt.Errorf("registry: marshal definition: %w", err)
		}

		_, err = r.st.Put(ctx, defKey(def.GetKind(), version), raw, 0)
		if errors.Is(err, store.ErrRevisionMismatch) {
			continue // lost the race, re-read latest
		}

		if err != nil {
			return 0, fmt.Errorf("registry: store definition: %w", err)
		}

		return version, nil
	}
}

// Get returns the definition of the kind; version 0 means latest.
func (r *Registry) Get(ctx context.Context, kind string, version uint32) (*graphenepbv1.ResourceDefinition, error) {
	if version == 0 {
		return r.latest(ctx, kind)
	}

	entry, err := r.st.Get(ctx, defKey(kind, version))
	if errors.Is(err, store.ErrNotFound) {
		// Distinguish "no kind at all" from "no such version".
		if _, lerr := r.latest(ctx, kind); errors.Is(lerr, ErrUnknownKind) {
			return nil, ErrUnknownKind
		}

		return nil, ErrUnknownVersion
	}

	if err != nil {
		return nil, fmt.Errorf("registry: read definition: %w", err)
	}

	return unmarshalDef(entry.Value)
}

// List returns the latest version of every defined kind, in kind order.
func (r *Registry) List(ctx context.Context) ([]*graphenepbv1.ResourceDefinition, error) {
	var (
		out    []*graphenepbv1.ResourceDefinition
		cursor []byte
	)

	byKind := map[string]*graphenepbv1.ResourceDefinition{}

	var order []string

	for {
		entries, next, err := r.st.Scan(ctx, key.New(KindKind).Encode(), scanPage, cursor)
		if err != nil {
			return nil, fmt.Errorf("registry: scan definitions: %w", err)
		}

		for _, e := range entries {
			def, err := unmarshalDef(e.Value)
			if err != nil {
				return nil, err
			}

			if _, seen := byKind[def.GetKind()]; !seen {
				order = append(order, def.GetKind())
			}
			// Ascending version order within a kind: the last one wins.
			byKind[def.GetKind()] = def
		}

		if next == nil {
			break
		}

		cursor = next
	}

	for _, k := range order {
		out = append(out, byKind[k])
	}

	return out, nil
}

// ValidateInstance checks an instance against its kind's definition:
// path shape, spec values, and status values (when present). version 0
// resolves to latest and is returned so the caller can pin it.
func (r *Registry) ValidateInstance(
	ctx context.Context,
	kind string,
	path []string,
	version uint32,
	spec *schemapb.StructValue,
	status *schemapb.StructValue,
) (uint32, error) {
	if kind == KindKind {
		return 0, ErrReservedKind
	}

	def, err := r.Get(ctx, kind, version)
	if err != nil {
		return 0, err
	}

	var reasons []string
	if len(path) != len(def.GetPathSegments()) {
		reasons = append(reasons, fmt.Sprintf(
			"path has %d segments, kind %q wants %d (%s)",
			len(path), kind, len(def.GetPathSegments()), strings.Join(def.GetPathSegments(), "/")))
	}

	for _, seg := range path {
		if err := validSegment(seg); err != nil {
			reasons = append(reasons, fmt.Sprintf("path segment %q: %v", seg, err))
		}
	}

	if verrs := validateValues(def.GetSpecSchema(), spec, "spec"); len(verrs) > 0 {
		reasons = append(reasons, verrs...)
	}
	// Status is validated only when present: instances are created with an
	// empty status, which is filled later by controllers.
	if status != nil && len(status.GetFields()) > 0 {
		if def.GetStatusSchema() == nil {
			reasons = append(reasons, "status present but kind defines no status_schema")
		} else if verrs := validateValues(def.GetStatusSchema(), status, "status"); len(verrs) > 0 {
			reasons = append(reasons, verrs...)
		}
	}

	if len(reasons) > 0 {
		return 0, &ValidationError{Reasons: reasons}
	}

	return def.GetVersion(), nil
}

func validateDefinition(def *graphenepbv1.ResourceDefinition) error {
	if def.GetKind() == "" {
		return &ValidationError{Reasons: []string{"kind is empty"}}
	}

	if def.GetKind() == KindKind {
		return ErrReservedKind
	}

	if err := validSegment(def.GetKind()); err != nil {
		return &ValidationError{Reasons: []string{fmt.Sprintf("kind: %v", err)}}
	}

	if len(def.GetPathSegments()) == 0 {
		return &ValidationError{Reasons: []string{"path_segments is empty"}}
	}

	for _, seg := range def.GetPathSegments() {
		if err := validSegment(seg); err != nil {
			return &ValidationError{Reasons: []string{fmt.Sprintf("path segment %q: %v", seg, err)}}
		}
	}

	if def.GetSpecSchema() == nil {
		return &ValidationError{Reasons: []string{"spec_schema is nil"}}
	}

	return nil
}

func validateValues(schema *schemapb.Schema, values *schemapb.StructValue, part string) []string {
	res, err := schema.Validate(values.ToGo())
	if err != nil {
		return []string{fmt.Sprintf("%s: schema does not compile: %v", part, err)}
	}

	if res.Ok() {
		return nil
	}

	var out []string
	for _, verr := range res.GetErrors() {
		out = append(out, fmt.Sprintf("%s.%s: %s", part, verr.GetPath(), verr.GetCode().String()))
	}

	return out
}

func (r *Registry) latest(ctx context.Context, kind string) (*graphenepbv1.ResourceDefinition, error) {
	// Versions are zero-padded: ascending key order == ascending version
	// order; the last entry under the kind prefix is the latest.
	var (
		last   []byte
		cursor []byte
	)
	for {
		entries, next, err := r.st.Scan(ctx, key.New(KindKind, kind).Encode(), scanPage, cursor)
		if err != nil {
			return nil, fmt.Errorf("registry: scan definitions: %w", err)
		}

		if len(entries) > 0 {
			last = entries[len(entries)-1].Value
		}

		if next == nil {
			break
		}

		cursor = next
	}

	if last == nil {
		return nil, ErrUnknownKind
	}

	return unmarshalDef(last)
}

func defKey(kind string, version uint32) []byte {
	return key.New(KindKind, kind, fmt.Sprintf("%010d", version)).Encode()
}

func unmarshalDef(raw []byte) (*graphenepbv1.ResourceDefinition, error) {
	def := &graphenepbv1.ResourceDefinition{}
	if err := proto.Unmarshal(raw, def); err != nil {
		return nil, fmt.Errorf("registry: unmarshal definition: %w", err)
	}

	return def, nil
}

// validSegment enforces what the store's key encoding assumes: non-empty,
// no separator bytes, no '/', sane length.
func validSegment(s string) error {
	if s == "" {
		return errSegmentEmpty
	}

	if len(s) > maxSegmentLen {
		return errSegmentTooLong
	}

	if strings.ContainsAny(s, "/\x1e\x1f") {
		return errSegmentSeparator
	}

	return nil
}
