package services

// Apply and Kinds: the two calls that make every other kind-specific
// door unnecessary. Creation goes through one verb, and what can be
// created is discovered rather than hard-coded.

import (
	"context"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/graphene/internal/authz"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// Apply declares a record of any kind this installation knows.
func (m *Management) Apply(ctx context.Context, creq *connect.Request[managementv1.ApplyRequest]) (*connect.Response[managementv1.ApplyResponse], error) {
	req := creq.Msg
	if req.GetKind() == "" || req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "kind and id are required")
	}
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindOf(req.GetKind()+"/"))
	if err != nil {
		return nil, err
	}
	ref, err := b.Worker.Apply(ctx, req.GetKind(), req.GetId(), req.GetSpec(), req.GetLabels())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	m.audit(ctx, b, ref, authz.VerbCreate)
	m.Log.Info("record applied",
		xlog.String("namespace", b.Namespace), xlog.String("ref", ref))
	return connect.NewResponse(&managementv1.ApplyResponse{Ref: ref}), nil
}

// Kinds answers what can be declared and commanded here.
func (m *Management) Kinds(ctx context.Context, _ *connect.Request[managementv1.KindsRequest]) (*connect.Response[managementv1.KindsResponse], error) {
	b, err := m.allow(ctx, authz.VerbList, authz.KindAll)
	if err != nil {
		return nil, err
	}
	out := &managementv1.KindsResponse{}
	for _, k := range b.Worker.Kinds() {
		kind := &managementv1.KindsResponse_Kind{
			Name: k.Name, Declarable: k.Declarable, Description: k.Description,
		}
		if k.Spec != nil {
			if raw, err := protojson.Marshal(k.Spec); err == nil {
				kind.SpecSchema = raw
			}
		}
		for _, c := range k.Commands {
			command := &managementv1.KindsResponse_Command{Name: c.Name}
			if c.Payload != nil {
				if raw, err := protojson.Marshal(c.Payload); err == nil {
					command.PayloadSchema = raw
				}
			}
			kind.Commands = append(kind.Commands, command)
		}
		out.Kinds = append(out.Kinds, kind)
	}
	return connect.NewResponse(out), nil
}
