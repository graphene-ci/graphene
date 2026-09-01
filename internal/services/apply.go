package services

// Apply and Kinds: the two calls that make every other kind-specific
// door unnecessary. Creation goes through one verb, and what can be
// created is discovered rather than hard-coded.

import (
	"context"
	"encoding/json"
	"io"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/authz"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
	"github.com/graphene-ci/pipeline/pkg/obs"
)

// downloadChunkBytes bounds one Download stream frame.
const downloadChunkBytes = 256 << 10

// Download streams the bytes a record holds: the blob its state names
// (an artifact's blob, and any future kind whose state carries one).
func (m *Management) Download(ctx context.Context, creq *connect.Request[managementv1.DownloadRequest], stream *connect.ServerStream[managementv1.DownloadChunk]) error {
	ref := creq.Msg.GetRef()
	b, err := m.allow(ctx, authz.VerbGet, authz.KindOf(ref))
	if err != nil {
		return err
	}
	res, err := m.describe(ctx, b, ref)
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	var st struct {
		Blob struct {
			Location string `json:"location"`
		} `json:"blob"`
	}
	if len(res.GetState()) > 0 {
		_ = json.Unmarshal(res.GetState(), &st)
	}
	if st.Blob.Location == "" {
		return status.Errorf(codes.FailedPrecondition, "%s has no downloadable bytes", ref)
	}
	if m.Blobs == nil {
		return status.Error(codes.Unimplemented, "this installation has no blob store")
	}
	rc, err := m.Blobs.Get(ctx, b.Namespace, st.Blob.Location)
	if err != nil {
		return status.Errorf(codes.NotFound, "blob %s: %v", st.Blob.Location, err)
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, downloadChunkBytes)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&managementv1.DownloadChunk{Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "blob: %v", err)
		}
	}
}

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
	// The DOOR's half of a record's telemetry: a declaration is an
	// event in that entity's life, and it must be findable under the
	// entity, not only in the server's log file.
	octx := obs.WithEntity(ctx, ref)
	obs.Info(octx, "record applied", obs.Str("kind", req.GetKind()))
	obs.Count(octx, MetricRecordApplied, 1, obs.Str("kind", req.GetKind()))
	m.Log.Info("record applied",
		xlog.String("namespace", b.Namespace), xlog.String("ref", ref))
	return connect.NewResponse(&managementv1.ApplyResponse{Ref: ref}), nil
}
