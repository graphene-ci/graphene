package api

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gopherex/xlog"

	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
)

// chunk is how much of a blob one frame carries.
//
// Big enough that a large file is not a hundred thousand round trips,
// small enough to stay well under the transport's message limit — which
// a frame must, because exceeding it fails the whole call rather than
// splitting it.
const chunk = 256 << 10

// Blobs answers for the byte store.
//
// A separate service from the kernel's because bytes are a separate kind
// of thing, and a separate type here because the two are authorized
// differently: the kernel's methods each carry a resource to confine a
// grant to, and an opaque id carries nothing.
type Blobs struct {
	blobpb.UnimplementedBlobServiceServer

	as  Caller
	log *xlog.Logger
}

// Caller hands back the store as one caller may use it.
//
// A function rather than a store plus a guard, because who is calling is
// known per request and nothing here should be deciding what to do about
// it. The composition root says how a caller becomes a permission; this
// package translates requests and answers, which is all it claims to do.
type Caller func(ctx context.Context) (blob.Store, error)

// NewBlobs builds the service.
func NewBlobs(as Caller, log *xlog.Logger) *Blobs {
	return &Blobs{as: as, log: log}
}

// Guarded is how a kernel turns a caller into the store they may use.
//
// It lives here rather than in blob because working out WHO is calling is
// a transport's business, and this package is where the transport ends.
//
//nolint:gocritic // a guard is a value; one exists per kernel, not per call
func Guarded(store blob.Store, guard auth.Guard, who func(context.Context) (auth.Principal, error)) Caller {
	return func(ctx context.Context) (blob.Store, error) {
		named, err := who(ctx)
		if err != nil {
			return nil, err
		}

		return blob.Guard(store, permission{session: guard.As(named)}), nil
	}
}

// Stat reports what is under an id.
func (b *Blobs) Stat(ctx context.Context, request *blobpb.StatRequest) (*blobpb.StatResponse, error) {
	store, err := b.as(ctx)
	if err != nil {
		return nil, b.fail(err)
	}

	// An id that is not one is an id nobody has. Answering "no" rather
	// than "malformed" keeps a probe from learning what a real id looks
	// like by being told which of its guesses were the wrong SHAPE and
	// which were merely absent.
	id, err := blob.NewId(request.GetRef().GetId())
	if err != nil {
		return &blobpb.StatResponse{}, nil //nolint:nilerr // absent and unusable are one answer
	}

	info, err := store.Stat(ctx, id)
	if errors.Is(err, blob.ErrNotFound) {
		return &blobpb.StatResponse{}, nil
	}

	if err != nil {
		return nil, b.fail(err)
	}

	return &blobpb.StatResponse{Exists: true, Info: infoToPb(info)}, nil
}

// Upload streams bytes in and names them.
//
// The blob is written as the frames arrive rather than gathered first: a
// bundle is as big as somebody's binary, and a kernel that held one in
// memory to hash it would fall over on the machine that needed it most.
func (b *Blobs) Upload(in blobpb.BlobService_UploadServer) error {
	ctx := in.Context()

	store, err := b.as(ctx)
	if err != nil {
		return b.fail(err)
	}

	writer, err := store.Create(ctx)
	if err != nil {
		return b.fail(err)
	}

	info, err := b.receive(in, writer)
	if err != nil {
		_ = writer.Abort()

		return b.fail(err)
	}

	return in.SendAndClose(&blobpb.UploadResponse{Info: infoToPb(info)})
}

// receive copies the frames into the writer and seals it when the sender
// says what it sent.
//
// A stream that ends without saying is an upload nobody completed, and
// nothing is stored. That is the difference between a client that changed
// its mind and one whose connection died mid-file — which used to look
// identical, and used to be stored.
func (b *Blobs) receive(in blobpb.BlobService_UploadServer, writer blob.Writer) (blob.Info, error) {
	for {
		frame, err := in.Recv()
		if errors.Is(err, io.EOF) {
			return blob.Info{}, errUnfinished
		}

		if err != nil {
			return blob.Info{}, err
		}

		if finish := frame.GetFinish(); finish != nil {
			return writer.Commit(finish.GetChecksum().GetValue(), finish.GetSize())
		}

		if _, err := writer.Write(frame.GetData()); err != nil {
			return blob.Info{}, err
		}
	}
}

// errUnfinished — the sender stopped without saying what it sent.
var errUnfinished = errors.New("upload ended without a finish frame; nothing was stored")

// Download streams bytes out, info first.
func (b *Blobs) Download(request *blobpb.DownloadRequest, out blobpb.BlobService_DownloadServer) error {
	ctx := out.Context()

	store, err := b.as(ctx)
	if err != nil {
		return b.fail(err)
	}

	id, err := blob.NewId(request.GetRef().GetId())
	if err != nil {
		return status.Error(codes.NotFound, "no such blob")
	}

	reader, info, err := store.Open(ctx, id, request.GetOffset())
	if err != nil {
		return b.fail(err)
	}

	defer func() { _ = reader.Close() }()

	if err := out.Send(&blobpb.DownloadResponse{
		Frame: &blobpb.DownloadResponse_Info{Info: infoToPb(info)},
	}); err != nil {
		return b.fail(err)
	}

	return b.send(reader, out)
}

func (b *Blobs) send(reader io.Reader, out blobpb.BlobService_DownloadServer) error {
	buffer := make([]byte, chunk)

	for {
		read, err := reader.Read(buffer)
		if read > 0 {
			if err := out.Send(&blobpb.DownloadResponse{
				Frame: &blobpb.DownloadResponse_Data{Data: buffer[:read]},
			}); err != nil {
				return b.fail(err)
			}
		}

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return b.fail(err)
		}
	}
}

// Delete removes a blob.
func (b *Blobs) Delete(ctx context.Context, request *blobpb.DeleteRequest) (*blobpb.DeleteResponse, error) {
	store, err := b.as(ctx)
	if err != nil {
		return nil, b.fail(err)
	}

	id, err := blob.NewId(request.GetRef().GetId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "no such blob")
	}

	if err := store.Delete(ctx, id); err != nil {
		return nil, b.fail(err)
	}

	return &blobpb.DeleteResponse{}, nil
}

func (b *Blobs) fail(err error) error {
	return fail(err, func(unexpected error) {
		b.log.Error("unexpected failure", xlog.Err(unexpected))
	})
}

func infoToPb(info blob.Info) *blobpb.Info {
	return &blobpb.Info{
		Ref:  &blobpb.Ref{Id: info.Id.String()},
		Size: info.Size,
		Checksum: &blobpb.Checksum{
			Algorithm: blobpb.Algorithm_ALGORITHM_SHA256,
			Value:     info.SHA256,
		},
	}
}

// permission answers the byte store's three questions out of the same
// grants everything else is answered from.
type permission struct{ session auth.Session }

//nolint:gocritic // a session is a value; there is one per call already
func (p permission) MayRead(ctx context.Context) error {
	return p.session.May(ctx, auth.Get, blob.Kind)
}

//nolint:gocritic // see MayRead
func (p permission) MayWrite(ctx context.Context) error {
	return p.session.May(ctx, auth.Put, blob.Kind)
}

//nolint:gocritic // see MayRead
func (p permission) MayDelete(ctx context.Context) error {
	return p.session.May(ctx, auth.Delete, blob.Kind)
}
