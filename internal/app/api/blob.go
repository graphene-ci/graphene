package api

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gopherex/xlog"

	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
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
// of thing — large, streamed, and not a value anything validates. What is
// NOT separate is the record about them: every blob has an ordinary
// resource, and every question of permission, existence and integrity is
// asked of that record through the ordinary kernel.
//
// So this service moves bytes and decides nothing. It writes the record
// after the bytes are whole, removes it before the bytes go, and lets the
// kernel refuse either.
type Blobs struct {
	blobpb.UnimplementedBlobServiceServer

	bytes blob.Store
	guard auth.Guard
	who   Identify
	log   *xlog.Logger
}

// NewBlobs builds the service over the raw byte store.
//
// The store is UNGUARDED, and that is deliberate: what may be read or
// written is decided by the record, through the guard, and a second
// permission in front of the bytes would be a second answer to the same
// question.
func NewBlobs(bytes blob.Store, guard auth.Guard, who Identify, log *xlog.Logger) *Blobs {
	return &Blobs{bytes: bytes, guard: guard, who: who, log: log}
}

// session works out who is calling and binds the guard to them.
func (b *Blobs) session(ctx context.Context) (auth.Session, error) {
	named, err := b.who(ctx)
	if err != nil {
		return auth.Session{}, err
	}

	return b.guard.As(named), nil
}

// Stat reports what is under an id.
func (b *Blobs) Stat(ctx context.Context, request *blobpb.StatRequest) (*blobpb.StatResponse, error) {
	session, err := b.session(ctx)
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

	// The RECORD and not the bytes. What is stored under an id is a fact
	// written down when it was stored, so reading it is one key rather
	// than a stat on a file — and it goes through the guard, which is the
	// only place that decides anything here.
	stored, err := b.record(ctx, session, id)
	if errors.Is(err, store.ErrNotFound) {
		return &blobpb.StatResponse{}, nil
	}

	if err != nil {
		return nil, b.fail(err)
	}

	return &blobpb.StatResponse{Exists: true, Info: infoToPb(blob.Told(id, stored.Value))}, nil
}

// record reads what a blob's resource says, as this caller may.
func (b *Blobs) record(
	ctx context.Context, session auth.Session, id blob.Id,
) (store.Value[resource.Resource], error) {
	at, err := blob.ResourceId(id)
	if err != nil {
		return store.Value[resource.Resource]{}, err
	}

	return session.Get(ctx, at)
}

// Upload streams bytes in and names them.
//
// The blob is written as the frames arrive rather than gathered first: a
// bundle is as big as somebody's binary, and a kernel that held one in
// memory to hash it would fall over on the machine that needed it most.
func (b *Blobs) Upload(in blobpb.BlobService_UploadServer) error {
	ctx := in.Context()

	session, err := b.session(ctx)
	if err != nil {
		return b.fail(err)
	}

	// A SCREEN AND NOT THE DECISION. The id is not known until the bytes
	// have arrived, so the grant that finally allows this — which may
	// name a prefix — can only be checked at the end. Asking first
	// whether this caller may write blobs AT ALL is what keeps somebody
	// with no business here from making the kernel write a gigabyte and
	// then throw it away.
	if err := session.May(ctx, auth.Put, blob.Kind); err != nil {
		return b.fail(err)
	}

	writer, err := b.bytes.Create(ctx)
	if err != nil {
		return b.fail(err)
	}

	info, err := b.receive(in, writer)
	if err != nil {
		_ = writer.Abort()

		return b.fail(err)
	}

	// The record LAST, and it is the record that decides. Bytes with no
	// resource are collectable litter; a resource naming bytes that never
	// arrived would be a process that cannot start.
	if err := b.write(ctx, session, info); err != nil {
		_ = b.bytes.Delete(ctx, info.Id)

		return b.fail(err)
	}

	return in.SendAndClose(&blobpb.UploadResponse{Info: infoToPb(info)})
}

// write puts down the resource that says these bytes exist.
func (b *Blobs) write(ctx context.Context, session auth.Session, info blob.Info) error {
	at, err := blob.ResourceId(info.Id)
	if err != nil {
		return err
	}

	intent, err := resource.NewIntent(at, blob.Said(info))
	if err != nil {
		return err
	}

	if _, err := session.Put(ctx, intent, revision.Absent); err != nil {
		return fmt.Errorf("record %s: %w", at, err)
	}

	return nil
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
			return blob.Info{}, fmt.Errorf("write the blob: %w", err)
		}
	}
}

// errUnfinished — the sender stopped without saying what it sent.
var errUnfinished = errors.New("upload ended without a finish frame; nothing was stored")

// Download streams bytes out, info first.
func (b *Blobs) Download(request *blobpb.DownloadRequest, out blobpb.BlobService_DownloadServer) error {
	ctx := out.Context()

	session, err := b.session(ctx)
	if err != nil {
		return b.fail(err)
	}

	id, err := blob.NewId(request.GetRef().GetId())
	if err != nil {
		return status.Error(codes.NotFound, "no such blob")
	}

	// Permission and existence in one read, of the record, through the
	// guard. The bytes are opened only once that has answered.
	stored, err := b.record(ctx, session, id)
	if err != nil {
		return b.fail(err)
	}

	reader, _, err := b.bytes.Open(ctx, id, request.GetOffset())
	if err != nil {
		return b.fail(err)
	}

	defer func() { _ = reader.Close() }()

	if err := out.Send(&blobpb.DownloadResponse{
		Frame: &blobpb.DownloadResponse_Info{Info: infoToPb(blob.Told(id, stored.Value))},
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

// Delete removes a blob, if nothing is pointing at it.
//
// The RECORD goes first, and that ordering is the whole safety of it: the
// kernel refuses to remove a resource something holds, so bytes a process
// is running cannot be taken away. Interrupted between the two, what is
// left is bytes nobody references — litter, which a collector finds, and
// not a reference to nothing, which nothing can fix.
func (b *Blobs) Delete(ctx context.Context, request *blobpb.DeleteRequest) (*blobpb.DeleteResponse, error) {
	session, err := b.session(ctx)
	if err != nil {
		return nil, b.fail(err)
	}

	id, err := blob.NewId(request.GetRef().GetId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "no such blob")
	}

	stored, err := b.record(ctx, session, id)
	if err != nil {
		return nil, b.fail(err)
	}

	at, err := blob.ResourceId(id)
	if err != nil {
		return nil, b.fail(err)
	}

	if _, err := session.Delete(ctx, at, stored.Revision); err != nil {
		return nil, b.fail(err)
	}

	if err := b.bytes.Delete(ctx, id); err != nil {
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
