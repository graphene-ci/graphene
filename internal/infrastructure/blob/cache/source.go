package cache

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/blob"
)

// OverStore reads the blob store this kernel holds; OverClient reads a
// kernel a link away. The same pair as everywhere else in the system: a
// kernel that holds the truth and a kernel that reaches it behave the
// same to everything above them.
func OverStore(store blob.Store) blob.Reader { return &storeSource{store: store} }

// OverClient reads bytes through the ordinary BlobService.
func OverClient(client graphenepbv1.BlobServiceClient) blob.Reader {
	return &clientSource{client: client}
}

type storeSource struct{ store blob.Store }

func (s *storeSource) Open(ctx context.Context, id string) (io.ReadCloser, blob.Info, error) {
	reader, info, err := s.store.Open(ctx, id, 0)
	if err != nil {
		return nil, blob.Info{}, fmt.Errorf("cache: read blob %s: %w", id, err)
	}

	return reader, info, nil
}

type clientSource struct {
	client graphenepbv1.BlobServiceClient
}

// Open starts a download and hands back a reader over the stream, so the
// bytes are never all held in memory at once — a bundle is as big as
// someone's binary, and a kernel may be a small machine.
func (s *clientSource) Open(ctx context.Context, id string) (io.ReadCloser, blob.Info, error) {
	stream, err := s.client.Download(ctx, &graphenepbv1.DownloadRequest{
		Ref: &graphenepbv1.BlobRef{Id: id},
	})
	if err != nil {
		return nil, blob.Info{}, downloadErr(id, err)
	}

	// The first frame carries the size and checksum; anything else means
	// the far side is not speaking the contract.
	first, err := stream.Recv()
	if err != nil {
		return nil, blob.Info{}, downloadErr(id, err)
	}

	info := first.GetInfo()
	if info == nil {
		return nil, blob.Info{}, fmt.Errorf("cache: blob %s: %w", id, errNoInfoFrame)
	}

	return &streamReader{stream: stream}, blob.Info{
		ID:     info.GetRef().GetId(),
		Size:   info.GetSize(),
		SHA256: info.GetChecksum().GetValue(),
	}, nil
}

var errNoInfoFrame = errors.New("download did not start with the info frame")

func downloadErr(id string, err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("cache: blob %s: %w", id, blob.ErrNotFound)
	}

	return fmt.Errorf("cache: download blob %s: %w", id, err)
}

// streamReader turns the frames into an io.Reader; leftover holds what a
// frame delivered beyond what the caller asked for.
type streamReader struct {
	stream   grpc.ServerStreamingClient[graphenepbv1.DownloadResponse]
	leftover []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.leftover) == 0 {
		msg, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}

		if err != nil {
			return 0, fmt.Errorf("cache: download: %w", err)
		}

		r.leftover = msg.GetData()
	}

	n := copy(p, r.leftover)
	r.leftover = r.leftover[n:]

	return n, nil
}

func (r *streamReader) Close() error { return nil }
