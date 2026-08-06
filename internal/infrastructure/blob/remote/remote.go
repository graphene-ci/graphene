// Package remote is a blob store that is somebody else's.
//
// A kernel that keeps no bytes still has to run what it is told to run,
// so it reaches for them the same way it reaches for everything else:
// over the connection it already has. What it gets back is a blob.Store,
// and it is a store in the full sense — it passes the same suite the
// filesystem does, which is the only way "the same thing, further away"
// can be more than a claim.
package remote

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	blobpb "github.com/graphene-ci/graphenepb/v1/blob"

	"github.com/graphene-ci/graphene/internal/blob"
)

// chunk is how much one upload frame carries; the same size the far side
// sends back, for the same reasons.
const chunk = 256 << 10

// Store reaches a blob service.
type Store struct {
	client blobpb.BlobServiceClient
}

// Over builds a store on a connected client.
func Over(client blobpb.BlobServiceClient) *Store {
	return &Store{client: client}
}

// Close implements the port. The connection belongs to whoever opened it:
// a store that closed it would take down everything else riding on it.
func (s *Store) Close() error { return nil }

// Create starts an upload.
//
// The stream is opened here rather than at the first Write, so a caller
// that may not write bytes learns it before reading a file it was going
// to send.
func (s *Store) Create(ctx context.Context) (blob.Writer, error) {
	stream, err := s.client.Upload(ctx)
	if err != nil {
		return nil, fmt.Errorf("blob remote: begin upload: %w", err)
	}

	return &writer{stream: stream}, nil
}

// Open reads a blob from offset.
func (s *Store) Open(ctx context.Context, id blob.Id, offset uint64) (io.ReadCloser, blob.Info, error) {
	if err := usable(id); err != nil {
		return nil, blob.Info{}, err
	}

	stream, err := s.client.Download(ctx, &blobpb.DownloadRequest{
		Ref:    &blobpb.Ref{Id: id.String()},
		Offset: offset,
	})
	if err != nil {
		return nil, blob.Info{}, wrap(id, err)
	}

	// The first frame is the info, so what the caller gets back is the
	// same pair the filesystem gives: a reader and what is known about
	// it, before a byte has been read.
	first, err := stream.Recv()
	if err != nil {
		return nil, blob.Info{}, wrap(id, err)
	}

	info := first.GetInfo()
	if info == nil {
		return nil, blob.Info{}, fmt.Errorf("blob remote: %s: %w", id, errNoInfoFrame)
	}

	known, err := fromPb(info)
	if err != nil {
		return nil, blob.Info{}, err
	}

	return &reader{stream: stream}, known, nil
}

// Stat reports what is stored without reading it.
func (s *Store) Stat(ctx context.Context, id blob.Id) (blob.Info, error) {
	if err := usable(id); err != nil {
		return blob.Info{}, err
	}

	answer, err := s.client.Stat(ctx, &blobpb.StatRequest{Ref: &blobpb.Ref{Id: id.String()}})
	if err != nil {
		return blob.Info{}, wrap(id, err)
	}

	if !answer.GetExists() {
		return blob.Info{}, blob.ErrNotFound
	}

	return fromPb(answer.GetInfo())
}

// Delete removes a blob.
func (s *Store) Delete(ctx context.Context, id blob.Id) error {
	if err := usable(id); err != nil {
		return err
	}

	if _, err := s.client.Delete(ctx, &blobpb.DeleteRequest{Ref: &blobpb.Ref{Id: id.String()}}); err != nil {
		return wrap(id, err)
	}

	return nil
}

var errNoInfoFrame = errors.New("blob remote: the download did not begin with what it is")

// usable answers a forged id here rather than sending it, which saves a
// round trip and means a store one hop away refuses exactly what a local
// one refuses.
func usable(id blob.Id) error {
	if _, err := blob.NewId(id.String()); err != nil {
		return blob.ErrNotFound
	}

	return nil
}

// wrap turns the far side's answer into this side's vocabulary, so a
// caller written against the port never has to know a status code.
func wrap(id blob.Id, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return blob.ErrNotFound
	case codes.DataLoss:
		return blob.ErrChecksumMismatch
	default:
		return fmt.Errorf("blob remote: %s: %w", id, err)
	}
}

func fromPb(info *blobpb.Info) (blob.Info, error) {
	id, err := blob.NewId(info.GetRef().GetId())
	if err != nil {
		return blob.Info{}, fmt.Errorf("blob remote: %w", err)
	}

	return blob.Info{
		Id:     id,
		Size:   info.GetSize(),
		SHA256: info.GetChecksum().GetValue(),
	}, nil
}

// reader turns the frames after the first into an io.Reader. held is what
// a frame delivered beyond what the caller asked for.
type reader struct {
	stream blobpb.BlobService_DownloadClient
	held   []byte
}

func (r *reader) Read(p []byte) (int, error) {
	for len(r.held) == 0 {
		frame, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}

		if err != nil {
			return 0, fmt.Errorf("blob remote: download: %w", err)
		}

		r.held = frame.GetData()
	}

	copied := copy(p, r.held)
	r.held = r.held[copied:]

	return copied, nil
}

func (r *reader) Close() error { return nil }

// writer sends the frames of one upload: the bytes, then what they were.
type writer struct {
	stream   blobpb.BlobService_UploadClient
	finished bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.finished {
		return 0, errFinished
	}

	for sent := 0; sent < len(p); {
		end := min(sent+chunk, len(p))

		if err := w.stream.Send(&blobpb.UploadRequest{
			Frame: &blobpb.UploadRequest_Data{Data: p[sent:end]},
		}); err != nil {
			return sent, fmt.Errorf("blob remote: upload: %w", err)
		}

		sent = end
	}

	return len(p), nil
}

// Commit sends the declaration and asks the far side to seal the blob.
func (w *writer) Commit(sum []byte, size uint64) (blob.Info, error) {
	if w.finished {
		return blob.Info{}, errFinished
	}

	w.finished = true

	finish := &blobpb.UploadFinish{Size: size}
	if len(sum) > 0 {
		finish.Checksum = &blobpb.Checksum{
			Algorithm: blobpb.Algorithm_ALGORITHM_SHA256,
			Value:     sum,
		}
	}

	if err := w.stream.Send(&blobpb.UploadRequest{
		Frame: &blobpb.UploadRequest_Finish{Finish: finish},
	}); err != nil {
		return blob.Info{}, fmt.Errorf("blob remote: finish upload: %w", err)
	}

	done, err := w.stream.CloseAndRecv()
	if err != nil {
		return blob.Info{}, commitErr(err)
	}

	return fromPb(done.GetInfo())
}

func (w *writer) Abort() error {
	if w.finished {
		return nil
	}

	w.finished = true

	// Closing without a finish frame is how the far side is told to keep
	// nothing: an upload that was never completed is an upload nobody
	// asked to store.
	if err := w.stream.CloseSend(); err != nil {
		return fmt.Errorf("blob remote: abort: %w", err)
	}

	return nil
}

var errFinished = errors.New("blob remote: this upload is already finished")

// commitErr keeps the two refusals apart across the wire.
//
// They are two different faults — bytes that arrived corrupted, and
// fewer or more of them than were promised — and the far side gives them
// two codes precisely so that this side does not have to read an error
// message to tell which happened.
func commitErr(err error) error {
	switch status.Code(err) {
	case codes.DataLoss:
		return blob.ErrChecksumMismatch
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", blob.ErrSizeMismatch, status.Convert(err).Message())
	default:
		return fmt.Errorf("blob remote: commit: %w", err)
	}
}
