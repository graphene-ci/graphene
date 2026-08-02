package service

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/blob"
)

// blobKind is the pseudo-kind blob operations are authorized against:
// the same grant machinery, object-less (see auth.Check).
const blobKind = "Blob"

const (
	defaultChunk = 256 * 1024
	maxChunk     = 1024 * 1024
)

// Blobs implements graphenepbv1.BlobServiceServer over the blob port.
type Blobs struct {
	graphenepbv1.UnimplementedBlobServiceServer

	st blob.Store
}

func NewBlobs(st blob.Store) *Blobs {
	return &Blobs{st: st}
}

// Stat implements the cheap existence probe.
func (b *Blobs) Stat(ctx context.Context, req *graphenepbv1.StatRequest) (*graphenepbv1.StatResponse, error) {
	if err := auth.Check(ctx, auth.VerbGet, blobKind); err != nil {
		return nil, denied(err)
	}

	info, err := b.st.Stat(ctx, req.GetRef().GetId())
	if errors.Is(err, blob.ErrNotFound) {
		return &graphenepbv1.StatResponse{Exists: false}, nil
	}

	if err != nil {
		return nil, internal(err)
	}

	return &graphenepbv1.StatResponse{Exists: true, Info: blobInfo(info)}, nil
}

// Upload implements the client-stream: one open frame, then data frames.
func (b *Blobs) Upload(stream graphenepbv1.BlobService_UploadServer) error {
	ctx := stream.Context()
	if err := auth.Check(ctx, auth.VerbPut, blobKind); err != nil {
		return denied(err)
	}

	open, err := receiveOpen(stream)
	if err != nil {
		return err
	}

	writer, err := b.st.Create(ctx)
	if err != nil {
		return internal(err)
	}

	if err := receiveData(stream, writer); err != nil {
		_ = writer.Abort()

		return err
	}

	info, err := writer.Commit(open.GetExpectedChecksum().GetValue(), open.GetExpectedSize())
	if errors.Is(err, blob.ErrChecksumMismatch) {
		return status.Error(codes.DataLoss, "declared checksum/size does not match the received bytes")
	}

	if err != nil {
		return internal(err)
	}

	if err := stream.SendAndClose(&graphenepbv1.UploadResponse{Info: blobInfo(info)}); err != nil {
		return fmt.Errorf("send upload response: %w", err)
	}

	return nil
}

func receiveOpen(stream graphenepbv1.BlobService_UploadServer) (*graphenepbv1.UploadOpen, error) {
	first, err := stream.Recv()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "empty upload stream")
	}

	open := first.GetOpen()
	if open == nil {
		return nil, status.Error(codes.InvalidArgument, "first upload frame must be open")
	}

	return open, nil
}

func receiveData(stream graphenepbv1.BlobService_UploadServer, writer io.Writer) error {
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("receive upload frame: %w", err)
		}

		if frame.GetOpen() != nil {
			return status.Error(codes.InvalidArgument, "duplicate open frame")
		}

		if _, err := writer.Write(frame.GetData()); err != nil {
			return internal(err)
		}
	}
}

// Download implements the server-stream: one info frame, then data frames.
func (b *Blobs) Download(req *graphenepbv1.DownloadRequest, stream graphenepbv1.BlobService_DownloadServer) error {
	ctx := stream.Context()
	if err := auth.Check(ctx, auth.VerbGet, blobKind); err != nil {
		return denied(err)
	}

	reader, info, err := b.st.Open(ctx, req.GetRef().GetId(), req.GetOffset())
	if errors.Is(err, blob.ErrNotFound) {
		return status.Errorf(codes.NotFound, "blob %s not found", req.GetRef().GetId())
	}

	if err != nil {
		return internal(err)
	}

	defer func() { _ = reader.Close() }()

	if err := stream.Send(&graphenepbv1.DownloadResponse{
		Msg: &graphenepbv1.DownloadResponse_Info{Info: blobInfo(info)},
	}); err != nil {
		return fmt.Errorf("send download info: %w", err)
	}

	return pumpDownload(stream, reader, chunkSize(req.GetChunkSize()))
}

func pumpDownload(stream graphenepbv1.BlobService_DownloadServer, reader io.Reader, chunk int) error {
	buf := make([]byte, chunk)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			frame := &graphenepbv1.DownloadResponse{
				Msg: &graphenepbv1.DownloadResponse_Data{Data: buf[:n]},
			}
			if err := stream.Send(frame); err != nil {
				return fmt.Errorf("send download frame: %w", err)
			}
		}

		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return internal(err)
		}
	}
}

func chunkSize(hint uint32) int {
	switch {
	case hint == 0:
		return defaultChunk
	case hint > maxChunk:
		return maxChunk
	default:
		return int(hint)
	}
}

func blobInfo(info blob.Info) *graphenepbv1.BlobInfo {
	return &graphenepbv1.BlobInfo{
		Ref:  &graphenepbv1.BlobRef{Id: info.ID},
		Size: info.Size,
		Checksum: &graphenepbv1.Digest{
			Algo:  graphenepbv1.DigestAlgo_DIGEST_ALGO_SHA256,
			Value: info.SHA256,
		},
	}
}
