package upstream

import (
	"context"
	"errors"
	"io"

	blobpb "github.com/graphene-ci/graphenepb/v1/blob"
)

// Blobs forwards the byte service, the same way Service forwards the
// kernel's.
//
// Written out rather than generated, for the reason the other one is: a
// reflective proxy would be shorter and would not fail to build the day
// the service grows a method.
// Nothing here wraps an error either, for the reason nothing there does:
// what the caller is told is what the kernel above said, in its words.
// Written down in .golangci.yaml beside the same decision for Service.
type Blobs struct {
	blobpb.UnimplementedBlobServiceServer

	up *Upstream
	// acting names the process this speaks for, or is empty when it
	// speaks for whoever is connected.
	acting string
}

// Forwarding is what a subordinate answers byte calls with.
func (u *Upstream) Forwarding() *Blobs { return &Blobs{up: u} }

// BytesFor is what one of this kernel's processes talks to.
func (u *Upstream) BlobsFor(named string) *Blobs {
	return &Blobs{up: u, acting: named}
}

// out is how a call leaves: as the caller when there is one, and as this
// kernel acting for a process when there is not.
func (b *Blobs) out(ctx context.Context) context.Context {
	if b.acting == "" {
		return forwarded(ctx)
	}

	return b.up.forProcess(ctx, b.acting)
}

func (b *Blobs) Stat(
	ctx context.Context, asked *blobpb.StatRequest,
) (*blobpb.StatResponse, error) {
	return b.client().Stat(b.out(ctx), asked)
}

func (b *Blobs) Delete(
	ctx context.Context, asked *blobpb.DeleteRequest,
) (*blobpb.DeleteResponse, error) {
	return b.client().Delete(b.out(ctx), asked)
}

// Upload pours the frames upward and hands back what the far side named.
func (b *Blobs) Upload(from blobpb.BlobService_UploadServer) error {
	to, err := b.client().Upload(b.out(from.Context()))
	if err != nil {
		return err
	}

	for {
		frame, err := from.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		if err := to.Send(frame); err != nil {
			return err
		}
	}

	answer, err := to.CloseAndRecv()
	if err != nil {
		return err
	}

	return from.SendAndClose(answer)
}

// Download pours the frames downward.
func (b *Blobs) Download(
	asked *blobpb.DownloadRequest, to blobpb.BlobService_DownloadServer,
) error {
	from, err := b.client().Download(b.out(to.Context()), asked)
	if err != nil {
		return err
	}

	for {
		frame, err := from.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		if err := to.Send(frame); err != nil {
			return err
		}
	}
}

// client is the byte service above, unsigned: this forwards its own
// credential per call rather than the connection's, the same as
// everything else here.
func (b *Blobs) client() blobpb.BlobServiceClient {
	return blobpb.NewBlobServiceClient(b.up.conn)
}
