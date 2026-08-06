package upstream

import (
	"context"

	"google.golang.org/grpc"

	blobpb "github.com/graphene-ci/graphenepb/v1/blob"
)

// Bytes is the byte store above, reached as THIS kernel.
//
// A kernel fetching what it was told to run is acting on its own account,
// the same as when it writes down that it exists — so the credential is
// its own and the grant is one somebody gave it by name.
//
// The credential is put on here rather than on the connection, and that
// difference is the point: the same connection also forwards other
// people's calls, and a token attached at dial time would land on those
// too. A call that arrived with nobody on it would leave as this kernel,
// which would turn every anonymous caller into a privileged one.
type Bytes struct {
	blobpb.BlobServiceClient

	up *Upstream
}

// Fetching is how a subordinate gets the bytes it must run.
func (u *Upstream) Fetching() *Bytes {
	return &Bytes{BlobServiceClient: blobpb.NewBlobServiceClient(u.conn), up: u}
}

func (b *Bytes) Stat(
	ctx context.Context, in *blobpb.StatRequest, opts ...grpc.CallOption,
) (*blobpb.StatResponse, error) {
	//nolint:wrapcheck // the far side's error, given to a caller that expects it
	return b.BlobServiceClient.Stat(b.up.own(ctx), in, opts...)
}

func (b *Bytes) Upload(
	ctx context.Context, opts ...grpc.CallOption,
) (grpc.ClientStreamingClient[blobpb.UploadRequest, blobpb.UploadResponse], error) {
	//nolint:wrapcheck // see Stat
	return b.BlobServiceClient.Upload(b.up.own(ctx), opts...)
}

func (b *Bytes) Download(
	ctx context.Context, in *blobpb.DownloadRequest, opts ...grpc.CallOption,
) (grpc.ServerStreamingClient[blobpb.DownloadResponse], error) {
	//nolint:wrapcheck // see Stat
	return b.BlobServiceClient.Download(b.up.own(ctx), in, opts...)
}

func (b *Bytes) Delete(
	ctx context.Context, in *blobpb.DeleteRequest, opts ...grpc.CallOption,
) (*blobpb.DeleteResponse, error) {
	//nolint:wrapcheck // see Stat
	return b.BlobServiceClient.Delete(b.up.own(ctx), in, opts...)
}
