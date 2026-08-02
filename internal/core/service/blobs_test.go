package service_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/auth/static"
	blobfs "github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
	"github.com/graphene-ci/graphene/internal/infrastructure/server"
)

func newBlobEnv(t *testing.T) func(token string) graphenepbv1.BlobServiceClient {
	t.Helper()

	st, err := blobfs.Open(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatalf("open blob store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	source := static.New(
		static.Entry{Token: adminToken, Credentials: static.Admin("root")},
		static.Entry{Token: kernelToken, Credentials: static.Kernel("k1")},
	)

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(server.UnaryAuth(source)),
		grpc.StreamInterceptor(server.StreamAuth(source)),
	)
	graphenepbv1.RegisterBlobServiceServer(srv, service.NewBlobs(st))

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	return func(token string) graphenepbv1.BlobServiceClient {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(bearerUnary(token)),
			grpc.WithStreamInterceptor(bearerStream(token)),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		t.Cleanup(func() { _ = conn.Close() })

		return graphenepbv1.NewBlobServiceClient(conn)
	}
}

func upload(
	t *testing.T,
	c graphenepbv1.BlobServiceClient,
	content []byte,
	open *graphenepbv1.UploadOpen,
) (*graphenepbv1.BlobInfo, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	stream, err := c.Upload(ctx)
	if err != nil {
		t.Fatalf("upload open: %v", err)
	}

	if err := stream.Send(&graphenepbv1.UploadRequest{
		Msg: &graphenepbv1.UploadRequest_Open{Open: open},
	}); err != nil {
		t.Fatalf("send open: %v", err)
	}

	const frame = 64 * 1024
	for start := 0; start < len(content); start += frame {
		end := min(start+frame, len(content))
		if err := stream.Send(&graphenepbv1.UploadRequest{
			Msg: &graphenepbv1.UploadRequest_Data{Data: content[start:end]},
		}); err != nil {
			t.Fatalf("send data: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return resp.GetInfo(), nil
}

func download(t *testing.T, c graphenepbv1.BlobServiceClient, id string, offset uint64) ([]byte, *graphenepbv1.BlobInfo) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	stream, err := c.Download(ctx, &graphenepbv1.DownloadRequest{
		Ref:    &graphenepbv1.BlobRef{Id: id},
		Offset: offset,
	})
	if err != nil {
		t.Fatalf("download: %v", err)
	}

	var (
		info *graphenepbv1.BlobInfo
		out  bytes.Buffer
	)

	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out.Bytes(), info
		}

		if err != nil {
			t.Fatalf("recv: %v", err)
		}

		if i := frame.GetInfo(); i != nil {
			info = i

			continue
		}

		out.Write(frame.GetData())
	}
}

func TestBlobRoundtrip(t *testing.T) {
	t.Parallel()

	env := newBlobEnv(t)
	client := env(kernelToken) // the kernel role carries the byte-plane grants

	content := make([]byte, 3*1024*1024+17)
	if _, err := rand.Read(content); err != nil {
		t.Fatal(err)
	}

	sum := sha256.Sum256(content)

	info, err := upload(t, client, content, &graphenepbv1.UploadOpen{
		ExpectedChecksum: &graphenepbv1.Digest{
			Algo:  graphenepbv1.DigestAlgo_DIGEST_ALGO_SHA256,
			Value: sum[:],
		},
		ExpectedSize: uint64(len(content)),
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	if info.GetSize() != uint64(len(content)) || !bytes.Equal(info.GetChecksum().GetValue(), sum[:]) {
		t.Fatalf("info mismatch: size=%d", info.GetSize())
	}

	got, gotInfo := download(t, client, info.GetRef().GetId(), 0)
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %d bytes", len(got))
	}

	if gotInfo.GetSize() != uint64(len(content)) {
		t.Fatalf("download info size: %d", gotInfo.GetSize())
	}

	// Resume from an offset: the tail must match.
	offset := uint64(len(content) - 1000)

	tail, _ := download(t, client, info.GetRef().GetId(), offset)
	if !bytes.Equal(tail, content[offset:]) {
		t.Fatalf("offset tail mismatch: got %d bytes", len(tail))
	}

	// Stat sees it.
	stat, err := client.Stat(context.Background(), &graphenepbv1.StatRequest{Ref: info.GetRef()})
	if err != nil || !stat.GetExists() {
		t.Fatalf("stat: exists=%v err=%v", stat.GetExists(), err)
	}
}

func TestBlobChecksumMismatch(t *testing.T) {
	t.Parallel()

	env := newBlobEnv(t)
	client := env(adminToken)

	wrong := sha256.Sum256([]byte("something else"))

	_, err := upload(t, client, []byte("payload"), &graphenepbv1.UploadOpen{
		ExpectedChecksum: &graphenepbv1.Digest{
			Algo:  graphenepbv1.DigestAlgo_DIGEST_ALGO_SHA256,
			Value: wrong[:],
		},
	})
	if status.Code(err) != codes.DataLoss {
		t.Fatalf("want DataLoss, got %v", err)
	}
}

func TestBlobUnknownRef(t *testing.T) {
	t.Parallel()

	env := newBlobEnv(t)
	client := env(adminToken)

	stat, err := client.Stat(context.Background(), &graphenepbv1.StatRequest{
		Ref: &graphenepbv1.BlobRef{Id: "deadbeef"},
	})
	if err != nil || stat.GetExists() {
		t.Fatalf("stat unknown: exists=%v err=%v", stat.GetExists(), err)
	}
}
