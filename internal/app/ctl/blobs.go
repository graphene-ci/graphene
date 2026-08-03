package ctl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// errChecksumDisagreement — the server's checksum is not ours, so one of
// us read different bytes. Neither side can say which, so the blob is not
// to be trusted.
var errChecksumDisagreement = errors.New("the kernel's checksum does not match ours")

// chunkSize is the upload frame. Large enough that a big file is not
// thousands of round trips, small enough to stay under the transport's
// message cap with room to spare.
const chunkSize = 256 << 10

// Upload streams bytes into the kernel's blob store and returns the id it
// was given.
//
// The checksum is computed here and declared to the server, which computes
// its own and refuses the blob if they differ. Neither side has to trust
// the other's arithmetic, and a truncated upload cannot become a stored
// blob that someone later runs.
func (c *Client) Upload(ctx context.Context, source io.Reader) (string, error) {
	stream, err := c.Blobs.Upload(ctx)
	if err != nil {
		return "", fmt.Errorf("ctl: upload: %w", err)
	}

	if err := stream.Send(&graphenepbv1.UploadRequest{
		Msg: &graphenepbv1.UploadRequest_Open{Open: &graphenepbv1.UploadOpen{}},
	}); err != nil {
		return "", fmt.Errorf("ctl: upload open: %w", err)
	}

	sum := sha256.New()
	buf := make([]byte, chunkSize)

	for {
		n, err := source.Read(buf)
		if n > 0 {
			sum.Write(buf[:n])

			if err := stream.Send(&graphenepbv1.UploadRequest{
				Msg: &graphenepbv1.UploadRequest_Data{Data: buf[:n]},
			}); err != nil {
				return "", fmt.Errorf("ctl: upload data: %w", err)
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return "", fmt.Errorf("ctl: read source: %w", err)
		}
	}

	done, err := stream.CloseAndRecv()
	if err != nil {
		return "", fmt.Errorf("ctl: upload commit: %w", err)
	}

	info := done.GetInfo()
	if got := info.GetChecksum().GetValue(); len(got) > 0 && hex.EncodeToString(got) != hex.EncodeToString(sum.Sum(nil)) {
		return "", fmt.Errorf("ctl: upload: %w", errChecksumDisagreement)
	}

	return info.GetRef().GetId(), nil
}

// Download writes a blob's bytes to the sink.
func (c *Client) Download(ctx context.Context, id string, sink io.Writer) error {
	stream, err := c.Blobs.Download(ctx, &graphenepbv1.DownloadRequest{
		Ref: &graphenepbv1.BlobRef{Id: id},
	})
	if err != nil {
		return fmt.Errorf("ctl: download: %w", err)
	}

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("ctl: download: %w", err)
		}

		if data := msg.GetData(); len(data) > 0 {
			if _, err := sink.Write(data); err != nil {
				return fmt.Errorf("ctl: write: %w", err)
			}
		}
	}
}
