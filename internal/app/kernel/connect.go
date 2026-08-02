package kernel

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
	"github.com/graphene-ci/graphene/internal/infrastructure/link"
	tlsutil "github.com/graphene-ci/graphene/internal/infrastructure/tls"
)

// connect establishes the link to another kernel and starts what rides on
// it. Today that is the lease renewal — the heartbeat by which the far
// side knows this kernel is alive (execution work joins later, over the
// same connection).
func (k *Kernel) connect(ctx context.Context, group *errgroup.Group) error {
	transport, err := k.buildLink()
	if err != nil {
		return err
	}

	token, err := k.cfg.Link.Token.Resolve()
	if err != nil {
		return fmt.Errorf("kernel: link token: %w", err)
	}

	tlsConfig, err := k.linkTLS()
	if err != nil {
		return err
	}

	conn, err := link.Connect(k.cfg.Link.Address, transport, token, tlsConfig)
	if err != nil {
		return fmt.Errorf("kernel: connect: %w", err)
	}

	k.closers = append(k.closers, conn.Close)
	k.log.Info("linked", "mode", k.cfg.Link.Mode, "address", k.cfg.Link.Address)

	if k.cfg.Lease != nil {
		client := graphenepbv1.NewResourceServiceClient(conn)

		group.Go(func() error {
			k.renewLease(ctx, client)

			return nil
		})
	}

	return nil
}

func (k *Kernel) buildLink() (corelink.Link, error) {
	cfg := k.cfg.Link

	switch cfg.Mode {
	case "stdio":
		return link.Stdio(), nil

	case "via":
		relayToken, err := cfg.Via.Token.Resolve()
		if err != nil {
			return nil, fmt.Errorf("kernel: relay token: %w", err)
		}

		return link.Via(cfg.Via.Address, relayToken), nil

	default:
		return link.TCP(cfg.Address), nil
	}
}

// linkTLS builds the client TLS configuration. A stdio link carries no
// TLS of its own: the ssh session it rides in already provides the
// channel — the bearer token is still required, as on every transport.
func (k *Kernel) linkTLS() (*tls.Config, error) {
	cfg := k.cfg.Link
	if cfg.Mode == "stdio" {
		return nil, nil //nolint:nilnil // the ssh session is the channel
	}

	tlsConfig, err := tlsutil.ClientConfig(cfg.CAFile, tlsutil.ServerNameFor(cfg.Address))
	if err != nil {
		return nil, fmt.Errorf("kernel: link: %w", err)
	}

	return tlsConfig, nil
}

// renewLease writes this kernel's KernelLease at the configured interval.
// A renewal is a revision bump and nothing more: the far side times
// expiry with its own clock, so a wrong clock here cannot extend a lease.
func (k *Kernel) renewLease(ctx context.Context, client graphenepbv1.ResourceServiceClient) {
	ticker := time.NewTicker(k.cfg.Lease.RenewInterval)
	defer ticker.Stop()

	for {
		if err := k.renewOnce(ctx, client); err != nil && ctx.Err() == nil {
			k.log.Warn("lease renewal failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (k *Kernel) renewOnce(ctx context.Context, client graphenepbv1.ResourceServiceClient) error {
	key := &graphenepbv1.Key{
		Kind: leaseKind,
		Path: []string{k.cfg.Identity.Name},
	}

	var expected uint64

	got, err := client.Get(ctx, &graphenepbv1.GetRequest{Key: key})

	switch {
	case err == nil:
		expected = got.GetResource().GetRevision()
	case status.Code(err) != codes.NotFound:
		return fmt.Errorf("read lease: %w", err)
	}

	_, err = client.Put(ctx, &graphenepbv1.PutRequest{
		Resource: &graphenepbv1.Resource{
			Key: key,
			Spec: schemapb.MustStructFromGo(map[string]any{
				"kernel":      k.cfg.Identity.Name,
				"ttl_seconds": int64(k.cfg.Lease.TTL / time.Second),
			}),
		},
		ExpectedRevision: expected,
	})
	if err != nil {
		return fmt.Errorf("write lease: %w", err)
	}

	return nil
}

// leaseKind is the kind renewLease writes; kept here to avoid importing the
// builtin package into the link path.
const leaseKind = "KernelLease"
