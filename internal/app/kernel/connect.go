package kernel

import (
	"context"
	"crypto/tls"
	"fmt"

	"golang.org/x/sync/errgroup"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/controller"
	corelink "github.com/graphene-ci/graphene/internal/core/link"
	"github.com/graphene-ci/graphene/internal/infrastructure/link"
	tlsutil "github.com/graphene-ci/graphene/internal/infrastructure/tls"
)

// connect establishes the link to another kernel and starts what rides on
// it: this kernel's presence, and the agent that runs whatever is placed
// on it. Both speak the ordinary API over the same connection.
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

	announce := k.presence(controller.OverClient(graphenepbv1.NewResourceServiceClient(conn)))

	group.Go(func() error { return announce.Run(ctx) })

	k.runLinkedAgent(ctx, group, conn)

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
