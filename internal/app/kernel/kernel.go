// Package kernel is the composition root: it turns a configuration into a
// running kernel. Nothing above it (the command layer) knows about stores,
// registries or grpc; nothing below it knows about configuration files.
//
// A kernel has no role. Whatever is configured, runs:
//
//	store   → truth is held here (registry, builtin definitions, controllers)
//	blobs   → content bytes are held here
//	listen  → the API is served (tcp with tls, unix socket)
//	link    → a connection to another kernel is established
//	lease   → liveness is renewed over that link
package kernel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/blob"
	"github.com/graphene-ci/graphene/internal/core/builtin"
	"github.com/graphene-ci/graphene/internal/core/controller"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
	authres "github.com/graphene-ci/graphene/internal/infrastructure/auth/resource"
	blobfs "github.com/graphene-ci/graphene/internal/infrastructure/blob/fs"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

const leaseSweepInterval = 5 * time.Second

// Kernel is an assembled, not yet running, kernel.
type Kernel struct {
	cfg *config.Config
	log *slog.Logger

	store     store.Store
	blobs     blob.Store
	registry  *registry.Registry
	resources *service.Resources
	blobSvc   *service.Blobs
	tokens    *authres.Source
	lease     *controller.Lease

	closers []func() error
}

// New assembles a kernel from configuration: opens backends, ensures the
// builtin definitions, and wires the services and controllers the
// configuration calls for. It does not listen or connect — that is Run.
func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Kernel, error) {
	k := &Kernel{cfg: cfg, log: log}

	if err := k.openBackends(); err != nil {
		_ = k.Close()

		return nil, err
	}

	if k.store != nil {
		if err := k.buildTruth(ctx); err != nil {
			_ = k.Close()

			return nil, err
		}
	}

	return k, nil
}

func (k *Kernel) openBackends() error {
	if k.cfg.Store != nil {
		st, err := bbolt.Open(k.cfg.Store.Path)
		if err != nil {
			return fmt.Errorf("kernel: open store: %w", err)
		}

		k.store = st
		k.closers = append(k.closers, st.Close)
		k.log.Info("store opened", "path", k.cfg.Store.Path)
	}

	if k.cfg.Blobs != nil {
		blobs, err := blobfs.Open(k.cfg.Blobs.Path)
		if err != nil {
			return fmt.Errorf("kernel: open blobs: %w", err)
		}

		k.blobs = blobs
		k.closers = append(k.closers, blobs.Close)
		k.log.Info("blob store opened", "path", k.cfg.Blobs.Path)
	}

	return nil
}

// buildTruth wires everything that only exists when this kernel holds the
// store: the registry with its builtin definitions, the resource and blob
// services, the token source and the controllers.
func (k *Kernel) buildTruth(ctx context.Context) error {
	k.registry = registry.New(k.store)

	if err := builtin.Ensure(controller.SystemContext(ctx), k.registry); err != nil {
		return fmt.Errorf("kernel: ensure builtin definitions: %w", err)
	}

	k.resources = service.NewResources(k.store, k.registry)
	if k.blobs != nil {
		k.blobSvc = service.NewBlobs(k.blobs)
	}

	bootstrapToken, bootstrapCreds, err := k.bootstrapCredentials()
	if err != nil {
		return err
	}

	k.tokens = authres.New(k.store, bootstrapToken, bootstrapCreds)
	k.lease = controller.NewLease(k.resources, k.store, time.Now)

	return nil
}

// bootstrapCredentials resolves the one credential that exists before any
// Identity resource does. Without it a fresh store could never be
// administered; with it, the operator creates the first Role and Identity
// and then stops using it.
func (k *Kernel) bootstrapCredentials() (string, auth.Credentials, error) {
	if k.cfg.Auth == nil {
		return "", auth.Credentials{}, nil
	}

	token, err := k.cfg.Auth.Bootstrap.Token.Resolve()
	if err != nil {
		return "", auth.Credentials{}, fmt.Errorf("kernel: bootstrap token: %w", err)
	}

	creds := auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalUser, Name: k.cfg.Auth.Bootstrap.Name},
		Grants: []auth.Grant{{
			Verbs: []auth.Verb{
				auth.VerbGet, auth.VerbList, auth.VerbWatch,
				auth.VerbPut, auth.VerbDelete, auth.VerbDefine,
			},
			Kind: "*",
		}},
	}

	return token, creds, nil
}

// Run starts everything configured and blocks until ctx is cancelled or a
// component fails.
func (k *Kernel) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)

	if k.tokens != nil {
		group.Go(func() error { return k.tokens.Run(ctx) })

		if err := k.tokens.WaitWarm(ctx); err != nil {
			return fmt.Errorf("kernel: token source warmup: %w", err)
		}
	}

	if k.lease != nil {
		group.Go(func() error { return k.lease.Run(ctx) })
		group.Go(func() error {
			k.lease.RunSweeper(ctx, leaseSweepInterval)

			return nil
		})
	}

	if k.cfg.Listen != nil {
		if err := k.serve(ctx, group); err != nil {
			return err
		}
	}

	if k.cfg.Link != nil {
		if err := k.connect(ctx, group); err != nil {
			return err
		}
	}

	if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("kernel: %w", err)
	}

	return nil
}

// Close releases the backends in reverse order of opening.
func (k *Kernel) Close() error {
	var errs []error

	for i := len(k.closers) - 1; i >= 0; i-- {
		if err := k.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}

	k.closers = nil

	return errors.Join(errs...)
}

// CACertPath reports where the served endpoint's CA lives (empty when the
// kernel serves no TLS endpoint).
func (k *Kernel) CACertPath() string {
	if k.cfg.TLS == nil {
		return ""
	}

	return k.cfg.TLS.Dir
}

// NewLogger builds the configured logger.
func NewLogger(cfg config.Log) *slog.Logger {
	level := slog.LevelInfo

	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}

	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}
