// Package controller is the runtime for the control kernel's built-in
// reconciliation loops. Controllers are ordinary API consumers
// (dogfooding): they read raw store events and write through the resource
// service under a system principal — no private paths into the store's
// semantics.
package controller

import (
	"context"
	"fmt"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

// SystemContext returns ctx carrying the system principal every built-in
// controller acts as.
func SystemContext(ctx context.Context) context.Context {
	return auth.WithCredentials(ctx, auth.FullAccess(auth.PrincipalSystem, "controller"))
}

// Handler consumes one event of the watched kind. For deletes the
// resource carries the record's final state (prev_kv semantics).
type Handler func(ctx context.Context, typ store.EventType, res *graphenepbv1.Resource) error

// Loop watches one kind, handing decoded resources to a handler. The
// cursor and resume rules live in store.WatchLoop — this only decodes.
type Loop struct {
	Store store.Store
	Kind  string
	// Handle is called sequentially, in store-revision order.
	Handle Handler
	// OnError observes decode and handler failures; optional.
	OnError func(err error)
}

// Run blocks until ctx is done.
func (l *Loop) Run(ctx context.Context) error {
	loop := &store.WatchLoop{
		Store:   l.Store,
		Prefix:  key.New(l.Kind).Encode(),
		OnError: l.OnError,
		Handle: func(ctx context.Context, event store.Event) error {
			res, err := service.DecodeEntry(event.Entry)
			if err != nil {
				return fmt.Errorf("controller: decode %s: %w", l.Kind, err)
			}

			return l.Handle(ctx, event.Type, res)
		},
	}

	if err := loop.Run(ctx); err != nil {
		return fmt.Errorf("controller: %w", err)
	}

	return nil
}
