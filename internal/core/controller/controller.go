// Package controller is the runtime for the control kernel's built-in
// reconciliation loops. Controllers are ordinary API consumers
// (dogfooding): they read raw store events and write through the resource
// service under a system principal — no private paths into the store's
// semantics.
package controller

import (
	"context"
	"errors"
	"time"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

const retryBackoff = time.Second

// SystemContext returns ctx carrying the system principal every built-in
// controller acts as.
func SystemContext(ctx context.Context) context.Context {
	return auth.WithCredentials(ctx, auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalSystem, Name: "controller"},
		Grants: []auth.Grant{{
			Verbs: []auth.Verb{auth.VerbGet, auth.VerbList, auth.VerbWatch, auth.VerbPut, auth.VerbDelete},
			Kind:  "*",
		}},
	})
}

// Handler consumes one event of the watched kind. For deletes the
// resource carries the record's final state (prev_kv semantics).
type Handler func(ctx context.Context, typ store.EventType, res *graphenepbv1.Resource) error

// Loop watches one kind and hands every event to the handler, resuming
// from the last SYNC revision whenever the watch channel closes (store
// resets, slow-consumer eviction).
//
// Resuming from a delivered event's revision would be wrong: catch-up
// events arrive in key order, so their revisions are not monotonic and an
// interrupted catch-up would silently drop the entries whose revision was
// lower than the last delivered one. Until the sync marker arrives the
// cursor stays where it was — an interrupted catch-up is redone in full.
//
// Handler errors are terminal: controllers own their retries per object.
type Loop struct {
	Store store.Store
	Kind  string
	// Handle is called sequentially, in store-revision order.
	Handle Handler
}

// Run blocks until ctx is done.
func (l *Loop) Run(ctx context.Context) error {
	var cursor uint64

	for {
		if err := l.watchOnce(ctx, &cursor); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryBackoff):
		}
	}
}

func (l *Loop) watchOnce(ctx context.Context, cursor *uint64) error {
	events, err := l.Store.Watch(ctx, store.EncodePrefix(l.Kind), *cursor)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return nil //nolint:nilerr // shutdown is a clean exit
		}

		return err //nolint:wrapcheck // the store error is the whole story
	}

	for event := range events {
		if event.Type == store.EventSync {
			*cursor = event.StoreRevision

			continue
		}

		res, decodeErr := service.DecodeEntry(event.Entry)
		if decodeErr != nil {
			// A corrupt record must not kill the control loop; skip it.
			continue
		}

		if err := l.Handle(ctx, event.Type, res); err != nil {
			return err
		}

		if event.StoreRevision > *cursor {
			*cursor = event.StoreRevision
		}
	}

	// The channel closed: store shutdown, slow-consumer eviction or ctx
	// cancellation — the caller decides whether to re-watch.
	return nil
}
