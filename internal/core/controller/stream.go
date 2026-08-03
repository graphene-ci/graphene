package controller

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/key"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/core/store"
)

// Local reads the store this process holds. Events arrive already decoded,
// so a controller never touches the byte layout.
func Local(st store.Store, kind string, path ...string) Stream {
	prefix := key.New(kind, path...).Encode()

	return func(ctx context.Context, from uint64) (<-chan Event, error) {
		raw, err := st.Watch(ctx, prefix, from)
		if errors.Is(err, store.ErrCompacted) {
			return nil, fmt.Errorf("%w: %w", ErrRestart, err)
		}

		if err != nil {
			return nil, fmt.Errorf("controller: watch %s: %w", kind, err)
		}

		out := make(chan Event)

		go func() {
			defer close(out)

			for event := range raw {
				decoded, err := decodeLocal(&event)
				if err != nil {
					// A record we cannot read is not a reason to stop
					// following the rest; it will be reported when the
					// loop asks the handler.
					continue
				}

				select {
				case out <- decoded:
				case <-ctx.Done():
					return
				}
			}
		}()

		return out, nil
	}
}

func decodeLocal(event *store.Event) (Event, error) {
	if event.Type == store.EventSync {
		return Event{Type: event.Type, StoreRevision: event.StoreRevision}, nil
	}

	res, err := service.DecodeEntry(event.Entry)
	if err != nil {
		return Event{}, fmt.Errorf("controller: decode: %w", err)
	}

	return Event{Type: event.Type, Resource: res, StoreRevision: event.StoreRevision}, nil
}

// Remote reads a kernel a link away, through the ordinary Watch rpc — the
// same one any client uses. A controller written against this is a client
// and nothing more, which is the whole point: the truth being elsewhere is
// a fact about deployment, not about the code.
func Remote(client graphenepbv1.ResourceServiceClient, kind string, path ...string) Stream {
	return func(ctx context.Context, from uint64) (<-chan Event, error) {
		stream, err := client.Watch(ctx, &graphenepbv1.WatchRequest{
			Kind:              kind,
			PathPrefix:        path,
			FromStoreRevision: from,
		})
		if err != nil {
			return nil, remoteErr(kind, err)
		}

		out := make(chan Event)

		go func() {
			defer close(out)

			for {
				msg, err := stream.Recv()
				if err != nil {
					return // the loop reopens; a closed stream is normal
				}

				select {
				case out <- fromProto(msg):
				case <-ctx.Done():
					return
				}
			}
		}()

		return out, nil
	}
}

// remoteErr maps the far side's answer into the loop's vocabulary: a
// revision it can no longer serve means the same thing a compacted store
// does — start over.
func remoteErr(kind string, err error) error {
	if status.Code(err) == codes.OutOfRange {
		return fmt.Errorf("%w: %w", ErrRestart, err)
	}

	return fmt.Errorf("controller: watch %s: %w", kind, err)
}

func fromProto(event *graphenepbv1.WatchEvent) Event {
	return Event{
		Type:          eventType(event.GetType()),
		Resource:      event.GetResource(),
		StoreRevision: event.GetStoreRevision(),
	}
}

func eventType(t graphenepbv1.EventType) store.EventType {
	switch t {
	case graphenepbv1.EventType_EVENT_TYPE_PUT:
		return store.EventPut
	case graphenepbv1.EventType_EVENT_TYPE_DELETE:
		return store.EventDelete
	case graphenepbv1.EventType_EVENT_TYPE_SYNC:
		return store.EventSync
	case graphenepbv1.EventType_EVENT_TYPE_UNSPECIFIED:
		return store.EventType(0)
	default:
		return store.EventType(0)
	}
}
