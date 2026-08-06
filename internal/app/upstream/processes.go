package upstream

import (
	"context"
	"errors"
	"io"
	"iter"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/process"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Processes is what an agent on a machine that keeps nothing watches.
//
// The same two calls a local agent makes — walk what is there, follow
// what happens after — asked of the kernel above with THIS kernel's
// credential. Not the caller's: nobody is calling. A kernel watching for
// work to run is doing it on its own account, and the grant that lets it
// is one somebody gave to that kernel by name.
//
// It reads and reports and cannot Put, which is not an oversight but the
// same shape the local agent has: what to run is placed on a kernel by
// whoever placed it, and a kernel that could write its own work would be
// answering a question nobody asked.
type Processes struct {
	*Recorder
}

// Watching is the agent's end of a link.
func (u *Upstream) Watching() *Processes { return &Processes{Recorder: u.Recording()} }

// List walks what is on the kernel above, under a prefix.
func (p *Processes) List(
	ctx context.Context, prefix resource.Id,
) iter.Seq2[store.Value[resource.Resource], error] {
	return func(yield func(store.Value[resource.Resource], error) bool) {
		stream, err := p.up.client.List(p.up.own(ctx), &graphenepbv1.ListRequest{
			Prefix: convert.IdToPb(prefix),
		})
		if err != nil {
			yield(store.Value[resource.Resource]{}, upstreamError(err))

			return
		}

		for {
			answer, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}

			if err != nil {
				yield(store.Value[resource.Resource]{}, upstreamError(err))

				return
			}

			stored, err := recordFromPb(answer.GetRecord())
			if !yield(stored, err) || err != nil {
				return
			}
		}
	}
}

// Watch follows what happens after a revision.
//
// The stream is pulled rather than delivered: Next blocks until the
// kernel above says something, and the loop that calls it is the agent's
// own. Nothing here starts a goroutine, so there is nothing here to leak
// when the agent stops.
func (p *Processes) Watch(
	ctx context.Context, prefix resource.Id, after revision.Revision,
) (process.Stream, error) {
	stream, err := p.up.client.Watch(p.up.own(ctx), &graphenepbv1.WatchRequest{
		Prefix: convert.IdToPb(prefix),
		After:  after.Uint64(),
	})
	if err != nil {
		return nil, upstreamError(err)
	}

	return &Events{stream: stream}, nil
}

// Events is a watch in progress on the kernel above. It is what an agent
// calls a Stream, which is the one line of adaptation a link needs.
type Events struct {
	stream graphenepbv1.KernelService_WatchClient
}

// Next hands back the next change, blocking until there is one.
//
// The context is the one the stream was opened with; a caller passing a
// different one is asking a question this cannot answer, so it is not
// read. Canceling the watch means canceling what opened it.
func (e *Events) Next(_ context.Context) (store.Event[resource.Resource], error) {
	answer, err := e.stream.Recv()
	if err != nil {
		return store.Event[resource.Resource]{}, upstreamError(err)
	}

	return eventFromPb(answer.GetEvent())
}

// Close stops the watch.
func (e *Events) Close() error {
	//nolint:wrapcheck // the transport's own error; nothing here to add
	return e.stream.CloseSend()
}

func eventFromPb(event *graphenepbv1.Event) (store.Event[resource.Resource], error) {
	stored, err := recordFromPb(event.GetRecord())
	if err != nil {
		return store.Event[resource.Resource]{}, err
	}

	return store.Event[resource.Resource]{
		Kind:  eventKindFromPb(event.GetKind()),
		Value: stored,
	}, nil
}

// eventKindFromPb reads what happened.
//
// Anything unrecognized is a change: a kernel above that learned a new
// kind of event should make an agent look again, not make it decide the
// record went away.
func eventKindFromPb(kind graphenepbv1.EventKind) store.EventKind {
	if kind == graphenepbv1.EventKind_EVENT_KIND_DELETE {
		return store.EventDelete
	}

	return store.EventPut
}
