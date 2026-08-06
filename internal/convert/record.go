package convert

import (
	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/def"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// RecordToPb writes a stored resource down: the value, and the two
// revisions the store keeps ABOUT it.
//
// The stamps travel beside the value on the wire the same way they sit
// beside it in the store, and for the same reason: a value that could
// carry a revision could carry a stale one.
func RecordToPb(stored store.Value[resource.Resource]) *graphenepbv1.Record {
	return &graphenepbv1.Record{
		Resource:        ResourceToPb(stored.Value),
		Revision:        stored.Revision.Uint64(),
		CreatedRevision: stored.CreatedRevision.Uint64(),
	}
}

// EventToPb writes one change down.
func EventToPb(event store.Event[resource.Resource]) *graphenepbv1.Event {
	return &graphenepbv1.Event{
		Kind:   eventKindToPb(event.Kind),
		Record: RecordToPb(event.Value),
	}
}

// KindEventToPb writes down one change to what is current for a kind.
func KindEventToPb(event store.Event[def.Head]) *graphenepbv1.KindEvent {
	return &graphenepbv1.KindEvent{
		Kind:       eventKindToPb(event.Kind),
		Definition: DefinitionToPb(event.Value.Value.Published),
	}
}

// eventKindToPb maps what happened.
//
// Written out rather than cast: proto reserves zero for "unset" and the
// Go side does not have to, so a cast would keep working while meaning
// something else.
func eventKindToPb(kind store.EventKind) graphenepbv1.EventKind {
	switch kind {
	case store.EventPut:
		return graphenepbv1.EventKind_EVENT_KIND_PUT
	case store.EventDelete:
		return graphenepbv1.EventKind_EVENT_KIND_DELETE
	default:
		return graphenepbv1.EventKind_EVENT_KIND_UNSPECIFIED
	}
}
