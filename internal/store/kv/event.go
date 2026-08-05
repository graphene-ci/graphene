package kv

// EventKind is what happened to a key.
type EventKind uint8

// The two things that can happen.
const (
	// EventPut — the key was written, whether it existed before or not.
	EventPut EventKind = iota + 1
	// EventDelete — the key was removed. The event still carries the last
	// value it had, because whoever filters the stream has to be able to
	// ask what it was that went away.
	EventDelete
)

// IsZero reports an event kind nobody set. A store never delivers one;
// it is what a zero Event carries, which is how a caller tells a real
// event from a variable it forgot to fill.
func (k EventKind) IsZero() bool { return k == 0 }

// String names what happened, for a log or an error.
func (k EventKind) String() string {
	switch k {
	case EventPut:
		return "put"
	case EventDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Event is one change under a watched prefix.
type Event struct {
	Kind  EventKind
	Entry Entry
}

// String names the event and the record it happened to.
func (e Event) String() string { return e.Kind.String() + " " + e.Entry.String() }
