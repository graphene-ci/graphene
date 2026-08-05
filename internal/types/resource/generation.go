package resource

import "strconv"

// Generation counts INTENT, not writes.
//
// A controller lives in a loop: it wakes on a change, reconciles, and
// writes status. Writing status changes the resource, which wakes it
// again — and without a way to ask "did the intent change, or was that my
// own echo", that loop never stops.
//
// The revision cannot answer it: the revision moves on every write,
// including the controller's own. This does not. It moves when the spec
// moves and at no other time, so a controller that records the generation
// it acted on can compare and go back to sleep:
//
//	generation == observed  →  the spec has not moved; nothing to do
//	generation >  observed  →  the spec is newer than my report; work
//
// A number rather than a hash of the spec, which would answer the same
// question, because a number is ORDERED. "Different" is enough to decide
// whether to act; "newer" is what tells a restarted controller whose
// report is stale when there is more than one writing.
type Generation uint64

// NoGeneration is what a resource has before it was ever admitted. The
// first admission is 1, so a stored resource never carries it.
const NoGeneration Generation = 0

// Next is the generation after this one — what an admission assigns when
// the spec has changed.
func (g Generation) Next() Generation { return g + 1 }

func (g Generation) Eq(other Generation) bool { return g == other }

// After reports an intent newer than the one the other counts. This is
// the question a controller asks of its own observed generation.
func (g Generation) After(other Generation) bool { return g > other }

// IsZero reports a resource that was never admitted.
func (g Generation) IsZero() bool { return g == NoGeneration }

func (g Generation) String() string { return strconv.FormatUint(uint64(g), 10) }
