package report

import (
	"time"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/types/resource"
)

// Liveness is one kernel saying it is still there, and anybody working
// out whether that is still true.
//
// TWO HALVES, and which half goes where is the whole design. Saying so is
// the kernel's, because only the kernel on that machine knows: it writes
// a time into its own record, on the record only it may write, at a
// period it declares. JUDGING is the reader's, because a judgement is a
// comparison anybody can make and a STORED one goes stale in exactly the
// case that matters — the judge dying is indistinguishable from every
// kernel dying at once.
//
// So there is no lease resource, no sweeper, and nothing in the kernel
// that decides other machines are gone. A controller that wants an EVENT
// when a kernel dies is a controller, and a controller is a client; what
// it needs from here is this function.
//
// The clocks are not synchronized and this does not pretend they are. The
// time is the reporting kernel's and the comparison is the reader's, so
// the two must agree to within the grace below — which is why the grace
// is minutes rather than the seconds the beat runs at. A fleet whose
// clocks are further apart than that has a problem this is not the place
// to solve.
const (
	// Beat is how often a kernel says it is there. Often enough that a
	// machine going away is noticed while somebody still cares, rarely
	// enough that a thousand kernels are not a write per millisecond.
	Beat = 15 * time.Second
	// missesAllowed is how many beats may be lost before a kernel is
	// called gone. Three, because one lost beat is a slow disk and two is
	// a bad minute, and calling a machine dead is a thing people act on.
	missesAllowed = 3
	// Grace is the whole of it: silence longer than this is gone.
	Grace = Beat * missesAllowed
)

// State is what a reader concludes.
type State string

const (
	// Up — it said so recently enough.
	Up State = "up"
	// Gone — it said so, and then stopped.
	Gone State = "gone"
	// Silent — it has never said. An old record, or a kernel that wrote
	// itself down and then failed before its first beat.
	Silent State = "silent"
)

// Alive is what a record says about the kernel it describes.
//
// The time is passed in rather than read here, because a function that
// reads the clock cannot be tested for the one thing worth testing: what
// it says at the boundary.
func Alive(value resource.Resource, now time.Time) (State, time.Time) {
	raw, found := schemapb.As[string](value.Status().GetFields()[heartbeatField])
	if !found || raw == "" {
		return Silent, time.Time{}
	}

	beat, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		// A record that says something unreadable says nothing. Reporting
		// it as gone would be a guess about a kernel that may be fine;
		// reporting it as up would be a guess in the direction that gets
		// somebody paged at the wrong time.
		return Silent, time.Time{}
	}

	if now.Sub(beat) > graceOf(value) {
		return Gone, beat
	}

	return Up, beat
}

// graceOf is how long this kernel's silence is allowed to last.
//
// Read from the record rather than assumed, because the kernel that
// writes it is the one that knows how often it means to write — and a
// reader from a different build assuming its own number would call a
// slower kernel dead the day the beat changed.
func graceOf(value resource.Resource) time.Duration {
	seconds, found := schemapb.As[uint64](value.Status().GetFields()[beatField])
	if !found || seconds == 0 {
		return Grace
	}

	// A beat longer than this is not a slow kernel, it is a number that
	// went wrong — and a duration built from it would overflow into the
	// past, which reads as "gone forever ago" for a kernel that is fine.
	if seconds > maxBeatSeconds {
		return Grace
	}

	return time.Duration(seconds) * time.Second * missesAllowed
}

// maxBeatSeconds is a day. Nothing sensible declares a longer beat, and
// the check exists so that nothing insensible can make the arithmetic
// wrap.
const maxBeatSeconds = 24 * 60 * 60
