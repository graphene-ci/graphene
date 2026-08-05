// Package revision is the store's clock.
//
// The store keeps ONE counter for the whole of itself. Every write bumps
// it and stamps the written record with the new value. That single number
// is what three separate things are built out of, and none of them work
// without it:
//
//   - Agreement between writers. "Write this, but only if it is still the
//     one I read" is the only way two processes settle a race without a
//     lock between them. A lock would need a third party to be alive; a
//     revision needs nobody.
//
//   - Catching up. "Everything that happened after this" is how a
//     controller resumes after a restart instead of re-reading the world.
//     The counter being store-wide and not per-record is exactly what
//     makes this one number rather than one per record.
//
//   - Telling a resource from its own corpse. A path can be deleted and
//     created again; the revision it was born at cannot repeat, so the
//     new one is not mistaken for the old.
//
// It is one type in four roles, and the zero value means something
// different in each. So each meaning has a name of its own, and they are
// all the same number:
//
//	role         zero        what the zero says
//	stamp        None        never written — a stored record never has it
//	expectation  Absent      must not exist yet: this write creates it
//	cursor       Beginning   from the start of history
//	birth        None        was not created
//
// One type and not three because the number is genuinely the same one:
// the cursor a watcher hands back IS the stamp it last saw, and the
// expectation a writer states IS the stamp it read. Splitting them would
// mean converting at every boundary, which is ceremony around a value
// that never changes in the crossing.
package revision

import (
	"fmt"
	"strconv"
)

// Revision is a position in the store's history: what the store-wide
// counter stood at when a write happened.
//
// Monotonic and never reused. Gaps are normal — a revision belongs to one
// write anywhere in the store, so most values belong to some other record
// than the one being looked at.
type Revision uint64

// The zero revision, under the three names its three roles give it. They
// are equal on purpose: what makes them different is which question is
// being asked, and the name is how the call site says which.
const (
	// None is the revision of a thing that was never written. A record
	// that came out of the store never carries it, so a None on a record
	// read back means the record was built by hand and not stored.
	None Revision = 0

	// Absent is what a caller expects of a path it means to CREATE:
	// nothing there yet. Written apart from None because the caller is
	// stating an expectation about the world, not reporting a stamp.
	Absent Revision = 0

	// Beginning is the cursor that asks for all of history — in practice
	// a snapshot of now, then everything after it.
	Beginning Revision = 0
)

// Parse reads a revision written as a decimal number, which is how one
// arrives from a command line or a URL.
//
// Only digits. No "r42", no sign, no whitespace to be lenient about: this
// reads what String wrote, and a caller who is guessing at the format
// should get an error rather than an off-by-one.
func Parse(raw string) (Revision, error) {
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return None, fmt.Errorf("%w: %q", ErrMalformed, raw)
	}

	return Revision(parsed), nil
}

// Uint64 is the number, for whoever has to put it on the wire or in a
// counter.
func (r Revision) Uint64() uint64 { return uint64(r) }

// IsZero reports the zero revision — whichever of its three meanings the
// caller is asking about.
func (r Revision) IsZero() bool { return r == None }

// Next is the revision after this one. The store bumping its counter is
// the only place this belongs; it is here so that "the next one" is
// written once and not as a bare +1 wherever the counter lives.
func (r Revision) Next() Revision { return r + 1 }

// Eq is the comparison compare-and-swap is made of: the record is at the
// revision the caller expected, or the caller is working from a stale
// read and must look again.
func (r Revision) Eq(other Revision) bool { return r == other }

// After reports that this revision happened later than the other. A
// watcher delivers what is After its cursor.
func (r Revision) After(other Revision) bool { return r > other }

// Before reports that this revision happened earlier than the other. A
// store that has compacted refuses what is Before the oldest it kept.
func (r Revision) Before(other Revision) bool { return r < other }

// String writes the plain decimal number, because that is the only form
// Parse reads back and because a revision is read alongside other
// revisions, where a prefix would be noise on every one of them.
func (r Revision) String() string { return strconv.FormatUint(uint64(r), 10) }
