package def

// Strength is the lifetime rule a reference carries.
//
// A reference is one relationship — "A points at B" — and the only thing
// that varies between kinds of them is what the pointer means for the two
// lifetimes. Making that a parameter rather than a second mechanism is
// why a resource has no owner field: an owning reference is declared like
// any other, on a field of the spec.
//
// The pointer and the rule run in OPPOSITE directions, which is the part
// worth reading twice, and the reason the two used to look unrelated. A
// strong reference points at what must outlive it. An owning reference
// points at what will take it down.
type Strength uint8

// The three lifetime rules. There is no zero value with a meaning: a
// reference that does not say is refused rather than guessed at, because
// every value below is a different answer to "what happens on delete".
const (
	// NoStrength is what an undeclared reference has. Never stored.
	NoStrength Strength = iota

	// Strong — the target cannot be deleted while this points at it.
	//
	// This is what referential integrity means here, and what an install
	// order is derived from: if a value must point at something that
	// exists, the graph says what has to be created first. It also makes a
	// cycle of strong references impossible to create, which is the price
	// and, on balance, the point.
	Strong

	// Owner — the target owns this one, and deleting it deletes this too,
	// children before parents.
	//
	// Immutable after creation. Re-pointing it would quietly change who
	// dies with whom, and the change would be invisible until something
	// died that should not have.
	Owner

	// Weak — a mention and nothing more. The target may be deleted freely
	// and this is left pointing at nothing.
	//
	// Not checked on write either, which is the whole meaning of weak: a
	// reference that had to exist before it could be written would be a
	// strong one with a softer name.
	Weak
)

// IsZero reports a reference that never said what it was.
func (s Strength) IsZero() bool { return s == NoStrength }

// Holds reports whether this reference keeps its target alive — whether
// deleting the target has to be refused while the reference is there.
func (s Strength) Holds() bool { return s == Strong }

// Owns reports whether the TARGET owns the referrer, and so takes it down
// when it goes.
func (s Strength) Owns() bool { return s == Owner }

// Requires reports whether the target has to exist when the reference is
// written. Weak is the one that does not, which is what makes it weak.
func (s Strength) Requires() bool { return s == Strong || s == Owner }

func (s Strength) String() string {
	switch s {
	case Strong:
		return "strong"
	case Owner:
		return "owner"
	case Weak:
		return "weak"
	default:
		return "unspecified"
	}
}
