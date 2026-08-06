package path

import (
	"fmt"
	"slices"
	"strings"

	"github.com/graphene-ci/graphene/internal/common/str"
)

// The separators the key encoding reserves, and the room a segment has
// in it.
const (
	// pathSeparator is what a person types and reads between segments.
	pathSeparator   = '/'
	maxSegmentBytes = 256
)

// KindSeparator and SegmentSeparator are the bytes a key is encoded with,
// so that a shorter key is a byte prefix of everything beneath it. A
// segment carrying one would not be ugly, it would be ambiguous — which
// is why they are forbidden below and not escaped.
//
// They are exported because the encoder and the rule that forbids them
// have to agree, and two copies of a load-bearing byte agree only until
// somebody changes one of them.
const (
	KindSeparator    = 0x1E
	SegmentSeparator = 0x1F
)

// tPathSegmentStrRules is the whole of what a segment name may be, in the
// order it is decided. These limits are stated nowhere else: a segment is
// what this list says, and the list is the documentation.
//
//nolint:gochecknoglobals // a validated value cannot be a const; treated as one
var tPathSegmentStrRules = []str.Rule{
	str.UTF8(),
	// NFKC rather than NFC: a fullwidth or ligature spelling is how
	// someone writes a segment that LOOKS like an existing one.
	str.NFKC(),
	str.TrimSpace(),
	// Fold rather than Lower: two segments differing only in case are the
	// same segment, and sameness is the question a key asks.
	str.Fold(),
	str.NotEmpty(),
	str.MaxBytes(maxSegmentBytes),
	// Invisible characters produce a segment that looks identical to the
	// one beside it and compares unequal to it.
	str.NoInvisible(),
	str.Forbid(pathSeparator, KindSeparator, SegmentSeparator),
}

// TPathSegment names one position in a path SHAPE — "tenant", "kernel".
// It is not the value that fills the position.
//
// A defined type over str.String inherits no methods, so String and Eq
// are declared here. That is four lines per named string type, and it is
// the price of a type that is still a map key and still compares with ==.
type TPathSegment str.String

// NewTPathSegment normalizes and checks one position's name.
func NewTPathSegment(raw string) (TPathSegment, error) {
	return str.New[TPathSegment](raw, tPathSegmentStrRules...)
}

func (t TPathSegment) String() string { return string(t) }

func (t TPathSegment) Eq(other TPathSegment) bool { return t == other }

// IsZero reports the unset name. NewTPathSegment never returns it: the
// rules refuse an empty value.
func (t TPathSegment) IsZero() bool { return t == "" }

// TPath is a path shape: what a kind calls each position, in order.
//
// It is a struct and not a slice so that it cannot be written down by
// hand. A shape assembled from a literal would skip the checks below and
// then act as a factory for paths — which is the one thing a shape does,
// so it has to be the one thing nobody can fake.
type TPath struct {
	names []TPathSegment
}

// NewTPath builds a shape, checking every name.
//
// Duplicates are refused. Two positions with one name make every message
// about "the tenant segment" ambiguous, and a shape is written by hand
// exactly once, so nothing is gained by allowing it.
func NewTPath(names ...string) (TPath, error) {
	shape := TPath{names: make([]TPathSegment, 0, len(names))}

	for index, raw := range names {
		name, err := NewTPathSegment(raw)
		if err != nil {
			return TPath{}, fmt.Errorf("position %d: %w", index, err)
		}

		if slices.Contains(shape.names, name) {
			return TPath{}, fmt.Errorf("%w: %q appears twice", ErrDuplicateName, name)
		}

		shape.names = append(shape.names, name)
	}

	return shape, nil
}

// MustNewTPath is NewTPath for a shape written into the binary.
//
// It panics for the same reason kind.MustNew does: a builtin shape that
// does not pass the rules is a binary that cannot start, and there is
// nobody to hand an error to. Never reach for it with names that came
// from outside.
func MustNewTPath(names ...string) TPath {
	shape, err := NewTPath(names...)
	if err != nil {
		panic("builtin path shape: " + err.Error())
	}

	return shape
}

// Arity is how many positions a path of this shape can fill.
func (t TPath) Arity() int { return len(t.names) }

// IsZero reports a shape that was never built.
func (t TPath) IsZero() bool { return len(t.names) == 0 }

// Names copies the position names out.
func (t TPath) Names() []TPathSegment { return slices.Clone(t.names) }

// Name is the name of one position; the zero name when there is no such
// position, which callers holding an Arity never ask for.
func (t TPath) Name(index int) TPathSegment {
	if index < 0 || index >= len(t.names) {
		return ""
	}

	return t.names[index]
}

func (t TPath) Eq(other TPath) bool {
	return slices.Equal(t.names, other.names)
}

// Agrees reports two shapes naming their positions the same way as far as
// both of them go.
//
// This is weaker than Eq on purpose, and it is what a PREFIX needs. A
// prefix built here keeps the whole shape and fills fewer positions, but
// one that arrived over the wire carries only the positions it filled —
// the message has no room for the rest — so the two spellings of
// "everything under /local" are (kernel, name) with one value and
// (kernel) with one value. Demanding Eq refuses the second, which means
// refusing every subtree a caller ever asks for.
//
// Where they overlap they must MATCH. Two kinds shaping their paths
// differently are still two different things, and a shape that disagrees
// at the first position is not a shorter version of anything.
func (t TPath) Agrees(other TPath) bool {
	shared := min(len(t.names), len(other.names))

	return slices.Equal(t.names[:shared], other.names[:shared])
}

func (t TPath) String() string {
	result := make([]string, len(t.names))
	for i, v := range t.names {
		result[i] = v.String()
	}

	return "/" + strings.Join(result, "/")
}

// New fills this shape with values, in order.
//
// Fewer values than the shape has positions is allowed, and is how a
// PREFIX is written: "everything under acme" is a real thing to ask for,
// and it is the same type as "acme/k1" so that no caller carries two.
// More values is refused — that is not a longer path, it is a path of
// some other kind.
func (t TPath) New(values ...string) (Path, error) {
	if len(values) > len(t.names) {
		return Path{}, fmt.Errorf("%w: %d values for shape %s (%d positions)",
			ErrArity, len(values), t, len(t.names))
	}

	filled := Path{shape: t, values: make([]str.String, 0, len(values))}

	for index, raw := range values {
		// The value goes through the same rules as the position's name,
		// and needs them more: a name is written once by whoever declared
		// the kind, a value arrives from whoever is calling. It is also
		// the half that ends up in the key, where a separator would not
		// be ugly but ambiguous.
		value, err := str.New[str.String](raw, tPathSegmentStrRules...)
		if err != nil {
			return Path{}, fmt.Errorf("%s: %w", t.names[index], err)
		}

		filled.values = append(filled.values, value)
	}

	return filled, nil
}

// Parse splits a written path — "acme/k1" — and fills the shape with it.
//
// Splitting is unambiguous because '/' is one of the characters a value
// may never contain, so this is the inverse of String and not a guess.
// A leading slash is tolerated; the empty string is the whole kind.
func (t TPath) Parse(raw string) (Path, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return t.New()
	}

	return t.New(strings.Split(trimmed, "/")...)
}
