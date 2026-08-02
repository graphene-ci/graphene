// Package key is the identity of a resource: a kind and a path.
//
// Everything that names a resource goes through this type — the store's
// byte encoding, the wire message, what an operator types, what a log
// line prints — so the rules live in one place: how segments are joined,
// when a path names one resource instead of a subtree.
//
// Segments carry no meaning here. What a path means — an owner, an
// environment, a version — is declared by the kind's definition and
// interpreted by whoever wrote it; the kernel only ever compares prefixes.
package key

import (
	"bytes"
	"errors"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// Separators of the stored form. A full key is:
//
//	kind 0x1E seg1 0x1F seg2 0x1F ... segN 0x1F
//
// Every segment is terminated (including the last): the encoding of
// (kind, p...) is then a strict byte-prefix of the encoding of
// (kind, p..., q...) and of nothing else — prefix scans match whole
// segments, never "app" matching "app2".
const (
	sepKind    = 0x1E
	sepSegment = 0x1F
	// SepPath is what humans type and read between segments.
	SepPath = "/"
)

var (
	// ErrEmptyKind — a key without a kind names nothing.
	ErrEmptyKind = errors.New("key: kind is empty")
	// ErrSeparatorInSegment — a segment carries a reserved character.
	ErrSeparatorInSegment = errors.New("key: segment contains a reserved separator")
	// ErrEmptySegment — an empty segment would make prefixes ambiguous.
	ErrEmptySegment = errors.New("key: segment is empty")
)

// Key identifies a resource. A Key whose path is shorter than its kind's
// arity is a PREFIX: it names the subtree under it.
type Key struct {
	Kind string
	Path []string
}

// New builds a key.
func New(kind string, path ...string) Key {
	return Key{Kind: kind, Path: path}
}

// Parse reads the form people type: a kind and a slash-separated path,
// e.g. Parse("Kernel", "acme/k1").
func Parse(kind, path string) Key {
	return Key{Kind: kind, Path: SplitPath(path)}
}

// SplitPath cuts a typed path into segments, tolerating leading and
// trailing separators.
func SplitPath(path string) []string {
	trimmed := strings.Trim(path, SepPath)
	if trimmed == "" {
		return nil
	}

	return strings.Split(trimmed, SepPath)
}

// FromProto converts the wire form.
func FromProto(k *graphenepbv1.Key) Key {
	return Key{Kind: k.GetKind(), Path: k.GetPath()}
}

// Proto converts to the wire form.
func (k Key) Proto() *graphenepbv1.Key {
	return &graphenepbv1.Key{Kind: k.Kind, Path: k.Path}
}

// String is the canonical human form: "Kernel acme/k1". Kind and path are
// separated by a space because both are typed as separate arguments.
func (k Key) String() string {
	if len(k.Path) == 0 {
		return k.Kind
	}

	return k.Kind + " " + k.PathString()
}

// PathString is the path alone: "acme/k1".
func (k Key) PathString() string {
	return strings.Join(k.Path, SepPath)
}

// IsExact reports whether the path names ONE resource of a kind with the
// given arity (the number of segments its definition declares).
func (k Key) IsExact(arity int) bool {
	return len(k.Path) == arity
}

// HasPrefix reports whether this key lies under the given prefix.
func (k Key) HasPrefix(prefix Key) bool {
	if prefix.Kind != "" && prefix.Kind != k.Kind {
		return false
	}

	if len(prefix.Path) > len(k.Path) {
		return false
	}

	for i, seg := range prefix.Path {
		if k.Path[i] != seg {
			return false
		}
	}

	return true
}

// Validate checks what the stored encoding assumes.
func (k Key) Validate() error {
	if k.Kind == "" {
		return ErrEmptyKind
	}

	for _, seg := range k.Path {
		if seg == "" {
			return ErrEmptySegment
		}

		if strings.ContainsAny(seg, SepPath+string(rune(sepKind))+string(rune(sepSegment))) {
			return ErrSeparatorInSegment
		}
	}

	return nil
}

// Encode is the stored form. A key of a shorter path IS the prefix of all
// its descendants, so the same function serves lookups and scans.
func (k Key) Encode() []byte {
	var buf bytes.Buffer

	buf.WriteString(k.Kind)
	buf.WriteByte(sepKind)

	for _, seg := range k.Path {
		buf.WriteString(seg)
		buf.WriteByte(sepSegment)
	}

	return buf.Bytes()
}

// Decode reads the stored form back.
func Decode(encoded []byte) Key {
	idx := bytes.IndexByte(encoded, sepKind)
	if idx < 0 {
		return Key{Kind: string(encoded)}
	}

	out := Key{Kind: string(encoded[:idx])}
	rest := encoded[idx+1:]

	for len(rest) > 0 {
		end := bytes.IndexByte(rest, sepSegment)
		if end < 0 {
			out.Path = append(out.Path, string(rest))

			break
		}

		out.Path = append(out.Path, string(rest[:end]))
		rest = rest[end+1:]
	}

	return out
}
