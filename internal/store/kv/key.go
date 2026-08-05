package kv

import (
	"bytes"
	"slices"
	"strconv"
)

// Key is what a value is stored under.
//
// Bytes and not a string, for two reasons that are both about honesty. It
// carries separators that are not text, so calling it a string would
// invite somebody to print it, split it or lowercase it. And the one
// property everything above rests on — a shorter key is a byte prefix of
// everything beneath it — is about bytes, so the type says bytes.
//
// Nothing here builds one. A key is what an id encodes to, and only the
// typed layer above knows what an id is; down here a key arrives already
// made and is only compared, copied and printed.
type Key []byte

// HasPrefix reports whether this key lies under the given one.
//
// It is the whole of what a scan and a watch mean, so it is one method
// rather than a call to bytes.HasPrefix written out at every store that
// implements the port. An implementation that hand-rolled it could get it
// subtly wrong and still pass every test that did not look.
func (k Key) HasPrefix(prefix Key) bool { return bytes.HasPrefix(k, prefix) }

// Equal reports two keys naming the same record.
func (k Key) Equal(other Key) bool { return bytes.Equal(k, other) }

// Compare orders two keys the way a store walks them: by bytes.
func (k Key) Compare(other Key) int { return bytes.Compare(k, other) }

// IsEmpty reports the key with nothing in it.
//
// As a prefix that is not "nothing", it is EVERYTHING — the empty key is
// a prefix of every key there is. A caller that meant "no key" and passed
// this to Scan asked for the whole store, so the two cases are worth
// telling apart before the call rather than after it.
func (k Key) IsEmpty() bool { return len(k) == 0 }

// Clone copies the bytes out.
//
// This is not a convenience. A store hands back memory it owns — bbolt's
// is only valid for the life of the read transaction it came from, and
// reading it afterwards returns whatever the page holds by then. An
// implementation returns a Clone; a caller that means to keep a key past
// the call takes one. The bug this prevents does not look like a bug: it
// looks like a key that was right and later was not.
func (k Key) Clone() Key { return slices.Clone(k) }

// String renders a key so a person can read it in an error.
//
// Quoted and escaped, because the separators are control bytes: printed
// raw they vanish, and "Process␞acme␟web" and "Processacmeweb" would look
// the same in the one message somebody is trying to understand.
//
// It does not try to say which byte is which separator. Down here nothing
// knows what an id looks like, and a renderer that guessed would be wrong
// the first time a key came from somewhere else.
func (k Key) String() string { return strconv.Quote(string(k)) }
