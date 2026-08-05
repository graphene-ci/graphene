package auth

import "errors"

// Why a caller was refused. The first two are about the request, the rest
// are about the grant that was supposed to permit it.
var (
	// ErrForbidden — the caller holds no grant that permits this.
	//
	// One error for every refusal, saying what was asked and by whom but
	// nothing about what exists. "You may not read that" and "that is not
	// there" have to look the same to somebody probing, or the refusal
	// itself becomes a way to enumerate the store.
	ErrForbidden = errors.New("not permitted")

	// ErrNoPrincipal — a session with nobody in it. Nothing is granted to
	// nobody, and this is told apart from a plain refusal because it means
	// the caller was never established, which is a bug in the transport
	// rather than a decision about permissions.
	ErrNoPrincipal = errors.New("no caller")

	// ErrEscalation — a role handing out more than its author holds.
	//
	// This is what makes "may manage users" a smaller privilege than "may
	// do anything". Without it the two are the same, because anyone who
	// can write a Role can write themselves one that permits everything.
	ErrEscalation = errors.New("a grant cannot hand out more than it holds")

	// ErrUnknownVerb — a grant naming something no method answers to.
	// Refused rather than ignored: a grant nobody can interpret permits
	// nothing, and a role full of them would look like permission.
	ErrUnknownVerb = errors.New("unknown verb")

	// ErrNoVerb, ErrNoKind — half a grant is not a grant.
	ErrNoVerb = errors.New("grant names no verb")
	ErrNoKind = errors.New("grant names no kind")

	// ErrKindVerbPath — a path on a grant of a kind-level verb. There is
	// no instance for it to confine, and a grant that looked confined but
	// was not would read as narrower than it is.
	ErrKindVerbPath = errors.New("a kind-level grant carries no path")
)
