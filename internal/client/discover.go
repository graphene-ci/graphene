package client

import (
	"fmt"
	"path/filepath"

	"github.com/graphene-ci/graphene/internal/app/config"
	"github.com/graphene-ci/graphene/internal/link"
)

// Local is the name a discovered kernel is saved under.
const Local = "local"

// Reach is the kernel a command means.
//
// Named, or the current one, or — if nothing is configured at all — the
// kernel running on THIS MACHINE, found by reading its file.
//
// Discovery rather than shared configuration. A client and a kernel keep
// separate files and always will: a context is carried between machines
// and points at whichever kernel somebody means today, while a kernel's
// file describes one kernel on one machine. But on the machine that runs
// one, that file is right there and has both halves in it — so making a
// person copy an address and a credential they already have would be
// ceremony, not safety.
//
// What is discovered is SAVED, so it is discovered once and is a context
// like any other afterwards — visible in the list, replaceable, and not a
// magic answer that changes under somebody when they edit a kernel.
func Reach(all *Contexts, named, kernelFile string) (Context, error) {
	if named != "" {
		return all.Named(named)
	}

	current, err := all.Current()
	if err == nil {
		return current, nil
	}

	found, err := discover(kernelFile)
	if err != nil {
		return Context{}, err
	}

	if err := all.Save(found); err != nil {
		return Context{}, err
	}

	return found, nil
}

// discover reads the kernel on this machine out of its own file.
//
// A kernel that FORWARDS is not discovered: it has no identities of its
// own and the credential in its file is its own, not one that would let a
// person do anything. Whoever wants that kernel names it themselves.
func discover(kernelFile string) (Context, error) {
	read, err := config.Read(kernelFile)
	if err != nil {
		return Context{}, fmt.Errorf("%w; and %s: %w", ErrNoContext, kernelFile, err)
	}

	local, keeps := read.Local()
	if !keeps {
		return Context{}, fmt.Errorf(
			"%w; the kernel in %s forwards to another one, so name that one instead",
			ErrNoContext, kernelFile)
	}

	if local.Token() == "" {
		return Context{}, fmt.Errorf(
			"%w; the kernel in %s has not started yet, so it has no credential",
			ErrNoContext, kernelFile)
	}

	// The pin is read off the kernel's own key material rather than
	// copied by hand. A fingerprint exists so that somebody pointing at a
	// kernel ACROSS a network can tell which one answered; on the machine
	// that runs it, the certificate is right there and public, and asking
	// a person to retype it would be ceremony rather than safety.
	pinned, err := link.PinIn(filepath.Dir(local.Store()))
	if err != nil {
		return Context{}, fmt.Errorf(
			"%w; the kernel in %s has no key yet, so it has not started: %w",
			ErrNoContext, kernelFile, err)
	}

	return NewContext(Local, read.Listen(), local.Token(), pinned.String())
}
