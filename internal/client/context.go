// Package client is the other side of the wire: what something talking TO
// a kernel needs, and nothing about being one.
//
// A kernel and a client keep separate files and always will. A context is
// a thing an operator carries between machines and points at whichever
// kernel they mean today; a kernel's file describes one kernel on one
// machine. Writing either into the other would tie a fleet's worth of
// addresses to one installation.
//
// What ties them together is DISCOVERY and not configuration: on the
// machine a kernel runs on, its file is right there, and a client with
// nothing configured can read it rather than making somebody copy an
// address they already have.
package client

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"

	"github.com/graphene-ci/graphene/internal/link"
)

// The names the file uses. A format: changing one silently drops every
// context that used it.
const (
	currentKey = "current"
	kernelsKey = "kernels"

	addressField = "address"
	tokenField   = "token"
	pinField     = "pin"

	dirMode = 0o700
	// fileMode is 0600 because this file IS credentials. It belongs to
	// whoever runs the client and to nobody else on the machine.
	fileMode = 0o600
)

// What a set of contexts can be wrong about.
var (
	// ErrNoContext — nothing is named and nothing was found.
	ErrNoContext = errors.New("no kernel is configured")
	// ErrNoSuchContext — a name nobody has saved.
	ErrNoSuchContext = errors.New("no such kernel")
	// ErrNoAddress, ErrNoToken and ErrNoPin — a third of a context is not
	// one. The pin is not optional: without it a client cannot tell the
	// kernel it means from whoever answers at that address, and it is
	// about to send that kernel a credential.
	ErrNoAddress = errors.New("a kernel needs an address")
	ErrNoToken   = errors.New("a kernel needs a token")
	ErrNoPin     = errors.New("a kernel needs a pin; ask it for one with `graphened pin`")
)

// Context is one kernel a client can reach.
type Context struct {
	name    string
	address string
	token   string
	pin     link.Pin
}

// NewContext states one, refusing the halves that mean nothing on their
// own: an address with no credential is refused by every call, and a
// credential with no address has nowhere to go.
func NewContext(name, address, token, pin string) (Context, error) {
	switch {
	case strings.TrimSpace(name) == "":
		return Context{}, ErrNoSuchContext
	case address == "":
		return Context{}, ErrNoAddress
	case token == "":
		return Context{}, ErrNoToken
	case pin == "":
		return Context{}, ErrNoPin
	}

	pinned, err := link.NewPin(pin)
	if err != nil {
		return Context{}, fmt.Errorf("%w: %w", ErrNoPin, err)
	}

	return Context{name: name, address: address, token: token, pin: pinned}, nil
}

// Name is what this kernel is called here — a label of the operator's,
// not the kernel's own name.
func (c Context) Name() string { return c.name }

// Address is where it is.
func (c Context) Address() string { return c.address }

// Token is what to call it with.
func (c Context) Token() string { return c.token }

// Pin is which kernel that address has to turn out to be.
func (c Context) Pin() link.Pin { return c.pin }

// IsZero reports a context that was never stated.
func (c Context) IsZero() bool { return c.address == "" }

// String says which kernel this is WITHOUT its credential: this ends up
// in output and in error text.
func (c Context) String() string {
	return fmt.Sprintf("%s (%s)", c.name, c.address)
}

// Contexts is every kernel a client knows, and which one it means now.
type Contexts struct {
	path    string
	current string
	kernels map[string]Context
}

// Read reads the contexts a client has saved.
//
// A file that is not there is an EMPTY set rather than a failure: a
// client that has never been told about a kernel is the ordinary case,
// and it is about to go looking for one on this machine.
func Read(path string) (*Contexts, error) {
	all := &Contexts{path: path, kernels: map[string]Context{}}

	loaded := koanf.New(".")

	if err := loaded.Load(file.Provider(path), yaml.Parser()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return all, nil
		}

		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	all.current = loaded.String(currentKey)

	for _, name := range names(loaded) {
		read, err := NewContext(name,
			loaded.String(kernelsKey+"."+name+"."+addressField),
			loaded.String(kernelsKey+"."+name+"."+tokenField),
			loaded.String(kernelsKey+"."+name+"."+pinField))
		if err != nil {
			return nil, fmt.Errorf("%s: %s: %w", path, name, err)
		}

		all.kernels[name] = read
	}

	return all, nil
}

// names is every kernel the file names, in the order a person reads.
func names(loaded *koanf.Koanf) []string {
	found := map[string]struct{}{}

	for _, key := range loaded.Keys() {
		rest, under := strings.CutPrefix(key, kernelsKey+".")
		if !under {
			continue
		}

		name, _, deeper := strings.Cut(rest, ".")
		if deeper {
			found[name] = struct{}{}
		}
	}

	sorted := make([]string, 0, len(found))
	for name := range found {
		sorted = append(sorted, name)
	}

	sort.Strings(sorted)

	return sorted
}

// All is every kernel saved, sorted.
func (c *Contexts) All() []Context {
	all := make([]Context, 0, len(c.kernels))

	for _, one := range c.kernels {
		all = append(all, one)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })

	return all
}

// Current is the kernel a command means when nobody said which.
func (c *Contexts) Current() (Context, error) {
	if c.current == "" {
		return Context{}, ErrNoContext
	}

	return c.Named(c.current)
}

// Named is one saved kernel.
func (c *Contexts) Named(name string) (Context, error) {
	found, saved := c.kernels[name]
	if !saved {
		return Context{}, fmt.Errorf("%w: %s", ErrNoSuchContext, name)
	}

	return found, nil
}

// Save adds or replaces one kernel, and makes it current if nothing was.
//
// Current if nothing was, because the first kernel somebody saves is the
// one they meant — and leaving it unselected would make the next command
// fail for a reason that reads like a bug.
func (c *Contexts) Save(one Context) error {
	c.kernels[one.name] = one

	if c.current == "" {
		c.current = one.name
	}

	return c.write()
}

// Use makes one saved kernel the current one.
func (c *Contexts) Use(name string) error {
	if _, err := c.Named(name); err != nil {
		return err
	}

	c.current = name

	return c.write()
}

// Forget drops one. The current one going means no current one, rather
// than a silently different kernel taking the commands aimed at it.
func (c *Contexts) Forget(name string) error {
	if _, err := c.Named(name); err != nil {
		return err
	}

	delete(c.kernels, name)

	if c.current == name {
		c.current = ""
	}

	return c.write()
}

// write puts the file back.
//
// By hand and not by marshaling, the same as the kernel's own file: what
// is written is read by a person, and a marshaller strips the comments
// out of the file it rewrites.
func (c *Contexts) write() error {
	if err := os.MkdirAll(filepath.Dir(c.path), dirMode); err != nil {
		return fmt.Errorf("prepare %s: %w", filepath.Dir(c.path), err)
	}

	var written strings.Builder

	written.WriteString("" +
		"# Every kernel this client knows, and which one it means now.\n" +
		"#\n" +
		"# These are CREDENTIALS. The file is written 0600 and belongs to\n" +
		"# whoever runs the client, not to any kernel: a kernel never\n" +
		"# writes here, and this is carried between machines while a\n" +
		"# kernel's own file describes one kernel on one machine.\n" +
		"\n" +
		currentKey + ": " + c.current + "\n\n" +
		kernelsKey + ":\n")

	for _, one := range c.All() {
		fmt.Fprintf(&written, "  %s:\n    %s: %s\n    %s: %s\n    %s: %s\n",
			one.name, addressField, one.address, tokenField, one.token,
			pinField, one.pin)
	}

	if err := os.WriteFile(c.path, []byte(written.String()), fileMode); err != nil {
		return fmt.Errorf("write %s: %w", c.path, err)
	}

	return nil
}

// DefaultPath is where a client keeps its contexts when nobody says
// otherwise.
func DefaultPath() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".config", "graphene", "contexts.yaml")
	}

	return "contexts.yaml"
}
