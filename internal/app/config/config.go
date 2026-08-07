package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// What a kernel does when the file says nothing.
const (
	defaultListen = "127.0.0.1:7373"
	defaultCache  = 4096
	defaultName   = "local"
)

// The names the file uses. They are a format: changing one silently
// ignores every configuration that used it.
const (
	nameKey   = "name"
	listenKey = "listen"

	storeKey      = "store"
	pathKey       = "store.path"
	cacheKey      = "store.cache"
	storeTokenKey = "store.token"

	// dirMode and fileMode: a configuration file carries a credential, and
	// a file that is only sometimes private is a file nobody can trust.
	dirMode  = 0o700
	fileMode = 0o600

	upstreamKey = "upstream"
	addressKey  = "upstream.address"
	tokenKey    = "upstream.token"
	workKey     = "upstream.work"
	pinKey      = "upstream.pin"
)

// What a configuration can be wrong about.
var (
	// ErrTwoModes — a kernel that keeps its own store and a kernel that
	// forwards to another one are two different things, and a file
	// describing both describes neither.
	ErrTwoModes = errors.New("a kernel is either a store or an upstream, not both")
	// ErrNoAddress — an upstream with nowhere to go.
	ErrNoAddress = errors.New("upstream needs an address")
	// ErrNoToken — an upstream a kernel cannot introduce itself to. Every
	// call the far side takes is authorized, so a subordinate with no
	// credential is a subordinate that is refused everything, including
	// the record that says it exists.
	ErrNoToken = errors.New("upstream needs a token")
	// ErrNoPin — an address with nothing saying which kernel is there.
	ErrNoPin = errors.New("upstream needs the pin of the kernel above")
)

// Config is how a kernel is configured, and it comes from a FILE.
//
// It used to live in the kernel's own store, and that was wrong for one
// reason that outweighs everything in its favor: a configuration that
// breaks the kernel could then only be fixed THROUGH the kernel. A
// mistyped address, a store that will not open, anything at all that
// stops it accepting calls — each of them locks the door from the inside.
//
// A file is fixed with a text editor, which is why every daemon keeps one.
//
// What the store keeps instead is a REPORT: the kernel writes what it is
// running with into its own record, so a fleet can be asked how it is
// configured over the same API as everything else. Being told and
// reporting are different directions, and only one of them can be locked
// out.
//
// A kernel is one of TWO THINGS, and the type says so. It either keeps a
// store or it forwards to another kernel, and the two are read out
// through Local and Upstream, each of which reports whether it is the one
// this configuration means. Nothing hands back a store path that is empty
// because the kernel has no store — a caller that did not ask which kind
// of kernel it has cannot get an answer that assumes.
type Config struct {
	name   string
	listen string

	local Local
	up    Upstream
}

// Local is a kernel that keeps everything itself.
type Local struct {
	store string
	cache int
	token string
}

// Store is the file the kernel keeps everything in.
func (l Local) Store() string { return l.store }

// Cache is how many keys the byte layer remembers.
func (l Local) Cache() int { return l.cache }

// Token is the credential of the FIRST caller in this store — the one
// the kernel made because nothing could enter an empty store otherwise.
//
// It lives beside the store it belongs to, for the same reason the
// upstream's credential lives beside the address it opens: a token is
// about one place, and the file already names the place. There is one
// file per kernel and this is it.
//
// Empty until the kernel has made one, which it does at the first start
// and writes back here. Written by hand it is taken as given — the store
// gets that identity rather than a minted one, which is how a kernel can
// be installed with a credential somebody already knows.
func (l Local) Token() string { return l.token }

// Upstream is a kernel that keeps nothing and forwards everything.
//
// It is subordinate in the only sense that matters: it has no answers of
// its own. Every call it takes it passes on with the CALLER'S credential,
// so the kernel above authorizes the person who actually asked, and the
// one in the middle decides nothing.
type Upstream struct {
	address string
	token   string
	work    string
	// pins is one or more: a subordinate that accepts the kernel above's
	// next key beside its current one can be told about the next one
	// before it is served, which is what makes a key replaceable without
	// a window where nothing can connect.
	pins []string
}

// Address is the kernel this one forwards to.
func (u Upstream) Address() string { return u.address }

// Token is how this kernel introduces ITSELF — used for the one thing it
// does on its own behalf, which is recording that it exists, and for
// fetching what it was told to run. A forwarded call carries its caller's
// credential instead.
func (u Upstream) Token() string { return u.token }

// Pins are WHICH kernel is above: the hash of its key, or of its keys
// while one is being replaced.
//
// Required, and there is no trust-on-first-use to fall back to. The first
// connection is exactly the one worth being in the middle of, and a
// subordinate that remembered whoever answered first would have spent its
// security to save a line of configuration.
func (u Upstream) Pins() []string { return slices.Clone(u.pins) }

// Work is the directory a subordinate runs things out of: fetched bytes,
// working directories, and the sockets processes talk back through.
//
// A kernel with a store puts these beside it, because the store already
// names a place. A subordinate has no store, so it says where instead —
// and it does need somewhere, because running things is not something
// only a kernel with a store does.
func (u Upstream) Work() string { return u.work }

// NewLocal states a kernel that keeps its own store, filling in what was
// left out.
//
// Absent is a default and not an error. A configuration that refused to
// be partial would make an administrator editing one line answerable for
// every other, and the lines they left out are exactly the ones they had
// no opinion about.
func NewLocal(name, listen, store string, cache int, token string) Config {
	if store == "" {
		store = defaultStore()
	}

	if cache <= 0 {
		cache = defaultCache
	}

	return Config{
		name:   named(name),
		listen: listening(listen),
		local:  Local{store: store, cache: cache, token: token},
	}
}

// NewUpstream states a kernel that forwards to another one.
//
// Both halves are required, and neither has a default worth guessing: an
// address nobody gave is a kernel talking to itself, and a credential
// nobody gave is a kernel refused everything it asks for.
func NewUpstream(name, listen, address, token, work string, pins ...string) (Config, error) {
	pinned := make([]string, 0, len(pins))

	for _, one := range pins {
		if one != "" {
			pinned = append(pinned, one)
		}
	}

	switch {
	case address == "":
		return Config{}, ErrNoAddress
	case token == "":
		return Config{}, ErrNoToken
	case len(pinned) == 0:
		return Config{}, ErrNoPin
	}

	if work == "" {
		work = defaultWork()
	}

	return Config{
		name:   named(name),
		listen: listening(listen),
		up:     Upstream{address: address, token: token, work: work, pins: pinned},
	}, nil
}

// Name is which kernel this is, and the path of its own record.
//
// Read once, at start: a kernel that renamed itself mid-flight would
// leave its old record behind and answer to a name nothing had heard of.
func (c Config) Name() string { return c.name }

// Listen is the address it serves on, and it is the ONLY thing an edit
// changes while a kernel runs — everything else describes the next start.
// A store cannot move under a kernel using it, a cache is sized when it
// is built, and a connection is made once; a file that changes any of
// them is describing what happens next time rather than now.
func (c Config) Listen() string { return c.listen }

// Begun is this configuration, with the credential the kernel just made.
//
// It is the one thing a kernel writes back into its own file, and it
// happens once in the life of a store: the secret exists in the clear for
// as long as it takes to get here, and what the store keeps is a digest
// that cannot produce it again.
func (c Config) Begun(token string) Config {
	c.local.token = token

	return c
}

// At is this configuration, serving somewhere else.
//
// It is the only edit that can be applied to a running kernel, so it is
// the only one with a method: everything else about a kernel was decided
// when it was built, and a copy with a different store would describe
// something that is not running.
func (c Config) At(listen string) Config {
	c.listen = listening(listen)

	return c
}

// Local reports the store this kernel keeps, and whether it keeps one.
func (c Config) Local() (Local, bool) { return c.local, c.local.store != "" }

// Upstream reports the kernel this one forwards to, and whether it
// forwards at all.
func (c Config) Upstream() (Upstream, bool) { return c.up, c.up.address != "" }

// Eq reports two configurations asking for the same thing.
//
// Field by field rather than by comparing the structs, because one of
// them holds a list now. Worth being explicit about anyway: this is what
// decides whether an edited file changed anything, and a comparison that
// silently stopped being total would make a kernel ignore a change.
func (c Config) Eq(other Config) bool {
	return c.name == other.name &&
		c.listen == other.listen &&
		c.local == other.local &&
		c.up.address == other.up.address &&
		c.up.token == other.up.token &&
		c.up.work == other.up.work &&
		slices.Equal(c.up.pins, other.up.pins)
}

// String says what a kernel is, WITHOUT its credential. This ends up in
// logs and in error text, and a token that reached either of those is a
// token that has to be changed.
func (c Config) String() string {
	if up, forwards := c.Upstream(); forwards {
		return fmt.Sprintf("name %s, listen %s, upstream %s",
			c.name, c.listen, up.address)
	}

	return fmt.Sprintf("name %s, listen %s, store %s, cache %d",
		c.name, c.listen, c.local.store, c.local.cache)
}

func named(name string) string {
	if name == "" {
		return defaultName
	}

	return name
}

func listening(at string) string {
	if at == "" {
		return defaultListen
	}

	return at
}

// Read reads a configuration out of a file.
//
// A file that is not there is every default rather than a failure: a
// kernel started with no configuration should come up somewhere sensible
// and say where, not refuse to exist. A file that IS there and will not
// parse is a failure — somebody wrote it meaning something, and guessing
// what would be worse than stopping.
//
// Which of the two kernels it describes is decided by which section is
// PRESENT, not by which fields happen to be filled. Both is refused
// rather than resolved by precedence: a precedence rule is a silent
// answer to a question somebody got wrong, and the wrong half of this one
// is a kernel storing locally what it meant to forward.
func Read(path string) (Config, error) {
	loaded := koanf.New(".")

	if err := loaded.Load(file.Provider(path), yaml.Parser()); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return NewLocal("", "", "", 0, ""), nil
		}

		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}

	name, listen := loaded.String(nameKey), loaded.String(listenKey)

	switch {
	case loaded.Exists(storeKey) && loaded.Exists(upstreamKey):
		return Config{}, fmt.Errorf("%s: %w", path, ErrTwoModes)

	case loaded.Exists(upstreamKey):
		read, err := NewUpstream(name, listen,
			loaded.String(addressKey), loaded.String(tokenKey),
			loaded.String(workKey), PinsIn(loaded, pinKey)...)
		if err != nil {
			return Config{}, fmt.Errorf("%s: %w", path, err)
		}

		return read, nil

	default:
		return NewLocal(name, listen,
			loaded.String(pathKey), loaded.Int(cacheKey),
			loaded.String(storeTokenKey)), nil
	}
}

// Write writes one down, which is what configure does around an editor.
//
// By hand rather than by marshaling, because what is being written is
// read by a PERSON: the comments are half of it, and a marshaller strips
// them from the file it rewrites — including any the administrator left
// there themselves.
func Write(path string, config Config) error {
	if err := os.MkdirAll(filepath.Dir(path), dirMode); err != nil {
		return fmt.Errorf("prepare %s: %w", filepath.Dir(path), err)
	}

	// 0600 because an upstream configuration carries a credential, and a
	// file that is only sometimes private is a file nobody can trust.
	if err := os.WriteFile(path, []byte(written(config)), fileMode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
}

// written is the file a person opens.
func written(config Config) string {
	head := fmt.Sprintf(""+
		"# Which kernel this is, and the path of its own record.\n"+
		"%s: %s\n\n"+
		"# The address it serves on. Editing this moves the socket; it is\n"+
		"# the only line that takes effect without a restart.\n"+
		"%s: %s\n\n",
		nameKey, config.name,
		listenKey, config.listen)

	if up, forwards := config.Upstream(); forwards {
		return head + fmt.Sprintf(""+
			"# This kernel keeps nothing. Every call is passed on to the\n"+
			"# kernel below with the credential of whoever made it, which\n"+
			"# is what makes it subordinate rather than a second kernel.\n"+
			"#\n"+
			"# The token is this kernel's OWN, used for the one thing it\n"+
			"# does on its own behalf: recording up there that it exists.\n"+
			"#\n"+
			"# Work is where it runs things out of: fetched bytes, working\n"+
			"# directories, and the sockets processes talk back through. A\n"+
			"# kernel with a store puts these beside it; this one has none.\n"+
			"#\n"+
			"# The pin says WHICH kernel is up there: the hash of its key,\n"+
			"# which that kernel prints with `graphened pin`. Without it\n"+
			"# there is nothing to tell the right kernel from whoever else\n"+
			"# answers at that address.\n"+
			"%s:\n"+
			"  address: %s\n"+
			"  token: %s\n"+
			"%s"+
			"  work: %s\n",
			upstreamKey, up.address, up.token, writtenPins(up.pins), up.work)
	}

	written := head + fmt.Sprintf(""+
		"# This kernel keeps its own store. Read once, at start.\n"+
		"#\n"+
		"# Replace this section with an `upstream:` one to make it\n"+
		"# subordinate instead. Both at once is refused.\n"+
		"%s:\n"+
		"  path: %s\n"+
		"  # How many keys the byte layer remembers.\n"+
		"  cache: %d\n",
		storeKey, config.local.store, config.local.cache)

	if config.local.token == "" {
		return written
	}

	return written + fmt.Sprintf(""+
		"\n"+
		"  # The first caller in this store, made by the kernel because\n"+
		"  # nothing could enter an empty one otherwise. Written here\n"+
		"  # once and never again: the store keeps a digest and cannot\n"+
		"  # produce this back. Losing it means replacing the store.\n"+
		"  token: %s\n",
		config.local.token)
}

// defaultStore is where a kernel keeps its file when nobody says
// otherwise: under the user's state directory, because a kernel installed
// as a user service has no business writing anywhere else.
func defaultStore() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "state", "graphene", "kernel.db")
	}

	return "kernel.db"
}

// PinsIn reads a pin field that may be one or several.
//
// Exported because a client reads the same field out of its own file, and
// two readers of one format disagree the day one of them is fixed.
//
// By ASKING WHAT IS THERE rather than by trying a scalar first: koanf
// renders a list through String() as "[a b]", which is not empty, so a
// scalar-first read takes a list of two pins for one unreadable one.
//
// A file is written by a person, and both spellings are theirs: one pin
// on one line is the ordinary case, and the list appears for as long as a
// key is being replaced. Making somebody write a list of one forever, to
// pay for a case that lasts an afternoon, would be the wrong trade.
func PinsIn(loaded *koanf.Koanf, key string) []string {
	switch found := loaded.Get(key).(type) {
	case string:
		if found == "" {
			return nil
		}

		return []string{found}

	case []any, []string:
		return loaded.Strings(key)

	default:
		return nil
	}
}

func writtenPins(pins []string) string {
	if len(pins) == 1 {
		return fmt.Sprintf("  pin: %s\n", pins[0])
	}

	var written strings.Builder

	written.WriteString("  pin:\n")

	for _, one := range pins {
		fmt.Fprintf(&written, "    - %s\n", one)
	}

	return written.String()
}

// defaultWork is where a subordinate runs things out of when nobody says
// otherwise: beside where a kernel with a store would have kept one.
func defaultWork() string { return filepath.Dir(defaultStore()) }

// DefaultPath is where the file lives when nobody says otherwise.
func DefaultPath() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".config", "graphene", "kernel.yaml")
	}

	return "kernel.yaml"
}
