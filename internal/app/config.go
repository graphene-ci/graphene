package app

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

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
	storeKey  = "store"
	nameKey   = "name"
	listenKey = "listen"
	cacheKey  = "cache"
)

// Config is how a kernel is configured, and it comes from a FILE.
//
// It used to live in the kernel's own store, and that was wrong for one
// reason that outweighs everything in its favour: a configuration that
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
type Config struct {
	store  string
	name   string
	listen string
	cache  int
}

// NewConfig states a configuration, filling in what was left out.
//
// Absent is a default and not an error. A configuration that refused to
// be partial would make an administrator editing one line answerable for
// every other, and the lines they left out are exactly the ones they had
// no opinion about.
func NewConfig(store, name, listen string, cache int) Config {
	if store == "" {
		store = defaultStore()
	}

	if name == "" {
		name = defaultName
	}

	if listen == "" {
		listen = defaultListen
	}

	if cache <= 0 {
		cache = defaultCache
	}

	return Config{store: store, name: name, listen: listen, cache: cache}
}

// Store is the file the kernel keeps everything in.
//
// Read once, at start. A store cannot move under a kernel that is using
// it, so a file that changes this while one is running is describing the
// next start rather than this one.
func (c Config) Store() string { return c.store }

// Name is which kernel this is, and the path of its own record. Read once
// for the same reason: a kernel that renamed itself mid-flight would
// leave its old record behind and answer to a name nothing had heard of.
func (c Config) Name() string { return c.name }

// Listen is the address it serves on, and this one does change: an edit
// moves the socket.
func (c Config) Listen() string { return c.listen }

// Cache is how many keys the byte layer remembers.
func (c Config) Cache() int { return c.cache }

// Eq reports two configurations asking for the same thing.
func (c Config) Eq(other Config) bool { return c == other }

func (c Config) String() string {
	return fmt.Sprintf("store %s, name %s, listen %s, cache %d",
		c.store, c.name, c.listen, c.cache)
}

// ReadConfig reads a configuration out of a file.
//
// A file that is not there is every default rather than a failure: a
// kernel started with no configuration should come up somewhere sensible
// and say where, not refuse to exist. A file that IS there and will not
// parse is a failure — somebody wrote it meaning something, and guessing
// what would be worse than stopping.
func ReadConfig(path string) (Config, error) {
	loaded := koanf.New(".")

	if err := loaded.Load(file.Provider(path), yaml.Parser()); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return NewConfig("", "", "", 0), nil
		}

		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}

	return NewConfig(
		loaded.String(storeKey),
		loaded.String(nameKey),
		loaded.String(listenKey),
		loaded.Int(cacheKey),
	), nil
}

// WriteConfig writes one down, which is what configure does around an
// editor.
func WriteConfig(path string, config Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("prepare %s: %w", filepath.Dir(path), err)
	}

	written := fmt.Sprintf(""+
		"# Where the kernel keeps everything. Read once, at start.\n"+
		"%s: %s\n\n"+
		"# Which kernel this is, and the path of its own record.\n"+
		"%s: %s\n\n"+
		"# The address it serves on. Editing this moves the socket.\n"+
		"%s: %s\n\n"+
		"# How many keys the byte layer remembers.\n"+
		"%s: %d\n",
		storeKey, config.store,
		nameKey, config.name,
		listenKey, config.listen,
		cacheKey, config.cache)

	if err := os.WriteFile(path, []byte(written), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
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

// DefaultConfigPath is where the file lives when nobody says otherwise.
func DefaultConfigPath() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".config", "graphene", "kernel.yaml")
	}

	return "kernel.yaml"
}

// runningOn is what a kernel reports about itself: what it is, and what
// it was told to be.
//
// os and arch are the reason another kernel reads this record at all — a
// controller is a binary, and delivering one means knowing what platform
// to build it for.
func runningOn(config Config, version string) map[string]any {
	return map[string]any{
		osField:      runtime.GOOS,
		archField:    runtime.GOARCH,
		versionField: version,
		listenField:  config.listen,
		storeField:   config.store,
		cacheField:   uint64(config.cache),
	}
}
