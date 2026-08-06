package app

import (
	"fmt"
	"runtime"

	"github.com/gopherex/schemapb/go/schemapb"
)

// The defaults a kernel starts with when nobody has said otherwise.
const (
	defaultListen = "127.0.0.1:7373"
	defaultCache  = 4096
)

// Config is the part of a kernel's configuration that lives in its own
// store, and can therefore change while it is running.
//
// The split is not a preference, it is forced: to read the store you have
// to know where the store is, so the path cannot come from inside it.
// Everything that is not needed to open the file is in here, where a
// write to it is an ordinary write and the kernel learns of it through
// its own watch.
//
// A value, built by one constructor and never edited afterwards. What
// changes is which Config the kernel is holding, and that is one pointer
// swap under a lock rather than fields moving under a reader.
type Config struct {
	listen string
	cache  int
}

// NewConfig states a configuration.
//
// An empty field is not an error but a default: a configuration that
// refused to be partial would mean an administrator editing one line had
// to know every other, and the ones they left out are exactly the ones
// they had no opinion about.
func NewConfig(listen string, cache int) Config {
	if listen == "" {
		listen = defaultListen
	}

	if cache <= 0 {
		cache = defaultCache
	}

	return Config{listen: listen, cache: cache}
}

// Listen is the address the kernel serves on.
func (c Config) Listen() string { return c.listen }

// Cache is how many keys the byte layer remembers.
func (c Config) Cache() int { return c.cache }

// Eq reports two configurations asking for the same thing, which is what
// tells a change worth acting on from a write that touched something
// else.
func (c Config) Eq(other Config) bool {
	return c.listen == other.listen && c.cache == other.cache
}

func (c Config) String() string {
	return fmt.Sprintf("listen %s, cache %d", c.listen, c.cache)
}

// Spec writes a configuration down as a resource's spec.
func (c Config) Spec() *schemapb.StructValue {
	return schemapb.MustStructFromGo(map[string]any{
		listenField: c.listen,
		cacheField:  uint64(c.cache),
	})
}

// ConfigFrom reads one back.
//
// It reads what it recognises and defaults the rest, which is the same
// forgiveness NewConfig gives and matters more here: a record written by
// an older or newer build carries fields this one does not know, and
// refusing the whole configuration over one of them would take a kernel
// down for a field it did not need.
func ConfigFrom(spec *schemapb.StructValue) Config {
	fields := spec.GetFields()

	return NewConfig(
		fields[listenField].GetStringValue(),
		int(fields[cacheField].GetUint64Value()),
	)
}

// status is what a kernel reports about itself: not anybody's to choose,
// and the reason another kernel reads this record at all — a controller
// is a binary, and delivering one means knowing what to build it for.
func status(version string) *schemapb.StructValue {
	return schemapb.MustStructFromGo(map[string]any{
		osField:      runtime.GOOS,
		archField:    runtime.GOARCH,
		versionField: version,
	})
}
