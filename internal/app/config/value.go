// Package config is the kernel's configuration: plain Go structs loaded by
// structconf (tag defaults < file < env), plus the resolution of sensitive
// values.
//
// A kernel has no "role" setting: what it does follows from what is
// configured. A Store section makes it hold truth, a Listen section makes
// it serve, a Link section makes it connect elsewhere. Optional sections
// are pointers — absent means nil, and their required fields do not fire.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

var (
	// ErrValueUnset — no source was given for a required value.
	ErrValueUnset = errors.New("config: value has no source (set exactly one of inline/file/env)")
	// ErrValueAmbiguous — more than one source was given.
	ErrValueAmbiguous = errors.New("config: value has multiple sources (set exactly one of inline/file/env)")
	// ErrValueEmpty — the source resolved to nothing.
	ErrValueEmpty = errors.New("config: value resolved empty")
)

// Value is a sensitive string with exactly one source. Every secret in the
// configuration (tokens, credentials) is expressed this way, so operators
// can keep material out of the file without the code caring how:
//
//	token: { file: /etc/graphen/token }
//	token: { env: GRAPHEN_TOKEN }
//	token: { inline: dev-only-secret }
type Value struct {
	Inline string `mapstructure:"inline"`
	File   string `mapstructure:"file"`
	Env    string `mapstructure:"env"`
}

// IsZero reports whether no source was configured at all.
func (v Value) IsZero() bool {
	return v.Inline == "" && v.File == "" && v.Env == ""
}

// Resolve reads the value. Exactly one source must be set; the result is
// trimmed of surrounding whitespace (files usually end with a newline).
func (v Value) Resolve() (string, error) {
	sources := 0

	for _, set := range []bool{v.Inline != "", v.File != "", v.Env != ""} {
		if set {
			sources++
		}
	}

	switch {
	case sources == 0:
		return "", ErrValueUnset
	case sources > 1:
		return "", ErrValueAmbiguous
	}

	raw, err := v.read()
	if err != nil {
		return "", err
	}

	value := strings.TrimSpace(raw)
	if value == "" {
		return "", ErrValueEmpty
	}

	return value, nil
}

func (v Value) read() (string, error) {
	switch {
	case v.Inline != "":
		return v.Inline, nil

	case v.File != "":
		raw, err := os.ReadFile(v.File)
		if err != nil {
			return "", fmt.Errorf("config: read value file %s: %w", v.File, err)
		}

		return string(raw), nil

	default:
		raw, ok := os.LookupEnv(v.Env)
		if !ok {
			return "", fmt.Errorf("config: env %s: %w", v.Env, ErrValueEmpty)
		}

		return raw, nil
	}
}
