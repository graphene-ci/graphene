package ctl

// File inputs, kubectl's stance: the inline flag and the -file flag
// are separate and mutually exclusive; "-" as the path reads stdin.
// JSON destinations accept YAML files and convert them (the k8s
// mapping); raw destinations (secrets) take the bytes untouched.

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"sigs.k8s.io/yaml"
)

// readFile reads the path, "-" meaning stdin.
func readFile(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path) //nolint:gosec // the user's own file argument
}

// jsonInput resolves the inline/file flag pair into JSON bytes: the
// file may be YAML (converted), the inline value must be JSON.
func jsonInput(flagName, inline, file string) ([]byte, error) {
	switch {
	case inline != "" && file != "":
		return nil, fmt.Errorf("--%s and --%s-file are mutually exclusive", flagName, flagName)
	case file != "":
		raw, err := readFile(file)
		if err != nil {
			return nil, fmt.Errorf("--%s-file: %w", flagName, err)
		}
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || trimmed[0] == '{' || trimmed[0] == '[' {
			return trimmed, nil
		}
		converted, err := yaml.YAMLToJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("--%s-file: neither JSON nor YAML: %w", flagName, err)
		}
		return converted, nil
	default:
		return []byte(inline), nil
	}
}
