package cmdutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"connectrpc.com/connect"
	yamlpkg "sigs.k8s.io/yaml"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// TargetRef reads one record target from the args: either "kind/id" in
// one word or "<kind> <id>" in two. Returns the ref and the leftovers.
func TargetRef(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, fmt.Errorf("want a target: \"<kind> <id>\" or \"kind/id\"")
	}
	if strings.Contains(args[0], "/") {
		return args[0], args[1:], nil
	}
	if len(args) < 2 {
		return "", nil, fmt.Errorf("want a target: \"%s <id>\" or \"%s/<id>\"", args[0], args[0])
	}
	return args[0] + "/" + args[1], args[2:], nil
}

// ReadFile reads the path, "-" meaning stdin.
func ReadFile(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path) //nolint:gosec // the user's own file argument
}

// JSONInput resolves the inline/file flag pair into JSON bytes: the
// file may be YAML (converted), the inline value must be JSON. The
// pair is mutually exclusive; "-" as the path reads stdin.
func JSONInput(flagName, inline, file string) ([]byte, error) {
	switch {
	case inline != "" && file != "":
		return nil, fmt.Errorf("--%s and --%s-file are mutually exclusive", flagName, flagName)
	case file != "":
		raw, err := ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("--%s-file: %w", flagName, err)
		}
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || trimmed[0] == '{' || trimmed[0] == '[' {
			return trimmed, nil
		}
		converted, err := yamlpkg.YAMLToJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("--%s-file: neither JSON nor YAML: %w", flagName, err)
		}
		return converted, nil
	default:
		return []byte(inline), nil
	}
}

// --- Live lookups for shell completion: best effort, a short timeout,
// silent on failure — completion must never nag. ---

// BuiltinKinds are offered even when the server is unreachable.
var BuiltinKinds = []string{"agent", "artifact", "stand", "pipeline", "run"}

// LiveKinds unions the built-in kinds, the kinds of live records, and
// the kinds named by the pipelines' manifests — the discovery surface.
func (f *Factory) LiveKinds() []string {
	seen := map[string]bool{}
	for _, k := range BuiltinKinds {
		seen[k] = true
	}
	if d, cctx, cancel := f.completionDoor(); d != nil {
		defer cancel()
		if resp, err := d.Resources.List(cctx, connect.NewRequest(&managementv1.ListRequest{
			Selector: &managementv1.Selector{},
		})); err == nil {
			for _, r := range resp.Msg.GetResources() {
				if r.GetKind() != "" {
					seen[r.GetKind()] = true
				}
				if r.GetKind() == "pipeline" {
					var st struct {
						Manifest struct {
							Kinds []string `json:"kinds"`
						} `json:"manifest"`
					}
					if json.Unmarshal(r.GetState(), &st) == nil {
						for _, k := range st.Manifest.Kinds {
							seen[k] = true
						}
					}
				}
			}
		}
	}
	kinds := make([]string, 0, len(seen))
	for k := range seen {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}

// LiveIds lists the ids of one kind.
func (f *Factory) LiveIds(kind string) []string {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	var ids []string
	if kind == "run" {
		resp, err := d.Runs.ListRuns(cctx, connect.NewRequest(&managementv1.ListRunsRequest{}))
		if err != nil {
			return nil
		}
		for _, r := range resp.Msg.GetRuns() {
			ids = append(ids, r.GetRunId())
		}
	} else {
		resp, err := d.Resources.List(cctx, connect.NewRequest(&managementv1.ListRequest{
			Selector: &managementv1.Selector{Kind: kind},
		}))
		if err != nil {
			return nil
		}
		for _, r := range resp.Msg.GetResources() {
			if _, id, ok := strings.Cut(r.GetRef(), "/"); ok {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	return ids
}
