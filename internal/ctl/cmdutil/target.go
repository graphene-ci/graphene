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

// LiveKinds asks the INSTALLATION what it can declare and command.
// The list is never spelled out here: a client that carries its own
// idea of the kinds is wrong the moment a kind is added, and it hides
// a kind that exists but has no records yet. The pipelines' own kinds
// are added on top — those come from user code, so only the records
// know them.
func (f *Factory) LiveKinds() []string {
	seen := map[string]bool{}
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	if resp, err := d.Resources.Kinds(cctx, connect.NewRequest(&managementv1.KindsRequest{})); err == nil {
		for _, k := range resp.Msg.GetKinds() {
			seen[k.GetName()] = true
		}
	}
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
	kinds := make([]string, 0, len(seen))
	for k := range seen {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}

// LiveCommands asks what THIS kind can be told to do. Like the kinds
// themselves, the answer belongs to the installation: a command added
// to a record shows up in the client without the client changing.
func (f *Factory) LiveCommands(kind string) []string {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	resp, err := d.Resources.Kinds(cctx, connect.NewRequest(&managementv1.KindsRequest{}))
	if err != nil {
		return nil
	}
	for _, k := range resp.Msg.GetKinds() {
		if k.GetName() != kind {
			continue
		}
		out := make([]string, 0, len(k.GetCommands()))
		for _, c := range k.GetCommands() {
			out = append(out, c.GetName())
		}
		sort.Strings(out)
		return out
	}
	return nil
}

// SpecSchema is the declaration type of one kind, as the installation
// describes it.
func (f *Factory) SpecSchema(kind string) []byte {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	resp, err := d.Resources.Kinds(cctx, connect.NewRequest(&managementv1.KindsRequest{}))
	if err != nil {
		return nil
	}
	for _, k := range resp.Msg.GetKinds() {
		if k.GetName() == kind {
			return k.GetSpecSchema()
		}
	}
	return nil
}

// CommandSchema is the payload type of one command, as the
// installation describes it — what an interactive prompt walks and
// what a caller reads to know the fields.
func (f *Factory) CommandSchema(kind, command string) []byte {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	resp, err := d.Resources.Kinds(cctx, connect.NewRequest(&managementv1.KindsRequest{}))
	if err != nil {
		return nil
	}
	for _, k := range resp.Msg.GetKinds() {
		if k.GetName() != kind {
			continue
		}
		for _, c := range k.GetCommands() {
			if c.GetName() == command {
				return c.GetPayloadSchema()
			}
		}
	}
	return nil
}

// LiveIds lists the ids of one kind.
func (f *Factory) LiveIds(kind string) []string {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	// Runs and entity records list through the same door: the system
	// kind "run" is translated by the server.
	resp, err := d.Resources.List(cctx, connect.NewRequest(&managementv1.ListRequest{
		Query: "kind=" + kind,
	}))
	if err != nil {
		return nil
	}
	var ids []string
	for _, r := range resp.Msg.GetResources() {
		if _, id, ok := strings.Cut(r.GetRef(), "/"); ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
