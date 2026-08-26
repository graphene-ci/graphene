package cmdutil

import (
	"bytes"
	"context"
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

// LiveKinds asks the DICTIONARY: kind records cover everything the
// installation serves — the system's own kinds and the ones pipelines
// bring — so one cheap listing answers, and nothing is spelled out or
// guessed here.
func (f *Factory) LiveKinds() []string {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	resp, err := d.Resources.List(cctx, connect.NewRequest(&managementv1.ListRequest{
		Query: "kind=kind",
	}))
	if err != nil {
		return nil
	}
	var kinds []string
	for _, r := range resp.Msg.GetResources() {
		if _, name, ok := strings.Cut(r.GetRef(), "/"); ok {
			kinds = append(kinds, name)
		}
	}
	sort.Strings(kinds)
	return kinds
}

// KindEntry is a dictionary entry as the client reads it.
type KindEntry struct {
	Origin      string          `json:"origin"`
	Declarable  bool            `json:"declarable"`
	Description string          `json:"description"`
	SpecSchema  json.RawMessage `json:"specSchema"`
	Commands    []struct {
		Name          string          `json:"name"`
		PayloadSchema json.RawMessage `json:"payloadSchema"`
	} `json:"commands"`
	Dimensions []string `json:"dimensions"`
	BroughtBy  []string `json:"broughtBy"`
	Records    int      `json:"records"`
}

// KindEntryOf reads one dictionary entry over an ORDINARY dial: a form
// that cannot learn its fields must say so, not silently skip itself.
func (f *Factory) KindEntryOf(ctx context.Context, kind string) (*KindEntry, error) {
	d, err := f.Dial()
	if err != nil {
		return nil, err
	}
	got, err := d.Resources.Get(ctx, connect.NewRequest(&managementv1.GetRequest{
		Ref: "kind/" + kind,
	}))
	if err != nil {
		return nil, fmt.Errorf("kind %q is not in this installation's dictionary: %w", kind, err)
	}
	var e KindEntry
	if err := json.Unmarshal(got.Msg.GetResource().GetState(), &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// LiveCommands is the completion half: silent, best effort.
func (f *Factory) LiveCommands(kind string) []string {
	d, cctx, cancel := f.completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	got, err := d.Resources.Get(cctx, connect.NewRequest(&managementv1.GetRequest{
		Ref: "kind/" + kind,
	}))
	if err != nil {
		return nil
	}
	var e KindEntry
	if json.Unmarshal(got.Msg.GetResource().GetState(), &e) != nil {
		return nil
	}
	out := make([]string, 0, len(e.Commands))
	for _, c := range e.Commands {
		out = append(out, c.Name)
	}
	sort.Strings(out)
	return out
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
