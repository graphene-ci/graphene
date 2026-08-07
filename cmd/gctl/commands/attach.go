package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/client"
)

// fileKey is how a manifest says "the bytes are in this file" where an id
// would otherwise go:
//
//	spec:
//	  blob: {file: ./hello}
//
// A map and not a prefixed string, because a string field holding
// "@./hello" would be ambiguous the first time somebody's blob id started
// with an @ — and because this reads as what it is.
const fileKey = "file"

// errNotABlobField — a file was offered where the kind keeps no bytes.
var errNotABlobField = errors.New("that field does not hold bytes")

// attached uploads the files a manifest names and replaces each with the
// id of what was stored.
//
// WHICH FIELDS may take a file is the KIND's answer, read from its
// definition, not this command's. That is what keeps `apply` from having
// to know anything about Process — a kind that declares a blob field can
// be given a file, and one that does not cannot, and neither of those is
// a rule written here.
//
// The upload happens BEFORE the write, and that order is the only one
// that works: a resource pointing at bytes nobody stored is a resource
// that cannot run. The reverse leaves bytes nobody points at, which the
// store can be told to collect and which costs disk rather than
// correctness.
func attached(
	ctx context.Context, on *client.Kernel, kind string, spec map[string]any,
) (map[string]any, error) {
	fields, err := blobFields(ctx, on, kind)
	if err != nil {
		return nil, err
	}

	for name, value := range spec {
		named, attaching := attachment(value)
		if !attaching {
			continue
		}

		if !fields[name] {
			return nil, fmt.Errorf("%w: %s.%s", errNotABlobField, kind, name)
		}

		id, err := upload(ctx, on, named)
		if err != nil {
			return nil, err
		}

		spec[name] = id.String()
	}

	return spec, nil
}

// blobFields is which of a kind's spec fields hold bytes, by name.
//
// Top-level names only, which is what a declared field path amounts to
// here: `spec.blob` is the only shape anything declares, and a nested one
// would need a manifest walk this does not do yet. A declaration deeper
// than that is left out of the set rather than half-honored, so a file
// offered for it is refused by name instead of silently ignored.
func blobFields(ctx context.Context, on *client.Kernel, kind string) (map[string]bool, error) {
	answer, err := on.Calls().GetDefinition(ctx, &graphenepbv1.GetDefinitionRequest{Kind: kind})
	if err != nil {
		return nil, err
	}

	fields := map[string]bool{}

	for _, declared := range answer.GetDefinition().GetBlobs() {
		head, rest, deeper := strings.Cut(strings.TrimPrefix(declared, "spec."), ".")
		if deeper && rest != "" {
			continue
		}

		fields[head] = true
	}

	return fields, nil
}

// attachment reads `{file: ...}`, or says this value is not one.
func attachment(value any) (string, bool) {
	written, isMap := value.(map[string]any)
	if !isMap {
		return "", false
	}

	named, found := written[fileKey].(string)
	if !found || len(written) != 1 {
		return "", false
	}

	return named, true
}

// upload puts one file in the kernel's byte store and hands back its id.
func upload(ctx context.Context, on *client.Kernel, named string) (blob.Id, error) {
	file, err := os.Open(filepath.Clean(named))
	if err != nil {
		return "", fmt.Errorf("attach %s: %w", named, err)
	}

	defer func() { _ = file.Close() }()

	info, err := blob.Put(ctx, on.Bytes(), file)
	if err != nil {
		return "", fmt.Errorf("attach %s: %w", named, err)
	}

	return info.Id, nil
}
