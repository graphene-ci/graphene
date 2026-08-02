package ctl

import (
	"fmt"
	"io"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// WriteResources prints resources as the canonical YAML stream.
func WriteResources(out io.Writer, resources []*graphenepbv1.Resource) error {
	raw, err := EncodeResources(resources)
	if err != nil {
		return err
	}

	if _, err := out.Write(raw); err != nil {
		return fmt.Errorf("ctl: write: %w", err)
	}

	return nil
}

// WriteDefinitions prints the kind table: what a kernel knows and the
// shape of each kind's path.
func WriteDefinitions(out io.Writer, defs []*graphenepbv1.ResourceDefinition) error {
	for _, def := range defs {
		line := fmt.Sprintf("%s\tv%d\t%s\n",
			def.GetKind(),
			def.GetVersion(),
			"/"+strings.Join(def.GetPathSegments(), "/"),
		)

		if _, err := io.WriteString(out, line); err != nil {
			return fmt.Errorf("ctl: write: %w", err)
		}
	}

	return nil
}

// WriteEvent prints one watch event: the type, the key, and — for changes —
// the resource itself.
func WriteEvent(out io.Writer, event *graphenepbv1.WatchEvent) error {
	switch event.GetType() {
	case graphenepbv1.EventType_EVENT_TYPE_SYNC:
		_, err := fmt.Fprintf(out, "# synced at revision %d\n", event.GetStoreRevision())

		return wrapWrite(err)

	case graphenepbv1.EventType_EVENT_TYPE_DELETE:
		_, err := fmt.Fprintf(out, "# deleted %s at revision %d\n",
			keyText(event.GetResource().GetKey()), event.GetStoreRevision())

		return wrapWrite(err)

	case graphenepbv1.EventType_EVENT_TYPE_PUT, graphenepbv1.EventType_EVENT_TYPE_UNSPECIFIED:
		if _, err := fmt.Fprintf(out, "# %s at revision %d\n",
			keyText(event.GetResource().GetKey()), event.GetStoreRevision()); err != nil {
			return wrapWrite(err)
		}

		return WriteResources(out, []*graphenepbv1.Resource{event.GetResource()})
	}

	return nil
}

func wrapWrite(err error) error {
	if err != nil {
		return fmt.Errorf("ctl: write: %w", err)
	}

	return nil
}

// ParseSelector turns "spec.placement=k1" terms into selector matches.
func ParseSelector(terms []string) ([]*graphenepbv1.FieldMatch, error) {
	if len(terms) == 0 {
		return nil, nil
	}

	out := make([]*graphenepbv1.FieldMatch, 0, len(terms))

	for _, term := range terms {
		path, value, found := strings.Cut(term, "=")
		if !found || path == "" {
			return nil, fmt.Errorf("%w: %q", ErrBadSelector, term)
		}

		out = append(out, &graphenepbv1.FieldMatch{Path: path, Value: value})
	}

	return out, nil
}
