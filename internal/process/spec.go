package process

import (
	"fmt"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/blob"
	"github.com/graphene-ci/graphene/internal/types/resource"
)

// spec is a process record read into what the agent acts on.
//
// Read once, when the record arrives, rather than field by field wherever
// somebody needs one: a value that failed to make sense should stop being
// a process at the moment it is seen, and not halfway through starting.
type spec struct {
	blob     blob.Id
	format   string
	args     []string
	env      map[string]string
	identity string
	restart  string
}

// specOf reads a record.
func specOf(found resource.Resource) (spec, error) {
	fields := found.Spec()
	if fields == nil {
		return spec{}, errNoSpec
	}

	id, err := blob.NewId(text(fields, blobField))
	if err != nil {
		return spec{}, fmt.Errorf("process %s: %w", found.Id(), err)
	}

	return spec{
		blob:     id,
		format:   text(fields, formatField),
		args:     list(fields, argsField),
		env:      pairs(fields, envField),
		identity: text(fields, identityField),
		restart:  text(fields, restartField),
	}, nil
}

// resident reports a process that an exit is a fault for.
func (s spec) resident() bool { return s.restart == RestartAlways }

func text(fields *schemapb.StructValue, name string) string {
	found, ok := schemapb.As[string](fields.GetFields()[name])
	if !ok {
		return ""
	}

	return found
}

func list(fields *schemapb.StructValue, name string) []string {
	values := fields.GetFields()[name].GetListValue().GetItems()

	out := make([]string, 0, len(values))

	for _, value := range values {
		if found, ok := schemapb.As[string](value); ok {
			out = append(out, found)
		}
	}

	return out
}

// pairs reads a list of {name, value} objects, which is how an
// environment is written: map values carry a schema of their own here,
// and every variable would otherwise be an object wrapping a string.
func pairs(fields *schemapb.StructValue, at string) map[string]string {
	values := fields.GetFields()[at].GetListValue().GetItems()

	out := make(map[string]string, len(values))

	for _, value := range values {
		pair := value.GetStructValue()
		if pair == nil {
			continue
		}

		named, ok := schemapb.As[string](pair.GetFields()[nameField])
		if !ok {
			continue
		}

		if held, filled := schemapb.As[string](pair.GetFields()[valueField]); filled {
			out[named] = held
		}
	}

	return out
}
