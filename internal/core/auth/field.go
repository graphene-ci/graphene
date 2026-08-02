package auth

import (
	"fmt"
	"strings"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// minFieldParts: a field path is at least "<root>.<field>".
const minFieldParts = 2

// FieldEquals reports whether the dotted path ("spec.placement",
// "status.phase") of the resource holds the given scalar, compared in its
// string form. Shared by the public selector and by grant constraints —
// one matching semantics for both.
func FieldEquals(res *graphenepbv1.Resource, path, want string) bool {
	parts := strings.Split(path, ".")
	if len(parts) < minFieldParts {
		return false
	}

	var root map[string]any

	switch parts[0] {
	case "spec":
		root = res.GetSpec().ToGo()
	case "status":
		root = res.GetStatus().ToGo()
	default:
		return false
	}

	val, ok := lookup(root, parts[1:])
	if !ok {
		return false
	}

	return fmt.Sprintf("%v", val) == want
}

func lookup(m map[string]any, path []string) (any, bool) {
	var cur any = m

	for _, p := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		if cur, ok = obj[p]; !ok {
			return nil, false
		}
	}

	return cur, true
}
