package auth

import (
	"errors"
	"fmt"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
)

// errGrantShape — a Role's grants list holds something that is not a grant.
var errGrantShape = errors.New("auth: grant is not an object")

// The Role/Identity kinds carry authorization as ordinary resource values.
// These helpers are the single translation point between that wire shape
// and the model enforced above — used by the resource-backed token source
// and by the escalation guard, so both read grants exactly the same way.

// GrantsFromSpec decodes a Role's spec.grants.
func GrantsFromSpec(spec *schemapb.StructValue) ([]Grant, error) {
	raw, isList := spec.ToGo()["grants"].([]any)
	if !isList {
		return nil, nil
	}

	grants := make([]Grant, 0, len(raw))

	for i, item := range raw {
		obj, isObject := item.(map[string]any)
		if !isObject {
			return nil, fmt.Errorf("%w: index %d", errGrantShape, i)
		}

		grants = append(grants, grantFromMap(obj))
	}

	return grants, nil
}

func grantFromMap(obj map[string]any) Grant {
	grant := Grant{Kind: stringOf(obj["kind"])}

	for _, verb := range stringsOf(obj["verbs"]) {
		grant.Verbs = append(grant.Verbs, Verb(verb))
	}

	grant.PathPrefix = stringsOf(obj["path_prefix"])

	for _, part := range stringsOf(obj["parts"]) {
		grant.Parts = append(grant.Parts, Part(part))
	}

	if where, isList := obj["where"].([]any); isList {
		for _, item := range where {
			term, isObject := item.(map[string]any)
			if !isObject {
				continue
			}

			grant.Where = append(grant.Where, Constraint{
				Path:  stringOf(term["path"]),
				Equal: stringOf(term["equal"]),
			})
		}
	}

	return grant
}

// GrantsToSpec is the inverse, used by tooling and tests.
func GrantsToSpec(grants []Grant) *schemapb.StructValue {
	items := make([]any, 0, len(grants))

	for _, grant := range grants {
		verbs := make([]any, 0, len(grant.Verbs))
		for _, verb := range grant.Verbs {
			verbs = append(verbs, string(verb))
		}

		parts := make([]any, 0, len(grant.Parts))
		for _, part := range grant.Parts {
			parts = append(parts, string(part))
		}

		prefix := make([]any, 0, len(grant.PathPrefix))
		for _, seg := range grant.PathPrefix {
			prefix = append(prefix, seg)
		}

		where := make([]any, 0, len(grant.Where))
		for _, term := range grant.Where {
			where = append(where, map[string]any{"path": term.Path, "equal": term.Equal})
		}

		items = append(items, map[string]any{
			"verbs":       verbs,
			"kind":        grant.Kind,
			"path_prefix": prefix,
			"parts":       parts,
			"where":       where,
		})
	}

	return schemapb.MustStructFromGo(map[string]any{"grants": items})
}

// IdentitySpec is the decoded form of an Identity resource.
type IdentitySpec struct {
	PrincipalKind PrincipalKind
	Roles         []string
	TokenSHA256   []string
}

// IdentityFromSpec decodes an Identity's spec.
func IdentityFromSpec(spec *schemapb.StructValue) IdentitySpec {
	values := spec.ToGo()

	return IdentitySpec{
		PrincipalKind: PrincipalKind(stringOf(values["principal_kind"])),
		Roles:         stringsOf(values["roles"]),
		TokenSHA256:   stringsOf(values["token_sha256"]),
	}
}

func stringOf(value any) string {
	str, _ := value.(string)

	return str
}

func stringsOf(value any) []string {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, stringOf(item))
	}

	return out
}
