// Package secrets resolves secret names to values. Only names travel the
// system; a value is handed out exactly at the point of use and never
// travels back. The static store is the dev contour's implementation —
// encrypted storage replaces it, the interface stays.
package secrets

import (
	"fmt"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// Store resolves secret names.
type Store interface {
	Get(name id.SecretId) (string, error)
}

// Static serves values from an in-memory map (loaded from config).
type Static map[string]string

// Get resolves one name.
func (s Static) Get(name id.SecretId) (string, error) {
	v, ok := s[string(name)]
	if !ok {
		return "", fmt.Errorf("secret %q is not configured", name)
	}
	return v, nil
}
