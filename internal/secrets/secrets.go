// Package secrets resolves secret names to values, per namespace. Only
// names travel the system; a value is handed out exactly at the point
// of use and never travels back. In-memory with a config seed — the
// interface stays when encrypted persistence replaces it.
package secrets

import (
	"fmt"
	"sort"
	"sync"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// Store resolves secret names within one namespace.
type Store interface {
	Get(name id.SecretId) (string, error)
}

// Namespaced keeps every namespace's secret set.
type Namespaced struct {
	mu sync.RWMutex
	m  map[string]map[string]string
}

// NewNamespaced seeds the "default" namespace from the config.
func NewNamespaced(seedDefault map[string]string) *Namespaced {
	s := &Namespaced{m: map[string]map[string]string{}}
	for k, v := range seedDefault {
		s.set("default", k, v)
	}
	return s
}

// Get resolves one name in a namespace.
func (s *Namespaced) Get(namespace, name string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[namespace][name]
	if !ok {
		return "", fmt.Errorf("secret %q is not configured in %q", name, namespace)
	}
	return v, nil
}

// Set writes a value (management plane).
func (s *Namespaced) Set(namespace, name, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(namespace, name, value)
}

func (s *Namespaced) set(namespace, name, value string) {
	if s.m[namespace] == nil {
		s.m[namespace] = map[string]string{}
	}
	s.m[namespace][name] = value
}

// Delete forgets a name.
func (s *Namespaced) Delete(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m[namespace], name)
}

// List returns the NAMES of a namespace's secrets — never the values.
func (s *Namespaced) List(namespace string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.m[namespace]))
	for k := range s.m[namespace] {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// In binds the store to one namespace as the flow-facing Store.
func (s *Namespaced) In(namespace string) Store {
	return bound{s: s, ns: namespace}
}

type bound struct {
	s  *Namespaced
	ns string
}

func (b bound) Get(name id.SecretId) (string, error) {
	return b.s.Get(b.ns, string(name))
}
