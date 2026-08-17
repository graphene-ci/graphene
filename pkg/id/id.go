// Package id is the identifier dictionary of graphene: distinct named
// types for everything the system addresses (suffix Id, per repository
// convention). Literals by cast for values written in code; Parse* for
// values arriving from the outside world. Identifiers end up in workflow
// IDs, queue names, and Event History — never put secrets or PII in them.
package id

import (
	"fmt"
	"strings"
)

// MachineId identifies one machine record.
type MachineId string

// RunId identifies one pipeline run.
type RunId string

// ArtifactId identifies one artifact record.
type ArtifactId string

// PipelineId identifies a registered pipeline.
type PipelineId string

// SecretId names a secret; only the name ever travels, never the value.
type SecretId string

func validate(kind, s string) error {
	if s == "" {
		return fmt.Errorf("empty %s id", kind)
	}
	if strings.ContainsAny(s, " \t\n") {
		return fmt.Errorf("%s id %q contains whitespace", kind, s)
	}
	return nil
}

// Validate reports whether the machine id is well-formed.
func (m MachineId) Validate() error { return validate("machine", string(m)) }

// Validate reports whether the run id is well-formed.
func (r RunId) Validate() error { return validate("run", string(r)) }

// Validate reports whether the artifact id is well-formed.
func (a ArtifactId) Validate() error { return validate("artifact", string(a)) }

// Validate reports whether the pipeline id is well-formed.
func (p PipelineId) Validate() error { return validate("pipeline", string(p)) }

// Validate reports whether the secret name is well-formed.
func (s SecretId) Validate() error { return validate("secret", string(s)) }

// ParseMachineId validates a machine id from external input.
func ParseMachineId(s string) (MachineId, error) {
	m := MachineId(s)
	if err := m.Validate(); err != nil {
		return "", err
	}
	return m, nil
}

// ParseRunId validates a run id from external input.
func ParseRunId(s string) (RunId, error) {
	r := RunId(s)
	if err := r.Validate(); err != nil {
		return "", err
	}
	return r, nil
}

// ParseArtifactId validates an artifact id from external input.
func ParseArtifactId(s string) (ArtifactId, error) {
	a := ArtifactId(s)
	if err := a.Validate(); err != nil {
		return "", err
	}
	return a, nil
}
