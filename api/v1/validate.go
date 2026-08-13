package v1

import (
	"errors"
	"fmt"
	"regexp"
)

// Size limits on the free-form JSON our records carry.
//
// These are not taste. Parameters travel into Temporal's history and stay
// there for the life of the run; a result sits in a record every controller
// in the cluster receives on every change. Anything genuinely large is an
// artifact and belongs in storage, with a reference here.
//
// The limit lives in Go rather than in the CRD schema because CRD
// validation counts keys and string lengths, not the bytes of an arbitrary
// JSON value. Callers check before writing: the CLI before it creates a
// record, the operator before it starts a workflow.
const (
	// MaxParamsBytes limits a run's parameters.
	MaxParamsBytes = 64 << 10
	// MaxResultBytes limits what a pipeline may return into its record.
	MaxResultBytes = 64 << 10
)

// Refusals these records can produce. Callers compare against them.
var (
	// ErrParamsTooBig means the parameters exceed MaxParamsBytes.
	ErrParamsTooBig = errors.New("параметров больше предела")
	// ErrResultTooBig means the result exceeds MaxResultBytes.
	ErrResultTooBig = errors.New("результат больше предела")
	// ErrNoRevision means a run does not say what to execute.
	ErrNoRevision = errors.New("прогон не указывает ревизию")
	// ErrNotDigest means an image is named by tag rather than by digest.
	ErrNotDigest = errors.New("образ указан не дайджестом")
	// ErrNoPipeline means a revision does not say whose it is.
	ErrNoPipeline = errors.New("ревизия не указывает пайплайн")
	// ErrNoQueue means a revision does not name a task queue.
	ErrNoQueue = errors.New("ревизия не указывает очередь")
)

// digestRef matches a reference pinned by digest, with or without a tag in
// front of it. A tag alone is not enough: tags are moved, and then
// repeating a run stops meaning repeating the same program.
var digestRef = regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`)

// Validate checks what a run asks for, before anything is written.
func (s RunSpec) Validate() error {
	if s.RevisionRef.Name == "" {
		return ErrNoRevision
	}

	if s.Params != nil && len(s.Params.Raw) > MaxParamsBytes {
		return fmt.Errorf("%w: %d > %d", ErrParamsTooBig, len(s.Params.Raw), MaxParamsBytes)
	}

	return nil
}

// Validate checks what a run reports, before it is written back.
func (s RunStatus) Validate() error {
	if s.Result != nil && len(s.Result.Raw) > MaxResultBytes {
		return fmt.Errorf("%w: %d > %d", ErrResultTooBig, len(s.Result.Raw), MaxResultBytes)
	}

	return nil
}

// Validate checks a revision before it is written. A revision is immutable
// once created, so this is the only chance.
func (s PipelineRevisionSpec) Validate() error {
	if s.PipelineRef.Name == "" {
		return ErrNoPipeline
	}

	if s.Queue == "" {
		return ErrNoQueue
	}

	if !digestRef.MatchString(s.Image) {
		return fmt.Errorf("%w: %q", ErrNotDigest, s.Image)
	}

	return nil
}
