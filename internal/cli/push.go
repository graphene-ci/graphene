// Package cli is what the commands do, kept apart from how they are
// spelled. A command parses flags and prints; everything below is what
// actually happens, and it can be checked without a terminal.
package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

// Builder turns a directory of Go code into an image and reports it BY
// DIGEST. It is an interface because building is slow and full of somebody
// else's failure modes, and neither belongs in a test of what we record.
type Builder interface {
	Build(ctx context.Context, dir string) (string, error)
}

// Refusals these operations produce.
var (
	// ErrNoRevision means the pipeline has nothing pushed yet.
	ErrNoRevision = errors.New("у пайплайна нет ни одной ревизии: сначала graphene push")
	// ErrNotDigest means the builder handed back something moveable.
	ErrNotDigest = errors.New("сборка вернула не дайджест")
	// ErrAlreadyOver means the run has already finished.
	ErrAlreadyOver = errors.New("прогон уже завершён")
)

// revisionSuffix is how much of the digest goes into the revision's name.
// Seven characters is what git uses for the same job and for the same
// reason: enough to be unique in practice, short enough to read.
const revisionSuffix = 7

// PushRequest is one push.
type PushRequest struct {
	Kube      client.Client
	Builder   Builder
	Namespace string
	Pipeline  string
	Dir       string
}

// Push builds the pipeline and records it as a revision.
//
// The revision's name comes from the image digest, so pushing the same code
// twice is the same revision rather than two. Its queue is its own, which is
// what lets runs of an old revision drain on the old worker while a new one
// takes new work — versioning without a single branch in the pipeline's code.
func Push(ctx context.Context, req PushRequest) (*v1.PipelineRevision, error) {
	image, err := req.Builder.Build(ctx, req.Dir)
	if err != nil {
		return nil, fmt.Errorf("сборка не прошла: %w", err)
	}

	short, err := shortDigest(image)
	if err != nil {
		return nil, err
	}

	pipeline, err := ensurePipeline(ctx, req)
	if err != nil {
		return nil, err
	}

	revision := &v1.PipelineRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Pipeline + "-" + short,
			Namespace: req.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1.GroupVersion.String(),
				Kind:       "Pipeline",
				Name:       pipeline.Name,
				UID:        pipeline.UID,
			}},
		},
		Spec: v1.PipelineRevisionSpec{
			PipelineRef: v1.LocalRef{Name: req.Pipeline},
			Image:       image,
			Queue:       req.Pipeline + "-" + short,
		},
	}

	if err := revision.Spec.Validate(); err != nil {
		return nil, err
	}

	if err := req.Kube.Create(ctx, revision); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("ревизия не записалась: %w", err)
	}

	pipeline.Status.LatestRevision = revision.Name
	if err := req.Kube.Status().Update(ctx, pipeline); err != nil {
		return nil, fmt.Errorf("пайплайн не обновился: %w", err)
	}

	return revision, nil
}

// ensurePipeline makes the pipeline record exist and returns it.
func ensurePipeline(ctx context.Context, req PushRequest) (*v1.Pipeline, error) {
	pipeline := &v1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: req.Pipeline, Namespace: req.Namespace},
	}

	err := req.Kube.Create(ctx, pipeline)
	if err == nil {
		return pipeline, nil
	}

	if !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("пайплайн не записался: %w", err)
	}

	key := client.ObjectKey{Namespace: req.Namespace, Name: req.Pipeline}
	if err := req.Kube.Get(ctx, key, pipeline); err != nil {
		return nil, fmt.Errorf("пайплайн не читается: %w", err)
	}

	return pipeline, nil
}

// shortDigest takes the readable head of the image's digest.
func shortDigest(image string) (string, error) {
	_, sum, found := strings.Cut(image, "@sha256:")
	if !found || len(sum) < revisionSuffix {
		return "", fmt.Errorf("%w: %q", ErrNotDigest, image)
	}

	return sum[:revisionSuffix], nil
}

// StartRequest is one run.
type StartRequest struct {
	Kube      client.Client
	Namespace string
	Pipeline  string
	// Revision pins the run to a specific one. Empty means the latest.
	Revision string
	Params   []byte
}

// Start asks for a run of the pipeline's latest revision.
//
// The run points at a revision and not at the pipeline, because a run is
// nailed to concrete code: otherwise "repeat this run" would mean "execute
// whatever is there now".
func Start(ctx context.Context, req StartRequest) (*v1.Run, error) {
	revision := req.Revision

	if revision == "" {
		var pipeline v1.Pipeline

		key := client.ObjectKey{Namespace: req.Namespace, Name: req.Pipeline}
		if err := req.Kube.Get(ctx, key, &pipeline); err != nil {
			return nil, fmt.Errorf("пайплайн не читается: %w", err)
		}

		revision = pipeline.Status.LatestRevision
		if revision == "" {
			return nil, fmt.Errorf("%w: %s", ErrNoRevision, req.Pipeline)
		}
	}

	run := &v1.Run{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: req.Pipeline + "-",
			Namespace:    req.Namespace,
		},
		Spec: v1.RunSpec{RevisionRef: v1.LocalRef{Name: revision}},
	}

	if len(req.Params) > 0 {
		run.Spec.Params = &apiextensionsv1.JSON{Raw: req.Params}
	}

	// Checked here rather than in the cluster: the person who wrote these
	// parameters is still looking at their terminal.
	if err := run.Spec.Validate(); err != nil {
		return nil, err
	}

	if err := req.Kube.Create(ctx, run); err != nil {
		return nil, fmt.Errorf("прогон не записался: %w", err)
	}

	return run, nil
}
