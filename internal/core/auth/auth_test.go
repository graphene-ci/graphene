package auth_test

import (
	"context"
	"errors"
	"testing"

	schemapb "github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/core/auth"
)

func execution(placement string, path ...string) *graphenepbv1.Resource {
	return &graphenepbv1.Resource{
		Key:  &graphenepbv1.Key{Kind: "Execution", Path: path},
		Spec: schemapb.MustStructFromGo(map[string]any{"placement": placement}),
	}
}

func kernelCtx(name string) context.Context {
	return auth.WithCredentials(context.Background(), auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalKernel, Name: name},
		Grants: []auth.Grant{
			{
				Verbs: []auth.Verb{auth.VerbGet, auth.VerbWatch},
				Kind:  "Execution",
				Where: []auth.Constraint{{Path: "spec.placement", Equal: "${principal.name}"}},
			},
			{
				Verbs: []auth.Verb{auth.VerbPut},
				Kind:  "Execution",
				Where: []auth.Constraint{{Path: "spec.placement", Equal: "${principal.name}"}},
				Parts: []auth.Part{auth.PartStatus},
			},
		},
	})
}

func TestWhereBindsPrincipal(t *testing.T) {
	t.Parallel()

	ctx := kernelCtx("k1")
	mine := execution("k1", "acme", "prod", "wf", "1", "build", "1")
	foreign := execution("k2", "acme", "prod", "wf", "1", "test", "1")

	if err := auth.CheckRead(ctx, "Execution", mine.GetKey().GetPath(), mine); err != nil {
		t.Fatalf("own execution denied: %v", err)
	}

	if err := auth.CheckRead(ctx, "Execution", foreign.GetKey().GetPath(), foreign); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("foreign execution: want ErrDenied, got %v", err)
	}
}

func TestPartsRestrictWrites(t *testing.T) {
	t.Parallel()

	ctx := kernelCtx("k1")
	mine := execution("k1", "acme", "prod", "wf", "1", "build", "1")

	if err := auth.CheckWrite(ctx, "Execution", mine.GetKey().GetPath(), []auth.Part{auth.PartStatus}, mine); err != nil {
		t.Fatalf("status write denied: %v", err)
	}

	if err := auth.CheckWrite(ctx, "Execution", mine.GetKey().GetPath(), []auth.Part{auth.PartSpec}, mine); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("spec write: want ErrDenied, got %v", err)
	}
}

func TestFilterUnionsGrants(t *testing.T) {
	t.Parallel()

	ctx := kernelCtx("k1")

	allowed, err := auth.Filter(ctx, auth.VerbWatch, "Execution", nil)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	if !allowed(execution("k1", "a", "b")) {
		t.Fatal("own execution filtered out")
	}

	if allowed(execution("k2", "a", "b")) {
		t.Fatal("foreign execution passed the mandatory filter")
	}

	if _, err := auth.Filter(ctx, auth.VerbDelete, "Execution", nil); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("uncovered verb: want ErrDenied, got %v", err)
	}

	if _, err := auth.Filter(context.Background(), auth.VerbWatch, "Execution", nil); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("unauthenticated: want ErrDenied, got %v", err)
	}
}

func TestPathPrefixInterpolation(t *testing.T) {
	t.Parallel()

	ctx := auth.WithCredentials(context.Background(), auth.Credentials{
		Principal: auth.Principal{Kind: auth.PrincipalKernel, Name: "k1"},
		Grants: []auth.Grant{{
			Verbs:      []auth.Verb{auth.VerbPut},
			Kind:       "KernelLease",
			PathPrefix: []string{"acme", "${principal.name}"},
		}},
	})

	lease := &graphenepbv1.Resource{Key: &graphenepbv1.Key{Kind: "KernelLease", Path: []string{"acme", "k1"}}}
	if err := auth.CheckWrite(ctx, "KernelLease", lease.GetKey().GetPath(), []auth.Part{auth.PartSpec}, lease); err != nil {
		t.Fatalf("own lease denied: %v", err)
	}

	foreign := []string{"acme", "k2"}
	if err := auth.CheckWrite(ctx, "KernelLease", foreign, []auth.Part{auth.PartSpec}, lease); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("foreign lease: want ErrDenied, got %v", err)
	}
}
