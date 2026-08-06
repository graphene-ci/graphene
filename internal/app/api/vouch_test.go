package api_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/convert"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/process"
)

// A process holds no credential, so the kernel that started it says who
// it is. These tests are about the ONE sentence that makes that safe: a
// Process is addressed /{kernel}/{name}, so a kernel can only ever name a
// process under its own name.

// vouchingFor puts the acting-for claim on a call.
func vouchingFor(ctx context.Context, named string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "graphene-acting-for", named)
}

// world stands the stack up with the real Process kind published and one
// process written down.
func vouching(t *testing.T) (graphenepbv1.KernelServiceClient, kernel.Kernel) {
	t.Helper()

	client, k := dial(t)

	if _, err := k.Define(context.Background(), process.Definition()); err != nil {
		t.Fatalf("define process: %v", err)
	}

	return client, k
}

// writeProcess records a process on a kernel, running as an identity.
func writeProcess(t *testing.T, k kernel.Kernel, on, named, identity string) {
	t.Helper()

	id, err := process.Id(on, named)
	if err != nil {
		t.Fatalf("process id: %v", err)
	}

	write(t, k, id, map[string]any{
		"blob":     "0123456789abcdef0123456789abcdef",
		"format":   process.RawExec,
		"identity": identity,
	})
}

func processId(t *testing.T, on, named string) *graphenepbv1.Id {
	t.Helper()

	id, err := process.Id(on, named)
	if err != nil {
		t.Fatalf("process id: %v", err)
	}

	return convert.IdToPb(id)
}

// The plain case: a kernel signs as itself, names one of its processes,
// and is answered as the identity that process's record gives it.
//
// The kernel itself is granted NOTHING, which is what makes the test
// about the vouch: the same call without the claim is refused.
func TestAKernelIsAnsweredAsTheProcessItVouchesFor(t *testing.T) {
	t.Parallel()

	client, k := vouching(t)
	ctx := context.Background()

	grant(t, k, "k1")
	grant(t, k, "watcher", rule("get", process.Kind.String(), "k1"))
	writeProcess(t, k, "k1", "probe", "watcher")

	asked := &graphenepbv1.GetRequest{Id: processId(t, "k1", "probe")}

	if _, err := client.Get(vouchingFor(as(ctx, "k1"), "probe"), asked); err != nil {
		t.Fatalf("vouched call: %v", err)
	}

	if _, err := client.Get(as(ctx, "k1"), asked); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the kernel itself: got %v, want PermissionDenied", err)
	}
}

// The check that matters. Two kernels, each with a process of the same
// name, and neither can name the other's — because the record a claim is
// checked against is the one under the CALLER'S name, and there is no
// other one to reach for.
func TestAKernelCannotVouchForAnotherKernelsProcess(t *testing.T) {
	t.Parallel()

	client, k := vouching(t)
	ctx := context.Background()

	grant(t, k, "k1")
	grant(t, k, "k2")
	grant(t, k, "privileged", rule("get", process.Kind.String(), ""))
	grant(t, k, "ordinary", rule("get", process.Kind.String(), "k2"))

	writeProcess(t, k, "k1", "probe", "privileged")
	writeProcess(t, k, "k2", "probe", "ordinary")

	// k2 names "probe" and gets ITS OWN probe — the ordinary identity,
	// which may not read what is on k1.
	elsewhere := &graphenepbv1.GetRequest{Id: processId(t, "k1", "probe")}

	_, err := client.Get(vouchingFor(as(ctx, "k2"), "probe"), elsewhere)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reaching across kernels: got %v, want PermissionDenied", err)
	}

	// It is answered as its own probe's identity, which may read what is
	// on k2 — the claim was not refused, it was resolved somewhere else.
	if _, err := client.Get(vouchingFor(as(ctx, "k2"), "probe"),
		&graphenepbv1.GetRequest{Id: processId(t, "k2", "probe")}); err != nil {
		t.Fatalf("its own process: %v", err)
	}

	// And the same claim from k1 is answered as the privileged identity,
	// which proves the two claims were told apart by the caller's name
	// and not by the word they both sent.
	if _, err := client.Get(vouchingFor(as(ctx, "k1"), "probe"), elsewhere); err != nil {
		t.Fatalf("the kernel that does hold it: %v", err)
	}
}

// A claim about a process nobody recorded is refused. Otherwise a kernel
// could name a process that does not exist and be handed whatever it
// asked to be called.
func TestAVouchForAProcessThatIsNotThereIsRefused(t *testing.T) {
	t.Parallel()

	client, k := vouching(t)
	ctx := context.Background()

	grant(t, k, "k1", rule("get", process.Kind.String(), ""))

	_, err := client.Get(vouchingFor(as(ctx, "k1"), "imaginary"),
		&graphenepbv1.GetRequest{Id: processId(t, "k1", "imaginary")})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a process nobody wrote down: got %v, want PermissionDenied", err)
	}
}

// Vouching is something a caller does, so there has to be a caller. An
// unnamed one asking to be somebody else is asking for authority for
// free, and the whole point of a name is that somebody gave it out.
func TestAnUnnamedCallerMayNotVouch(t *testing.T) {
	t.Parallel()

	client, k := vouching(t)
	ctx := context.Background()

	grant(t, k, "watcher", rule("get", process.Kind.String(), ""))
	writeProcess(t, k, "k1", "probe", "watcher")

	_, err := client.Get(vouchingFor(ctx, "probe"),
		&graphenepbv1.GetRequest{Id: processId(t, "k1", "probe")})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an anonymous vouch: got %v, want Unauthenticated", err)
	}
}

// A process that asked for no identity gets none. The vouch is true — the
// kernel really did start it — and the answer is still nobody, because
// nobody is what its record says it runs as.
func TestAVouchForAProcessWithNoIdentityGrantsNothing(t *testing.T) {
	t.Parallel()

	client, k := vouching(t)
	ctx := context.Background()

	grant(t, k, "k1", rule("get", process.Kind.String(), ""))
	writeProcess(t, k, "k1", "quiet", "")

	_, err := client.Get(vouchingFor(as(ctx, "k1"), "quiet"),
		&graphenepbv1.GetRequest{Id: processId(t, "k1", "quiet")})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("a process running as nobody: got %v, want Unauthenticated", err)
	}
}
