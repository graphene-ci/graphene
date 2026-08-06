package health_test

import (
	"context"
	"testing"

	"github.com/gopherex/xprobe/pkg/probe"

	"github.com/graphene-ci/graphene/internal/app/health"
	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store/kv/memory"
)

// The probe reads THROUGH the store, and that is the whole of its value.
//
// A probe that answered from a field, or from "the process is running",
// would say Up for exactly the failure a health check exists to catch: a
// kernel that is alive as a process and gone as a service.
func TestTheProbeFollowsTheStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	bytes := memory.New()
	asked := health.Probe(kernel.New(bytes))

	if status := asked.Check(ctx); status != probe.StatusUp {
		t.Fatalf("a working kernel reported %s", status)
	}

	if err := bytes.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if status := asked.Check(ctx); status != probe.StatusDown {
		t.Fatalf("a kernel with no store reported %s", status)
	}
}
