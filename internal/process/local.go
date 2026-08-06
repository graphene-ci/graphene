package process

import (
	"context"
	"iter"

	"github.com/gopherex/schemapb/go/schemapb"

	"github.com/graphene-ci/graphene/internal/kernel"
	"github.com/graphene-ci/graphene/internal/store"
	"github.com/graphene-ci/graphene/internal/types/resource"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// Local is the kernel this process holds, as an agent sees it.
//
// The one line of adaptation is Watch: the store hands back a concrete
// stream, and the port asks for an interface so that a kernel across a
// link can answer the same question. Everything else passes straight
// through, which is what makes the two indistinguishable above.
type Local struct{ kernel kernel.Kernel }

// Here wraps a kernel for an agent.
func Here(k kernel.Kernel) Local { return Local{kernel: k} }

func (l Local) Revision(ctx context.Context) (revision.Revision, error) {
	return l.kernel.Revision(ctx)
}

func (l Local) List(
	ctx context.Context, prefix resource.Id,
) iter.Seq2[store.Value[resource.Resource], error] {
	return l.kernel.List(ctx, prefix)
}

func (l Local) Get(ctx context.Context, id resource.Id) (store.Value[resource.Resource], error) {
	return l.kernel.Get(ctx, id)
}

func (l Local) Watch(
	ctx context.Context, prefix resource.Id, after revision.Revision,
) (Stream, error) {
	stream, err := l.kernel.Watch(ctx, prefix, after)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func (l Local) Report(
	ctx context.Context, id resource.Id, status *schemapb.StructValue, expect revision.Revision,
) (revision.Revision, error) {
	return l.kernel.Report(ctx, id, status, expect)
}
