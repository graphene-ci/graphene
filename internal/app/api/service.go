package api

import (
	"context"

	"github.com/gopherex/xlog"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/kernel"
)

// Service answers for a kernel.
//
// It holds the guard AND the unguarded kernel, and the second one is used
// for exactly one thing: working out who a caller is. That cannot go
// through the guard, because asking the guard anything means already
// knowing who is asking.
type Service struct {
	graphenepbv1.UnimplementedKernelServiceServer

	guard  auth.Guard
	kernel kernel.Kernel
	log    *xlog.Logger
}

// New builds the service over a kernel.
func New(guard auth.Guard, unguarded kernel.Kernel, log *xlog.Logger) *Service {
	return &Service{guard: guard, kernel: unguarded, log: log}
}

// session works out who is calling and binds the guard to them.
//
// Every handler starts here and none of them may skip it: a handler that
// reached for the kernel instead would be a handler nobody authorised,
// and it would look exactly like the others.
func (s *Service) session(ctx context.Context) (auth.Session, error) {
	who, err := s.identify(ctx)
	if err != nil {
		return auth.Session{}, s.fail(err)
	}

	return s.guard.As(who), nil
}

// fail turns what the kernel said into what a caller is told, logging
// what a caller is not.
func (s *Service) fail(err error) error {
	return fail(err, func(unexpected error) {
		s.log.Error("unexpected failure", xlog.Err(unexpected))
	})
}
