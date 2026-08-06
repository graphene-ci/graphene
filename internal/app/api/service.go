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
	who    Identify
	log    *xlog.Logger
}

// Identify answers who is calling.
//
// A field rather than a method because there is more than one honest
// answer to the question, and which one applies is a fact about the
// LISTENER rather than about the service. On a network endpoint a caller
// presents a credential and it is read. On a socket opened for one
// spawned process there is no credential to read and none is wanted: the
// process is whoever that socket was opened for, and the kernel that
// opened it is the one saying so.
type Identify func(ctx context.Context) (auth.Principal, error)

// ByCredential is how a network endpoint answers "who is calling": by
// reading what the caller presented. It is exported because the byte
// service asks the same question and must get the same answer — two ways
// of establishing a caller would be two things to keep in step.
func ByCredential(guard auth.Guard, unguarded kernel.Kernel, log *xlog.Logger) Identify {
	return New(guard, unguarded, log).byCredential
}

// New builds the service over a kernel, reading credentials the usual way.
func New(guard auth.Guard, unguarded kernel.Kernel, log *xlog.Logger) *Service {
	service := &Service{guard: guard, kernel: unguarded, log: log}
	service.who = service.byCredential

	return service
}

// As builds the same service for a caller already known.
//
// Nothing is read from the request, so nothing a caller sends can change
// who they are. That is the whole security property of a door: the
// question is answered before the connection is accepted.
func As(guard auth.Guard, unguarded kernel.Kernel, who auth.Principal, log *xlog.Logger) *Service {
	return &Service{
		guard:  guard,
		kernel: unguarded,
		who:    func(context.Context) (auth.Principal, error) { return who, nil },
		log:    log,
	}
}

// session works out who is calling and binds the guard to them.
//
// Every handler starts here and none of them may skip it: a handler that
// reached for the kernel instead would be a handler nobody authorized,
// and it would look exactly like the others.
func (s *Service) session(ctx context.Context) (auth.Session, error) {
	who, err := s.who(ctx)
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
