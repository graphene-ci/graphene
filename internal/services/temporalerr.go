package services

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// temporalStatus maps an error of the Temporal client onto the API's own
// codes, keeping what a caller needs to recover: NotFound only when the
// service SAID the workflow does not exist; Unavailable, DeadlineExceeded,
// Canceled, PermissionDenied and the rest keep their meaning. An error
// with no code at all — a connection that never answered — is
// Unavailable: the answer is unknown, and "unknown" must never read as
// "absent", or a client retries a start it believes was never accepted.
// fallback is the code for an error that is the operation's own refusal
// rather than the transport's (a workflow's failure, a rejected command).
func temporalStatus(err error, fallback codes.Code) error {
	if err == nil {
		return nil
	}
	var se serviceerror.ServiceError
	if errors.As(err, &se) {
		return status.Error(se.Status().Code(), err.Error())
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
		return err
	}
	return status.Error(fallback, err.Error())
}
