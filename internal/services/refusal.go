package services

import (
	"errors"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refusalDomain marks the stable refusal codes a record answers with.
const refusalDomain = "graphene.io"

// refusalStatus answers a record's refusal as failed_precondition and
// carries its stable code ("busy", "dirty", "stand_leased"...) as an
// ErrorInfo reason, so a caller branches on the code, never the text.
func refusalStatus(err error) error {
	st := status.New(codes.FailedPrecondition, err.Error())
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() == "" {
		return st.Err()
	}
	detailed, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: app.Type(), Domain: refusalDomain})
	if derr != nil {
		return st.Err()
	}
	return detailed.Err()
}
