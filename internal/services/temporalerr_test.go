package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NotFound is what the service SAID; every other answer of the transport
// keeps its own meaning, and no answer at all is Unavailable — a client
// must never read "unknown" as "absent" and start again.
func TestTemporalStatusKeepsTheMeaning(t *testing.T) {
	cases := map[string]struct {
		err      error
		fallback codes.Code
		want     codes.Code
	}{
		"not found":         {serviceerror.NewNotFound("no such workflow"), codes.Unavailable, codes.NotFound},
		"unavailable":       {serviceerror.NewUnavailable("frontend down"), codes.Unavailable, codes.Unavailable},
		"deadline":          {serviceerror.NewDeadlineExceeded("slow"), codes.Unavailable, codes.DeadlineExceeded},
		"canceled":          {serviceerror.NewCanceled("gone"), codes.Unavailable, codes.Canceled},
		"permission":        {serviceerror.NewPermissionDenied("no", ""), codes.Unavailable, codes.PermissionDenied},
		"already started":   {serviceerror.NewWorkflowExecutionAlreadyStarted("taken", "", ""), codes.Unavailable, codes.AlreadyExists},
		"wrapped not found": {fmt.Errorf("describe: %w", serviceerror.NewNotFound("x")), codes.Unavailable, codes.NotFound},
		"ctx deadline":      {context.DeadlineExceeded, codes.Unavailable, codes.DeadlineExceeded},
		"ctx canceled":      {context.Canceled, codes.Unavailable, codes.Canceled},
		"a status already":  {status.Error(codes.InvalidArgument, "bad"), codes.Unavailable, codes.InvalidArgument},
		"silence":           {errors.New("connection refused"), codes.Unavailable, codes.Unavailable},
		"own refusal":       {errors.New("workflow failed: boom"), codes.FailedPrecondition, codes.FailedPrecondition},
	}
	for name, c := range cases {
		got := temporalStatus(c.err, c.fallback)
		require.Equal(t, c.want, status.Code(got), name)
		require.Contains(t, got.Error(), c.err.Error(), name)
	}
	require.NoError(t, temporalStatus(nil, codes.Unavailable))
}
