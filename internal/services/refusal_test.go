package services

import (
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A record's refusal reaches API callers as failed_precondition with its
// stable code in an ErrorInfo — over gRPC and after the connect
// translation alike.
func TestRefusalStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		code string
	}{
		{name: "busy", err: temporal.NewNonRetryableApplicationError("leased by r1", "busy", nil), code: "busy"},
		{name: "wrapped dirty", err: fmt.Errorf("acquire: %w", temporal.NewApplicationError("leaks", "dirty")), code: "dirty"},
		{name: "plain", err: errors.New("boom")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := refusalStatus(c.err)
			st, _ := status.FromError(err)
			if st.Code() != codes.FailedPrecondition {
				t.Fatalf("code %s", st.Code())
			}
			reason := ""
			for _, d := range st.Proto().GetDetails() {
				var info errdetails.ErrorInfo
				if d.UnmarshalTo(&info) == nil {
					reason = info.GetReason()
				}
			}
			if reason != c.code {
				t.Fatalf("gRPC reason %q, want %q", reason, c.code)
			}
			ce := asConnectError(err)
			if connect.CodeOf(ce) != connect.CodeFailedPrecondition {
				t.Fatalf("connect code %s", connect.CodeOf(ce))
			}
			var cerr *connect.Error
			if !errors.As(ce, &cerr) {
				t.Fatalf("not a connect error: %T", ce)
			}
			reason = ""
			for _, d := range cerr.Details() {
				var info errdetails.ErrorInfo
				if d.Type() == string(info.ProtoReflect().Descriptor().FullName()) && proto.Unmarshal(d.Bytes(), &info) == nil {
					reason = info.GetReason()
				}
			}
			if reason != c.code {
				t.Fatalf("connect reason %q, want %q", reason, c.code)
			}
		})
	}
}
