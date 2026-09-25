package services

import (
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/graphene/internal/telemetry"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// A page token names a position exactly and survives the trip.
func TestLogCursorTokenRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 25, 8, 0, 0, 123456789, time.UTC)
	tok := encodeCursor(telemetry.LogCursor{Time: at, Skip: 3})
	require.NotEmpty(t, tok)
	back, err := decodeCursor(tok)
	require.NoError(t, err)
	require.True(t, back.Time.Equal(at))
	require.Equal(t, 3, back.Skip)
	require.Empty(t, encodeCursor(telemetry.LogCursor{}), "no cursor, no token")
	for _, bad := range []string{"zzz", "MTIz", encodeCursor(telemetry.LogCursor{Time: at, Skip: 1})[1:]} {
		if _, err := decodeCursor(bad); err == nil {
			t.Errorf("token %q was accepted", bad)
		}
	}
}

// The wire selection becomes a backend query: filters mapped to the
// attributes the door stamps, the order and limit checked at the door.
func TestLogQueryOfReadsTheSelection(t *testing.T) {
	q, err := logQueryOf(&managementv1.LogsRequest{
		Ref: "run/x", Query: `level:error`, Limit: 20, Order: "desc",
		Severities: []string{"WARN"}, Stream: "stderr", Agent: "db-1", Entity: "docker/pg", Text: "refused",
		SinceUnixNano: 100, UntilUnixNano: 200,
	})
	require.NoError(t, err)
	require.True(t, q.Desc)
	require.Equal(t, 20, q.Limit)
	require.Equal(t, "level:error", q.Filter)
	require.Equal(t, map[string]string{"stream": "stderr", "graphene.agent": "db-1", "graphene.entity": "docker/pg"}, q.Attributes)
	require.Equal(t, int64(100), q.Since.UnixNano())
	require.Equal(t, int64(200), q.Until.UnixNano())

	_, err = logQueryOf(&managementv1.LogsRequest{Ref: "run/x", Order: "sideways"})
	require.ErrorContains(t, err, "order")
	_, err = logQueryOf(&managementv1.LogsRequest{Ref: "run/x", Limit: telemetry.MaxLogLimit + 1})
	require.ErrorContains(t, err, "limit")
	_, err = logQueryOf(&managementv1.LogsRequest{Ref: "run/x", PageToken: "nope"})
	require.ErrorContains(t, err, "page token")
}

// The door tells the query's fault from the backend's from a form the
// backend cannot serve — by code, not by text.
func TestBackendErrorCodes(t *testing.T) {
	cases := map[error]connect.Code{
		&telemetry.BackendError{Backend: "logs", Status: 400}: connect.CodeInvalidArgument,
		&telemetry.BackendError{Backend: "logs", Status: 503}: connect.CodeUnavailable,
		&telemetry.ClientError{Msg: "step too fine"}:          connect.CodeInvalidArgument,
		telemetry.ErrScopedQueryUnsupported:                   connect.CodeUnimplemented,
		errors.New("dial tcp: connection refused"):            connect.CodeUnavailable,
	}
	for err, want := range cases {
		require.Equal(t, want, connect.CodeOf(backendError(err)), "%v", err)
	}
	require.NoError(t, backendError(nil))
}

func TestMetricsQueryOfDefaultsTheWindow(t *testing.T) {
	q := metricsQueryOf(&managementv1.MetricsRequest{Ref: "run/x", StepSeconds: 30})
	require.Equal(t, 30*time.Second, q.Step)
	require.WithinDuration(t, time.Now(), q.End, 2*time.Second)
	require.Equal(t, time.Hour, q.End.Sub(q.Start))
	q = metricsQueryOf(&managementv1.MetricsRequest{StartUnixNano: 1e9, EndUnixNano: 2e9})
	require.Equal(t, int64(1), q.Start.Unix())
	require.Equal(t, int64(2), q.End.Unix())
}

// Follow reads forward from the present with the selection's fields; the
// backend's language cannot be evaluated on live records, so a query is
// refused rather than silently applied to the past alone.
func TestFollowTakesFieldsButNoQuery(t *testing.T) {
	require.NoError(t, followable(false, telemetry.LogQuery{Filter: "level:error", Desc: true}))
	require.NoError(t, followable(true, telemetry.LogQuery{Severities: []string{"error"}, Text: "refused", Attributes: map[string]string{"stream": "stderr"}}))
	require.ErrorContains(t, followable(true, telemetry.LogQuery{Filter: "level:error"}), "no query")
	require.ErrorContains(t, followable(true, telemetry.LogQuery{Desc: true}), "desc")
	require.ErrorContains(t, followable(true, telemetry.LogQuery{Cursor: telemetry.LogCursor{Time: time.Now(), Skip: 1}}), "page token")
}
