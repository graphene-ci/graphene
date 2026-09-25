package observecmd

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func optionsFor(t *testing.T, dim string, args ...string) (Options, error) {
	t.Helper()
	cmd := &cobra.Command{Use: dim}
	BindFlags(cmd, dim)
	require.NoError(t, cmd.ParseFlags(args))
	return ReadOptions(cmd, dim)
}

// A moment is an RFC3339 timestamp or a duration ago; the window must be
// ordered; the step is a duration of at least a second.
func TestReadOptionsWindowAndStep(t *testing.T) {
	t.Parallel()
	o, err := optionsFor(t, "metrics", "--start", "2026-09-25T08:00:00Z", "--end", "2026-09-25T09:00:00Z", "--step", "30s")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC).UnixNano(), o.Start)
	require.Equal(t, int32(30), o.Step)

	o, err = optionsFor(t, "logs", "--start", "-2h")
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(-2*time.Hour), time.Unix(0, o.Start), 5*time.Second)

	_, err = optionsFor(t, "metrics", "--start", "2026-09-25T09:00:00Z", "--end", "2026-09-25T08:00:00Z")
	require.ErrorContains(t, err, "before")
	_, err = optionsFor(t, "metrics", "--step", "500ms")
	require.ErrorContains(t, err, "at least 1s")
	_, err = optionsFor(t, "metrics", "--start", "yesterday")
	require.ErrorContains(t, err, "RFC3339")
}

// The log selection reads every filter; the trace snapshot size shares
// the limit field.
func TestReadOptionsLogSelection(t *testing.T) {
	t.Parallel()
	o, err := optionsFor(t, "logs", "--limit", "50", "--desc", "--severity", "WARN,ERROR", "--stream", "stderr",
		"--agent", "db-1", "--entity", "docker/pg", "--text", "refused", "--page", "abc", "--query", "level:error", "--facets", "severity,job")
	require.NoError(t, err)
	require.Equal(t, int32(50), o.Limit)
	require.True(t, o.Desc)
	require.Equal(t, []string{"WARN", "ERROR"}, o.Severities)
	require.Equal(t, "stderr", o.Stream)
	require.Equal(t, "db-1", o.Agent)
	require.Equal(t, "docker/pg", o.Entity)
	require.Equal(t, "refused", o.Text)
	require.Equal(t, "abc", o.PageToken)
	require.Equal(t, "level:error", o.Query)
	require.Equal(t, []string{"severity", "job"}, o.Facets)

	o, err = optionsFor(t, "trace", "--traces", "5", "--query", "service=stroppy")
	require.NoError(t, err)
	require.Equal(t, int32(5), o.Limit)
	require.Equal(t, "service=stroppy", o.Query)
}

// Events have no window: the flag is not even offered, so a caller cannot
// misuse it.
func TestEventsHaveNoWindowFlags(t *testing.T) {
	t.Parallel()
	cmd := &cobra.Command{Use: "events"}
	BindFlags(cmd, "events")
	require.Nil(t, cmd.Flags().Lookup("start"))
	require.Nil(t, cmd.Flags().Lookup("query"))
}
