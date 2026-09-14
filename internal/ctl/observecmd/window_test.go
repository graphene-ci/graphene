package observecmd

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestMetricsWindow(t *testing.T) {
	for _, tc := range []struct{ name, dim, start, end, wantError string }{
		{"defaults", "metrics", "", "", ""},
		{"explicit offset", "metrics", "2026-09-14T18:47:19.588089402+03:00", "2026-09-14T15:49:22Z", ""},
		{"only end", "metrics", "", "2026-09-14T15:49:22Z", ""},
		{"invalid start", "metrics", "yesterday", "", "RFC3339"},
		{"inverted", "metrics", "2026-09-14T16:00:00Z", "2026-09-14T15:00:00Z", "before"},
		{"equal", "metrics", "2026-09-14T16:00:00Z", "2026-09-14T16:00:00Z", "before"},
		{"zero", "metrics", "1970-01-01T00:00:00Z", "", "positive"},
		{"overflow", "metrics", "2500-01-01T00:00:00Z", "", "positive"},
		{"unsupported dimension", "logs", "2026-09-14T16:00:00Z", "", "only supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			BindWindowFlags(cmd)
			for _, field := range []struct{ name, value string }{{"start", tc.start}, {"end", tc.end}} {
				if field.value != "" {
					if err := cmd.Flags().Set(field.name, field.value); err != nil {
						t.Fatal(err)
					}
				}
			}
			window, err := ReadWindow(cmd, tc.dim)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("got %v; want %s", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []struct {
				input string
				got   int64
			}{{tc.start, window.Start}, {tc.end, window.End}} {
				var want int64
				if field.input != "" {
					value, e := time.Parse(time.RFC3339Nano, field.input)
					if e != nil {
						t.Fatal(e)
					}
					want = value.UnixNano()
				}
				if field.got != want {
					t.Fatalf("got %d, want %d", field.got, want)
				}
			}
		})
	}
}
