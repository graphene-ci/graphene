package observecmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

// Window is the optional historical metrics interval in Unix nanoseconds.
// Zero values retain the server defaults: end now, start one hour before end.
type Window struct{ Start, End int64 }

// BindWindowFlags adds the historical metrics interval to either CLI grammar.
func BindWindowFlags(cmd *cobra.Command) {
	cmd.Flags().String("start", "", "metrics range start (RFC3339)")
	cmd.Flags().String("end", "", "metrics range end (RFC3339)")
}

// ReadWindow validates explicit timestamps before opening an API connection.
func ReadWindow(cmd *cobra.Command, dim string) (Window, error) {
	var window Window
	for _, field := range []struct {
		name  string
		value *int64
	}{{"start", &window.Start}, {"end", &window.End}} {
		flag := cmd.Flags().Lookup(field.name)
		if flag == nil || !flag.Changed {
			continue
		}
		if dim != "metrics" {
			return Window{}, fmt.Errorf("--%s is only supported for metrics", field.name)
		}
		value, err := time.Parse(time.RFC3339Nano, flag.Value.String())
		if err != nil {
			return Window{}, fmt.Errorf("--%s must be an RFC3339 timestamp: %w", field.name, err)
		}
		nanos := value.UnixNano()
		if nanos <= 0 || !time.Unix(0, nanos).Equal(value) {
			return Window{}, fmt.Errorf("--%s must fit a positive Unix nanosecond timestamp", field.name)
		}
		*field.value = nanos
	}
	if window.Start > 0 && window.End > 0 && window.Start >= window.End {
		return Window{}, fmt.Errorf("--start must be before --end")
	}
	return window, nil
}
