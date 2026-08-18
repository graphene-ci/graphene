// Package logging builds the server's logger from configuration —
// every component logs through xlog, including the Temporal SDK via the
// adapter below.
package logging

import (
	"fmt"

	"github.com/gopherex/xlog"
	temporallog "go.temporal.io/sdk/log"
)

// New builds the configured logger.
func New(level, format string) (*xlog.Logger, error) {
	parsed, err := xlog.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("log level: %w", err)
	}
	opts := []xlog.Option{
		xlog.WithLevel(parsed),
		xlog.WithFields(xlog.String("service", "graphene-server")),
	}
	switch format {
	case "console":
		return xlog.NewConsole(opts...), nil
	default:
		return xlog.NewJSON(opts...), nil
	}
}

// Temporal adapts xlog to the Temporal SDK's logger contract.
func Temporal(l *xlog.Logger) temporallog.Logger {
	return temporalAdapter{l: l.With(xlog.String("component", "temporal-sdk"))}
}

type temporalAdapter struct {
	l *xlog.Logger
}

func (a temporalAdapter) Debug(msg string, keyvals ...any) { a.l.Debug(msg, fields(keyvals)...) }
func (a temporalAdapter) Info(msg string, keyvals ...any)  { a.l.Info(msg, fields(keyvals)...) }
func (a temporalAdapter) Warn(msg string, keyvals ...any)  { a.l.Warn(msg, fields(keyvals)...) }
func (a temporalAdapter) Error(msg string, keyvals ...any) { a.l.Error(msg, fields(keyvals)...) }

// fields folds the SDK's loose key-value pairs into typed fields.
func fields(keyvals []any) []xlog.Field {
	out := make([]xlog.Field, 0, len(keyvals)/2)
	for i := 0; i+1 < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			key = fmt.Sprint(keyvals[i])
		}
		out = append(out, xlog.Any(key, keyvals[i+1]))
	}
	return out
}
