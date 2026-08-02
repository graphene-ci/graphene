// Package cmdflags reads cobra flags without the ceremony.
//
// Every command used to repeat the same block — get the value, wrap the
// error, name the flag — a dozen times. The error a flag lookup returns is
// always a programming mistake (a flag that was never declared), never
// something an operator can fix, so it is worth exactly one line.
package cmdflags

import (
	"fmt"

	"github.com/spf13/cobra"
)

// String reads a string flag.
func String(command *cobra.Command, name string) (string, error) {
	value, err := command.Flags().GetString(name)
	if err != nil {
		return "", fmt.Errorf("read --%s: %w", name, err)
	}

	return value, nil
}

// Bool reads a boolean flag.
func Bool(command *cobra.Command, name string) (bool, error) {
	value, err := command.Flags().GetBool(name)
	if err != nil {
		return false, fmt.Errorf("read --%s: %w", name, err)
	}

	return value, nil
}

// Uint64 reads a uint64 flag.
func Uint64(command *cobra.Command, name string) (uint64, error) {
	value, err := command.Flags().GetUint64(name)
	if err != nil {
		return 0, fmt.Errorf("read --%s: %w", name, err)
	}

	return value, nil
}

// StringSlice reads a repeatable string flag.
func StringSlice(command *cobra.Command, name string) ([]string, error) {
	value, err := command.Flags().GetStringSlice(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}

	return value, nil
}

// Strings reads several string flags at once, in declaration order.
// A single error ends the read: the caller has nothing useful to do with
// a partially filled set.
func Strings(command *cobra.Command, names ...string) ([]string, error) {
	out := make([]string, 0, len(names))

	for _, name := range names {
		value, err := String(command, name)
		if err != nil {
			return nil, err
		}

		out = append(out, value)
	}

	return out, nil
}

// RegisterCompletion attaches a completion function, panicking on the only
// way it can fail: naming a flag that does not exist.
func RegisterCompletion(
	command *cobra.Command,
	name string,
	fn func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective),
) {
	if err := command.RegisterFlagCompletionFunc(name, fn); err != nil {
		panic(fmt.Sprintf("cmdflags: completion for --%s: %v", name, err))
	}
}
