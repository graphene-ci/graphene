// Package commands is graphened's surface: everything about running a
// kernel on this machine.
//
// It is deliberately small and deliberately LOCAL. A kernel is a kernel —
// there are no roles and no modes — and this binary is the whole of one.
// What it is not is a client: talking to a kernel somewhere else is
// gctl's job, and the two are separate binaries because they are separate
// programs. This one holds a file, listens on a port, runs for years and
// starts as a service; that one runs for a second from somebody's laptop
// and has no business touching the file.
package commands

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// version is what this build calls itself, stamped at link time.
var version = "dev"

// The two things that cannot come from the kernel's own store, because
// they are what finding the store depends on.
var (
	storePath string
	name      string
)

// Root is graphened.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:           "graphened",
		Short:         "A graphene kernel",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}

	root.PersistentFlags().StringVar(&storePath, "store", defaultStore(),
		"where the kernel keeps everything")
	root.PersistentFlags().StringVar(&name, "name", defaultName(),
		"which kernel this is")

	root.AddCommand(runCommand())

	return root
}

// Execute runs graphened and reports whether it failed.
func Execute() int {
	if err := Root().Execute(); err != nil {
		_, _ = os.Stderr.WriteString("graphened: " + err.Error() + "\n")

		return 1
	}

	return 0
}

// defaultStore is where a kernel keeps its file when nobody says
// otherwise: under the user's state directory, because a kernel installed
// as a user service has no business writing anywhere else.
func defaultStore() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "state", "graphene", "kernel.db")
	}

	return "kernel.db"
}

// defaultName is what a kernel calls itself when nobody says otherwise.
//
// The hostname, because a kernel's name is how OTHER kernels address it,
// and the machine's own name is the one answer that is already agreed on.
func defaultName() string {
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}

	return "local"
}
