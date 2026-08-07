package commands

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// undefineCommand removes a kind.
//
// It is under `kinds` and not beside `delete` because the two are
// different in the way that matters: deleting a resource takes away one
// thing, and undefining a kind takes away the possibility of that thing.
// The kernel refuses while any instance is left, so this cannot be the
// accident it reads like — but the words are worth putting where somebody
// looking for them will read them first.
func undefineCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "undefine <kind>",
		Short: "Remove a kind and every version of it",
		Long: "Take a kind away. The kernel refuses while any instance of " +
			"it is left, so this cannot quietly orphan anything: delete " +
			"the instances first, and the kind after.",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			on, err := reached()
			if err != nil {
				return err
			}

			if _, err := on.Calls().Undefine(calling(command, on),
				&graphenepbv1.UndefineRequest{Kind: args[0]}); err != nil {
				return err
			}

			_, _ = fmt.Fprintf(command.OutOrStdout(), "%s is gone\n", args[0])

			return nil
		},
	}
}

// kindsCommand lists what a kernel knows how to hold.
//
// The PATH column is a shape and not an address: it says what an instance
// of this kind is addressed by, which is what somebody needs before they
// can address one. Written as placeholders so it cannot be mistaken for a
// resource that exists.
func kindsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "kinds",
		Short: "List the kinds this kernel has been told about",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			on, err := reached()
			if err != nil {
				return err
			}

			ctx := calling(command, on)

			listing, err := on.Calls().ListKinds(ctx, &graphenepbv1.ListKindsRequest{})
			if err != nil {
				return err
			}

			return tabulated(command.OutOrStdout(), func(write func(...string)) error {
				write("KIND", "VERSION", "PATH")

				for {
					answer, err := listing.Recv()
					if errors.Is(err, io.EOF) {
						return nil
					}

					if err != nil {
						return err
					}

					definition := answer.GetDefinition()
					write(
						definition.GetKind(),
						strconv.FormatUint(uint64(definition.GetVersion()), 10),
						placeholders(definition.GetShape()),
					)
				}
			})
		},
	}
}

// placeholders writes a shape so it cannot be read as a path: /<kernel>/<name>.
func placeholders(shape []string) string {
	if len(shape) == 0 {
		return "/"
	}

	written := make([]string, 0, len(shape))

	for _, name := range shape {
		written = append(written, "<"+name+">")
	}

	return "/" + strings.Join(written, "/")
}
