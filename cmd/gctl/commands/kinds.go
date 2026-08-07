package commands

import (
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

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
