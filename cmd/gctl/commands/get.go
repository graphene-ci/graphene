package commands

import (
	"errors"
	"fmt"
	"io"
	"text/tabwriter"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"github.com/spf13/cobra"
)

// getCommand reads resources.
//
// One command for one thing and for many, because they are one question
// asked with more or less of a path — which is the kernel's own model and
// not a convenience: a prefix names a subtree, and a whole path names the
// one resource in it.
func getCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get <kind> [path]",
		Short: "Read resources",
		Long: "Read one resource, or everything under a path, or every " +
			"instance of a kind. The path is written the way it reads: " +
			"local/one.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(command *cobra.Command, args []string) error {
			on, err := reached(command)
			if err != nil {
				return err
			}

			ctx := calling(command, on)

			at, err := addressed(ctx, on, args[0], written(args))
			if err != nil {
				return err
			}

			listing, err := on.Calls().List(ctx, &graphenepbv1.ListRequest{Prefix: at})
			if err != nil {
				return err
			}

			return tabulated(command.OutOrStdout(), func(write func(...string)) error {
				write("PATH", "REVISION", "STATUS")

				found := 0

				for {
					answer, err := listing.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					if err != nil {
						return err
					}

					record := answer.GetRecord()
					write(
						"/"+pathOf(record.GetResource().GetId()),
						fmt.Sprint(record.GetRevision()),
						condition(record),
					)

					found++
				}

				if found == 0 {
					// A person who asked for something and got nothing is
					// owed the difference between "none" and "broken".
					_, _ = fmt.Fprintf(command.ErrOrStderr(),
						"no %s under /%s\n", args[0], written(args))
				}

				return nil
			})
		},
	}
}

// written is the path a person typed, or none.
func written(args []string) string {
	if len(args) < 2 {
		return ""
	}

	return args[1]
}

// condition is the short answer to "what is this doing", which is the
// only reason a listing is worth reading at a glance.
func condition(record *graphenepbv1.Record) string {
	resource := record.GetResource()

	switch {
	case resource.GetDeleting():
		return "deleting"
	case resource.GetGeneration() == 0:
		return "-"
	case len(resource.GetStatus().GetFields()) == 0:
		return "unreported"
	default:
		return "reported"
	}
}

// tabulated writes columns that line up.
func tabulated(out io.Writer, rows func(write func(...string)) error) error {
	table := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)

	err := rows(func(cells ...string) {
		for at, cell := range cells {
			if at > 0 {
				_, _ = io.WriteString(table, "\t")
			}

			_, _ = io.WriteString(table, cell)
		}

		_, _ = io.WriteString(table, "\n")
	})
	if err != nil {
		return err
	}

	return table.Flush()
}
