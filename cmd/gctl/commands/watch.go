package commands

import (
	"errors"
	"fmt"
	"io"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
	"github.com/spf13/cobra"
)

// watchCommand follows what changes.
//
// IT DELIVERS NO SNAPSHOT, and neither does this: what is printed is what
// happened after the command started. Somebody who wants both runs get
// first, which is the same three calls anything reconciling makes —
// revision, list, watch — and is the kernel's model rather than a gap.
func watchCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "watch <kind> [path]",
		Short: "Follow resources as they change",
		Long: "Print each change under a path as it happens. Nothing is " +
			"printed for what is already there: run get for that.",
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

			// From now, which is what a person watching a terminal means.
			// The revision is taken first so that nothing between the two
			// calls is missed.
			now, err := on.Calls().Revision(ctx, &graphenepbv1.RevisionRequest{})
			if err != nil {
				return err
			}

			following, err := on.Calls().Watch(ctx, &graphenepbv1.WatchRequest{
				Prefix: at,
				After:  now.GetRevision(),
			})
			if err != nil {
				return err
			}

			out := command.OutOrStdout()

			for {
				event, err := following.Recv()

				switch {
				case errors.Is(err, io.EOF):
					return nil
				case err != nil:
					return err
				}

				record := event.GetEvent().GetRecord()

				_, _ = fmt.Fprintf(out, "%-8s /%s\t%d\n",
					happened(event.GetEvent().GetKind()),
					pathOf(record.GetResource().GetId()),
					record.GetRevision())
			}
		},
	}
}

// happened is the word for what a change was.
func happened(kind graphenepbv1.EventKind) string {
	switch kind {
	case graphenepbv1.EventKind_EVENT_KIND_PUT:
		return "put"
	case graphenepbv1.EventKind_EVENT_KIND_DELETE:
		return "deleted"
	default:
		return "?"
	}
}
