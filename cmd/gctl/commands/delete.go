package commands

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// deleteCommand asks a resource to go away.
//
// ASKS. Whether it goes at once is the resource's answer and not the
// caller's: with claims on it the record stays, marked, until whatever
// placed them lets go. So what is printed is what happened, which is not
// always what was asked for.
// errWholeKind — a delete was given a path shorter than its kind's
// shape, which names a subtree. Removing a subtree is a different act
// with a different blast radius, and it is not this one.
var errWholeKind = errors.New("delete names one resource, not a whole kind")

func deleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <kind> <path>",
		Short: "Delete one resource",
		Long: "Ask a resource to go away. It goes at once if nothing is " +
			"holding it open; otherwise it is marked and goes when the " +
			"last claim is released.",
		Args: cobra.ExactArgs(2),
		RunE: func(command *cobra.Command, args []string) error {
			on, err := reached()
			if err != nil {
				return err
			}

			ctx := calling(command, on)

			at, err := addressed(ctx, on, args[0], args[1])
			if err != nil {
				return err
			}

			if len(at.GetPath()) == 0 {
				return errWholeKind
			}

			// Read first: a delete is a write and carries the same
			// expectation as any other, and the zero one would mean "must
			// not exist yet" — which is never what a delete means.
			expect, err := expectation(ctx, on, at)
			if err != nil {
				return err
			}

			answer, err := on.Calls().Delete(ctx, &graphenepbv1.DeleteRequest{
				Id:     at,
				Expect: expect,
			})
			if err != nil {
				return err
			}

			// WHAT HAPPENED is not in the answer. DeleteResponse carries a
			// revision and nothing else, so "gone" and "marked, waiting
			// for a claim to be released" read the same here — the caller
			// has to Get it again to find out. That is a gap in the API
			// rather than in this command; it wants a `deleting` field
			// the way Put's answer carries what it wrote.
			_, _ = fmt.Fprintf(command.OutOrStdout(), "deleted /%s at %d\n",
				args[1], answer.GetRevision())

			return nil
		},
	}
}
