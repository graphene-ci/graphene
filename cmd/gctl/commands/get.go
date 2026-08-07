package commands

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	"github.com/graphene-ci/graphene/internal/app/report"
	"github.com/graphene-ci/graphene/internal/convert"
)

// getCommand reads resources.
//
// One command for one thing and for many, because they are one question
// asked with more or less of a path — which is the kernel's own model and
// not a convenience: a prefix names a subtree, and a whole path names the
// one resource in it.
func getCommand() *cobra.Command {
	var whole bool

	command := &cobra.Command{
		Use:   "get <kind> [path]",
		Short: "Read resources",
		Long: "Read one resource, or everything under a path, or every " +
			"instance of a kind. The path is written the way it reads: " +
			"local/one.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(command *cobra.Command, args []string) error {
			on, err := reached()
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

			if whole {
				return printed(command.OutOrStdout(), listing)
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
						strconv.FormatUint(record.GetRevision(), 10),
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

	command.Flags().BoolVarP(&whole, "output", "o", false,
		"print each record whole — spec and status — instead of a listing")

	return command
}

// printed writes the records out whole.
//
// A listing answers "what is there"; this answers "what does it SAY",
// which is the question anybody debugging a controller has. Without it a
// status written by something else can only be inferred from a column
// reading "reported", which is the least useful true thing to say about
// it.
func printed(out io.Writer, listing graphenepbv1.KernelService_ListClient) error {
	for {
		answer, err := listing.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		stored := answer.GetRecord()

		written, err := yaml.Marshal(map[string]any{
			"kind":     stored.GetResource().GetId().GetKind(),
			"path":     "/" + pathOf(stored.GetResource().GetId()),
			"revision": stored.GetRevision(),
			"spec":     plain(stored.GetResource().GetSpec()),
			"status":   plain(stored.GetResource().GetStatus()),
		})
		if err != nil {
			return err
		}

		if _, err := fmt.Fprintf(out, "---\n%s", written); err != nil {
			return err
		}
	}
}

// plain turns a schema value into what a person wrote, or would have.
//
// The wire encoding is not it: protojson renders `{"stringValue": "x"}`,
// which is faithful and unreadable, and reading a status is the whole
// reason this output exists. So the tags are unwrapped, and what comes
// out is the shape a manifest is written in.
func plain(value *schemapb.StructValue) map[string]any {
	if value == nil {
		return map[string]any{}
	}

	read := make(map[string]any, len(value.GetFields()))
	for name, field := range value.GetFields() {
		read[name] = scalar(field)
	}

	return read
}

// scalar unwraps one value, whatever kind it turns out to be.
func scalar(value *schemapb.Value) any {
	if items, isList := value.AsList(); isList {
		read := make([]any, 0, len(items))
		for _, item := range items {
			read = append(read, scalar(item))
		}

		return read
	}

	if fields, isStruct := value.AsStruct(); isStruct {
		read := make(map[string]any, len(fields))
		for name, field := range fields {
			read[name] = scalar(field)
		}

		return read
	}

	if text, isText := schemapb.As[string](value); isText {
		return text
	}

	if number, isInt := schemapb.As[int64](value); isInt {
		return number
	}

	if number, isFloat := schemapb.As[float64](value); isFloat {
		return number
	}

	if yes, isBool := schemapb.As[bool](value); isBool {
		return yes
	}

	// Null, or something a later contract added. Written as nothing
	// rather than guessed at: an empty value that printed as `false`
	// would be a value nobody wrote.
	return nil
}

// written is the path a person typed, or none.
//
// The second argument, because the first is the kind: `get Process
// /k1` is one question with more of a path in it.
func written(args []string) string {
	if len(args) < withPath {
		return ""
	}

	return args[1]
}

// condition is the short answer to "what is this doing", which is the
// only reason a listing is worth reading at a glance.
func condition(record *graphenepbv1.Record) string {
	stored := record.GetResource()

	switch {
	case stored.GetDeleting():
		return "deleting"
	case stored.GetGeneration() == 0:
		return "-"
	case len(stored.GetStatus().GetFields()) == 0:
		return "unreported"
	}

	// A kernel is the one kind whose record answers a question people
	// actually ask of a listing — is that machine there — so the listing
	// answers it rather than saying "reported" about a kernel that has
	// been off for a week.
	if stored.GetId().GetKind() == report.KernelKind.String() {
		return liveness(stored)
	}

	return "reported"
}

// liveness is what a kernel's record says about the kernel.
//
// Worked out here from what the record says, not read out of it: nothing
// stores a verdict, because a stored one goes stale in exactly the case
// that matters.
func liveness(stored *graphenepbv1.Resource) string {
	read, err := convert.ResourceFromPb(stored)
	if err != nil {
		return "reported"
	}

	state, since := report.Alive(read, time.Now())
	if state == report.Up || since.IsZero() {
		return string(state)
	}

	return fmt.Sprintf("%s (last seen %s ago)", state, roughly(time.Since(since)))
}

// roughly is a duration a person reads, which is one unit and no decimals.
func roughly(over time.Duration) time.Duration {
	if over < time.Minute {
		return over.Round(time.Second)
	}

	if over < time.Hour {
		return over.Round(time.Minute)
	}

	return over.Round(time.Hour)
}

// withPath is how many arguments a command has when one of them is a
// path, and columnGap is how far apart the columns of a listing sit.
const (
	withPath  = 2
	columnGap = 3
)

// tabulated writes columns that line up.
func tabulated(out io.Writer, rows func(write func(...string)) error) error {
	table := tabwriter.NewWriter(out, 0, 0, columnGap, ' ', 0)

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
