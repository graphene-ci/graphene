package commands

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/gopherex/schemapb/go/schemapb"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"
)

// The three things a written resource says. They are a format: this is
// what a person types and what a repository keeps.
const (
	kindField = "kind"
	pathField = "path"
	specField = "spec"
)

// applyCommand writes what a file asks for.
//
// THE SPEC AND NOTHING ELSE. A file cannot set a status, a generation, a
// version or a deletion — not because this command strips them, but
// because Put takes an intent and an intent has no room for them. What a
// person writes down is what they want; everything else is the kernel's
// account of what came of it.
//
// It reads ONE resource, not a stream of documents. A file per resource
// is what a repository of these looks like, and a multi-document file
// would need an answer for what happens when the third one fails that
// nothing here can honor — there are no transactions across resources.
func applyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "apply <file>",
		Short: "Write what a file asks for",
		Long: "Create or update one resource from a YAML file:\n\n" +
			"  kind: Process\n" +
			"  path: local/one\n" +
			"  spec:\n" +
			"    bundle: something\n\n" +
			"A file cannot set a status or anything else the kernel keeps " +
			"about a resource. Use - to read standard input.",
		Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			written, err := read(command, args[0])
			if err != nil {
				return err
			}

			asked, err := parsed(written)
			if err != nil {
				return fmt.Errorf("%s: %w", args[0], err)
			}

			on, err := reached(command)
			if err != nil {
				return err
			}

			ctx := calling(command, on)

			at, err := addressed(ctx, on, asked.Kind, asked.Path)
			if err != nil {
				return err
			}

			spec, err := schemapb.StructFromGo(asked.Spec)
			if err != nil {
				return fmt.Errorf("%s: spec: %w", args[0], err)
			}

			// READ FIRST, which is what makes this create-or-update: the
			// kernel takes no write without an expectation, and the zero
			// one means "must not exist yet".
			expect, err := expectation(ctx, on, at)
			if err != nil {
				return err
			}

			answer, err := on.Calls().Put(ctx, &graphenepbv1.PutRequest{
				Id:     at,
				Spec:   spec,
				Expect: expect,
			})
			if err != nil {
				return err
			}

			_, _ = fmt.Fprintf(command.OutOrStdout(), "%s /%s at %d\n",
				asked.Kind, asked.Path, answer.GetRevision())

			return nil
		},
	}
}

// asked is a resource as a person writes one down.
type asked struct {
	Kind string         `yaml:"kind"`
	Path string         `yaml:"path"`
	Spec map[string]any `yaml:"spec"`
}

// errMissingField — the document does not say something a write needs.
var errMissingField = errors.New("missing field")

// parsed reads one, refusing what cannot be acted on.
func parsed(written []byte) (asked, error) {
	var read asked

	if err := yaml.Unmarshal(written, &read); err != nil {
		return asked{}, err
	}

	switch {
	case read.Kind == "":
		return asked{}, fmt.Errorf("%w: %s", errMissingField, kindField)
	case read.Path == "":
		return asked{}, fmt.Errorf("%w: %s", errMissingField, pathField)
	case read.Spec == nil:
		// An empty spec is a real thing to write — some kinds have no
		// fields — but an ABSENT one is somebody who forgot the key, and
		// the two are worth telling apart before a resource is emptied.
		return asked{}, fmt.Errorf("%w: %s (write `%s: {}` for a kind with no fields)",
			errMissingField, specField, specField)
	}

	return read, nil
}

// read takes a file, or standard input.
func read(command *cobra.Command, at string) ([]byte, error) {
	if at == "-" {
		return io.ReadAll(command.InOrStdin())
	}

	return os.ReadFile(at)
}
