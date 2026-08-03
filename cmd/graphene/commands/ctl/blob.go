package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/utils/cmdflags"
)

// BlobFlags names a local file and the kernel to talk to.
type BlobFlags struct {
	Target *TargetFlags
	File   string
}

// Put uploads a file and prints the id the kernel gave it. That id — not
// a checksum — is how everything afterwards refers to those bytes.
func Put(ctx context.Context, out io.Writer, flags *BlobFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	file, err := os.Open(flags.File)
	if err != nil {
		return fmt.Errorf("blob put: %w", err)
	}

	defer func() { _ = file.Close() }()

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	id, err := client.Upload(ctx, file)
	if err != nil {
		return fmt.Errorf("blob put: %w", err)
	}

	if _, err := fmt.Fprintln(out, id); err != nil {
		return fmt.Errorf("blob put: %w", err)
	}

	return nil
}

// Fetch writes a blob's bytes to the given file, or to stdout.
func Fetch(ctx context.Context, out io.Writer, id string, flags *BlobFlags) error {
	if flags == nil {
		return errFlagsRequired
	}

	client, err := connect(flags.Target)
	if err != nil {
		return err
	}

	defer func() { _ = client.Close() }()

	sink := out

	if flags.File != "" {
		file, err := os.Create(flags.File)
		if err != nil {
			return fmt.Errorf("blob get: %w", err)
		}

		defer func() { _ = file.Close() }()

		sink = file
	}

	if err := client.Download(ctx, id, sink); err != nil {
		return fmt.Errorf("blob get: %w", err)
	}

	return nil
}

func newBlobCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "blob",
		Short: "Move bytes in and out of a kernel",
	}

	command.AddCommand(newBlobPutCommand(), newBlobGetCommand())

	return command
}

func newBlobPutCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "put <file>",
		Short:   "Upload a file and print its blob id",
		Example: "  graphene ctl blob put ./driver",
		Args:    cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			return Put(command.Context(), command.OutOrStdout(),
				&BlobFlags{Target: target, File: args[0]})
		},
	}

	return command
}

func newBlobGetCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "get <id>",
		Short:   "Download a blob",
		Example: "  graphene ctl blob get b-3 --file ./driver",
		Args:    cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			target, err := newTargetFlags(command)
			if err != nil {
				return err
			}

			file, err := cmdflags.String(command, "file")
			if err != nil {
				return err
			}

			return Fetch(command.Context(), command.OutOrStdout(), args[0],
				&BlobFlags{Target: target, File: file})
		},
	}

	command.Flags().String("file", "", "write to this file instead of stdout")

	return command
}
