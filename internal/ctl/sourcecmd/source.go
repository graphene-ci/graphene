// Package sourcecmd is the ctl surface of a pipeline's SOURCE side:
// create one from a Git repository or a local directory, re-sync it,
// download or edit the working tree. Reading, listing and deleting are
// the ordinary record verbs — a pipeline is a record like any other.
package sourcecmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/revisioncmd"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// New builds the `source` tree: the BYTES of a source. Declaring a
// source, changing it and reading its state are the generic verbs —
// what lives here is only what bytes cannot travel through them.
func New(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source",
		Short: "Source bytes: upload, download, and the files of a tree",
	}

	var uploadPipeline string
	upload := &cobra.Command{
		Use:   "upload <pipeline> <dir|file.tgz>",
		Short: "Upload a tree and print its reference",
		Long: `Store a local directory as bytes in the installation and print the
reference to it. Nothing is declared here: hand the reference to a
managedsource's "upload" field with the ordinary apply.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			packed, err := revisioncmd.PackSource(args[1])
			if err != nil {
				return err
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "uploading %s (%d KB)\n", args[1], len(packed)/1024)
			resp, err := d.Source.UploadSource(cmd.Context(), connect.NewRequest(&managementv1.UploadSourceRequest{
				PipelineId: args[0], Source: packed,
			}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintln(cmdutil.Out, resp.Msg.GetLocation())
			return nil
		},
	}
	_ = uploadPipeline

	var out string
	download := &cobra.Command{
		Use:   "download <source>",
		Short: "Download the source's tree (tar.gz)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			ref, err := sourceRef(args[0])
			if err != nil {
				return err
			}
			stream, err := d.Source.DownloadSource(cmd.Context(),
				connect.NewRequest(&managementv1.DownloadSourceRequest{Source: ref}))
			if err != nil {
				return err
			}
			defer func() { _ = stream.Close() }()
			var sink io.Writer = os.Stdout
			if out != "" {
				f, err := os.Create(out) //nolint:gosec // the user's named output
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				sink = f
			}
			n := 0
			for stream.Receive() {
				chunk := stream.Msg().GetData()
				if _, err := sink.Write(chunk); err != nil {
					return err
				}
				n += len(chunk)
			}
			if err := stream.Err(); err != nil {
				return err
			}
			if out != "" {
				fmt.Fprintf(os.Stderr, "%s (%d KB)\n", out, n/1024)
			}
			return nil
		},
	}
	download.Flags().StringVarP(&out, "output", "o", "", "write to this file instead of stdout")

	runtimesCmd := &cobra.Command{
		Use:   "runtimes",
		Short: "Which languages this installation can build a pipeline from",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Source.ListRuntimes(cmd.Context(), connect.NewRequest(&managementv1.ListRuntimesRequest{}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "RUNTIME\tVERSION\tDEFAULT\tIMAGE\n")
			for _, r := range resp.Msg.GetRuntimes() {
				def := ""
				if r.GetIsDefault() {
					def = "*"
				}
				fmt.Fprintf(cmdutil.Out, "%s\t%s\t%s\t%s\n", r.GetName(), r.GetVersion(), def, r.GetImage())
			}
			return nil
		},
	}

	files := &cobra.Command{
		Use:   "files <source>",
		Short: "List the source's tree",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			ref, err := sourceRef(args[0])
			if err != nil {
				return err
			}
			resp, err := d.Source.ListFiles(cmd.Context(), connect.NewRequest(&managementv1.ListFilesRequest{Source: ref}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "SIZE\tPATH\n")
			for _, file := range resp.Msg.GetFiles() {
				fmt.Fprintf(cmdutil.Out, "%d\t%s\n", file.GetSize(), file.GetPath())
			}
			return nil
		},
	}

	cat := &cobra.Command{
		Use:   "cat <source> <path>",
		Short: "Read one file of the working tree",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			ref, err := sourceRef(args[0])
			if err != nil {
				return err
			}
			resp, err := d.Source.ReadFile(cmd.Context(), connect.NewRequest(&managementv1.ReadFileRequest{
				Source: ref, Path: args[1],
			}))
			if err != nil {
				return err
			}
			_, err = cmdutil.Out.Write(resp.Msg.GetContent())
			return err
		},
	}

	var fromFile string
	write := &cobra.Command{
		Use:   "write <source> <path>",
		Short: "Write one file into the working tree (stdin, or --from)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			var content []byte
			var err error
			if fromFile != "" {
				content, err = os.ReadFile(fromFile) //nolint:gosec // the user's named file
			} else {
				content, err = io.ReadAll(os.Stdin)
			}
			if err != nil {
				return err
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			ref, err := sourceRef(args[0])
			if err != nil {
				return err
			}
			resp, err := d.Source.WriteFile(cmd.Context(), connect.NewRequest(&managementv1.WriteFileRequest{
				Source: ref, Path: args[1], Content: content,
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "%s written; tree %s (generation %d)\n",
				args[1], resp.Msg.GetTreeDigest(), resp.Msg.GetGeneration())
			return nil
		},
	}
	write.Flags().StringVar(&fromFile, "from", "", "read the content from this local file")

	rm := &cobra.Command{
		Use:   "rm <source> <path>",
		Short: "Delete one file from the working tree",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			ref, err := sourceRef(args[0])
			if err != nil {
				return err
			}
			resp, err := d.Source.DeleteFile(cmd.Context(), connect.NewRequest(&managementv1.DeleteFileRequest{
				Source: ref, Path: args[1],
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "%s deleted; tree %s (generation %d)\n",
				args[1], resp.Msg.GetTreeDigest(), resp.Msg.GetGeneration())
			return nil
		},
	}

	cmd.AddCommand(upload, download, runtimesCmd, files, cat, write, rm)
	return cmd
}

// sourceRef takes the reference as given. A bare name is refused
// rather than guessed: which kind a name belongs to is the
// installation's answer, and guessing "managedsource" here would send
// a write to a record the caller never named.
func sourceRef(name string) (string, error) {
	if strings.Contains(name, "/") {
		return name, nil
	}
	return "", fmt.Errorf("name the source as kind/id (`graphenectl kinds` lists the kinds; `get gitsource` and `get managedsource` list the records)")
}

// short renders a digest the way a person reads it.
func short(digest string) string {
	d := strings.TrimPrefix(digest, "sha256:")
	if len(d) > 12 {
		return d[:12]
	}
	return d
}
