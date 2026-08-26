// Package sourcecmd is the ctl surface of a pipeline's SOURCE side:
// create one from a Git repository or a local directory, re-sync it,
// download or edit the working tree. Reading, listing and deleting are
// the ordinary record verbs — a pipeline is a record like any other.
package sourcecmd

import (
	"encoding/json"
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

// Attach hangs the source verbs on the `pipeline` tree: a project's
// source is the pipeline's own, not a separate thing to manage.
func Attach(f *cmdutil.Factory, cmd *cobra.Command) {

	var gitUrl, gitRef, subdir, credential, upload, runtime, sourceName string
	create := &cobra.Command{
		Use:   "create <pipeline>",
		Short: "Create a pipeline and its source",
		Long: `Declare a pipeline and one source under it: a gitsource, whose files
follow a ref and are read-only, or a managedsource, whose files are the
project's own and are edited in place.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var tree []byte
			switch {
			case gitUrl != "" && upload != "":
				return fmt.Errorf("a source comes from one place: --git or --upload, not both")
			case upload != "":
				packed, err := revisioncmd.PackSource(upload)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "uploading %s (%d KB)\n", upload, len(packed)/1024)
				tree = packed
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			// The pipeline first: a source belongs to it, so it must
			// exist before the source names it.
			if _, err := d.Resources.Apply(cmd.Context(), connect.NewRequest(&managementv1.ApplyRequest{
				Kind: "pipeline", Id: args[0], Spec: []byte("{}"),
			})); err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "pipeline/%s applied\n", args[0])

			name := sourceName
			if name == "" {
				name = args[0]
			}
			kind := "managedsource"
			spec := map[string]any{"pipelineId": args[0], "runtime": runtime}
			switch {
			case gitUrl != "":
				kind = "gitsource"
				spec["url"], spec["ref"] = gitUrl, gitRef
				spec["subdir"], spec["credentialRef"] = subdir, credential
			case tree != nil:
				// Bytes travel their own channel first; the declaration
				// carries the reference.
				up, err := d.Source.UploadSource(cmd.Context(), connect.NewRequest(&managementv1.UploadSourceRequest{
					PipelineId: args[0], Source: tree,
				}))
				if err != nil {
					return err
				}
				spec["upload"] = up.Msg.GetLocation()
			}
			specJSON, err := json.Marshal(spec)
			if err != nil {
				return err
			}
			applied, err := d.Resources.Apply(cmd.Context(), connect.NewRequest(&managementv1.ApplyRequest{
				Kind: kind, Id: name, Spec: specJSON,
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "%s applied\n", applied.Msg.GetRef())
			return nil
		},
	}
	create.Flags().StringVar(&gitUrl, "git", "", "git repository url (omit for an editable managed source)")
	create.Flags().StringVar(&gitRef, "ref", "", "branch, tag or commit")
	create.Flags().StringVar(&subdir, "subdir", "", "pipeline root inside a monorepo")
	create.Flags().StringVar(&credential, "credential", "", "name of the secret holding the git token")
	create.Flags().StringVarP(&upload, "upload", "f", "", "local directory or .tgz to start a managed source from")
	create.Flags().StringVar(&runtime, "runtime", "", "project runtime (default: the installation's)")
	create.Flags().StringVar(&sourceName, "source-name", "", "name of the source record (default: the pipeline's name)")

	sync := &cobra.Command{
		Use:   "sync <source>",
		Short: "Fetch a git source's ref again",
		Long: `Re-read the ref a gitsource follows. A managed source has nothing to
sync: its files ARE the source, and writing one is the change.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			ref := args[0]
			if !strings.Contains(ref, "/") {
				ref = "gitsource/" + ref
			}
			resp, err := d.Resources.Invoke(cmd.Context(), connect.NewRequest(&managementv1.InvokeRequest{
				Ref: ref, Command: "sync", Payload: []byte("{}"),
			}))
			if err != nil {
				return err
			}
			var out struct {
				TreeDigest string `json:"treeDigest"`
				Commit     string `json:"commit"`
				Generation uint64 `json:"generation"`
			}
			_ = json.Unmarshal(resp.Msg.GetResult(), &out)
			fmt.Fprintf(cmdutil.Out, "tree       %s\ngeneration %d\n", out.TreeDigest, out.Generation)
			if out.Commit != "" {
				fmt.Fprintf(cmdutil.Out, "commit     %s\n", out.Commit)
			}
			return nil
		},
	}

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
			stream, err := d.Source.DownloadSource(cmd.Context(),
				connect.NewRequest(&managementv1.DownloadSourceRequest{Source: sourceRef(args[0])}))
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
			resp, err := d.Source.ListFiles(cmd.Context(), connect.NewRequest(&managementv1.ListFilesRequest{Source: sourceRef(args[0])}))
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
			resp, err := d.Source.ReadFile(cmd.Context(), connect.NewRequest(&managementv1.ReadFileRequest{
				Source: sourceRef(args[0]), Path: args[1],
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
			resp, err := d.Source.WriteFile(cmd.Context(), connect.NewRequest(&managementv1.WriteFileRequest{
				Source: sourceRef(args[0]), Path: args[1], Content: content,
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
			resp, err := d.Source.DeleteFile(cmd.Context(), connect.NewRequest(&managementv1.DeleteFileRequest{
				Source: sourceRef(args[0]), Path: args[1],
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "%s deleted; tree %s (generation %d)\n",
				args[1], resp.Msg.GetTreeDigest(), resp.Msg.GetGeneration())
			return nil
		},
	}

	cmd.AddCommand(create, sync, download, runtimesCmd, files, cat, write, rm)
}

// sourceRef lets a bare name mean the managed source of that name:
// "gctl pipeline files my-app" is the common case, and the kind is
// only spelled out when it matters.
func sourceRef(name string) string {
	if strings.Contains(name, "/") {
		return name
	}
	return "managedsource/" + name
}
