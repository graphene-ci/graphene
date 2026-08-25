// Package workspacecmd is the ctl surface of workspaces: create from a
// Git repository or a local directory, re-sync the source, download
// the working tree. Reading, listing and deleting are the ordinary
// record verbs — a workspace is a record like any other.
package workspacecmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/revisioncmd"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// New builds the `workspace` tree.
func New(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workspace",
		Short: "Workspaces: create, sync, download — the project's working area",
	}

	var gitUrl, gitRef, subdir, credential, upload, runtime, pipelineId string
	create := &cobra.Command{
		Use:   "create <workspace>",
		Short: "Create a workspace from a git repository or a local directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var tree []byte
			switch {
			case gitUrl != "" && upload != "":
				return fmt.Errorf("a workspace has one source: --git or --upload, not both")
			case gitUrl == "" && upload == "":
				return fmt.Errorf("a workspace needs a source: --git <url> or --upload <dir>")
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
			// A workspace is declared through the one door every kind is
			// declared through. An UPLOADED source is the exception the
			// rule allows: bytes travel their own channel first, and the
			// declaration carries the reference.
			spec := map[string]any{"runtime": runtime, "pipelineId": pipelineId}
			switch {
			case gitUrl != "":
				spec["git"] = map[string]any{
					"url": gitUrl, "ref": gitRef, "subdir": subdir, "credentialRef": credential,
				}
			default:
				up, err := d.Workspaces.UploadSource(cmd.Context(), connect.NewRequest(&managementv1.UploadSourceRequest{
					WorkspaceId: args[0], Source: tree,
				}))
				if err != nil {
					return err
				}
				spec["snapshot"] = map[string]any{
					"location": up.Msg.GetLocation(), "digest": up.Msg.GetDigest(),
				}
			}
			specJSON, err := json.Marshal(spec)
			if err != nil {
				return err
			}
			applied, err := d.Resources.Apply(cmd.Context(), connect.NewRequest(&managementv1.ApplyRequest{
				Kind: "workspace", Id: args[0], Spec: specJSON,
			}))
			if err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "%s applied\n", applied.Msg.GetRef())
			return nil
		},
	}
	create.Flags().StringVar(&gitUrl, "git", "", "git repository url")
	create.Flags().StringVar(&gitRef, "ref", "", "branch, tag or commit")
	create.Flags().StringVar(&subdir, "subdir", "", "pipeline root inside a monorepo")
	create.Flags().StringVar(&credential, "credential", "", "name of the secret holding the git token")
	create.Flags().StringVarP(&upload, "upload", "f", "", "local directory or .tgz to upload as the source")
	create.Flags().StringVar(&runtime, "runtime", "", "project runtime (default: the installation's)")
	create.Flags().StringVar(&pipelineId, "pipeline", "", "the pipeline this workspace publishes")

	var syncUpload string
	sync := &cobra.Command{
		Use:   "sync <workspace>",
		Short: "Re-fetch the git ref, or replace the tree with a local directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			payload := map[string]any{}
			if syncUpload != "" {
				tree, err := revisioncmd.PackSource(syncUpload)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "uploading %s (%d KB)\n", syncUpload, len(tree)/1024)
				up, err := d.Workspaces.UploadSource(cmd.Context(), connect.NewRequest(&managementv1.UploadSourceRequest{
					WorkspaceId: args[0], Source: tree,
				}))
				if err != nil {
					return err
				}
				payload["location"], payload["digest"] = up.Msg.GetLocation(), up.Msg.GetDigest()
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			resp, err := d.Resources.Invoke(cmd.Context(), connect.NewRequest(&managementv1.InvokeRequest{
				Ref: "workspace/" + args[0], Command: "sync", Payload: raw,
			}))
			if err != nil {
				return err
			}
			var out struct {
				TreeDigest string `json:"treeDigest"`
				GitCommit  string `json:"gitCommit"`
				Generation uint64 `json:"generation"`
			}
			_ = json.Unmarshal(resp.Msg.GetResult(), &out)
			fmt.Fprintf(cmdutil.Out, "tree       %s\ngeneration %d\n", out.TreeDigest, out.Generation)
			if out.GitCommit != "" {
				fmt.Fprintf(cmdutil.Out, "commit     %s\n", out.GitCommit)
			}
			return nil
		},
	}
	sync.Flags().StringVarP(&syncUpload, "upload", "f", "", "local directory or .tgz replacing the working tree")

	var out string
	download := &cobra.Command{
		Use:   "download <workspace>",
		Short: "Download the workspace's current working tree (tar.gz)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			stream, err := d.Workspaces.DownloadSource(cmd.Context(),
				connect.NewRequest(&managementv1.DownloadSourceRequest{WorkspaceId: args[0]}))
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
			resp, err := d.Workspaces.ListRuntimes(cmd.Context(), connect.NewRequest(&managementv1.ListRuntimesRequest{}))
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
		Use:   "files <workspace>",
		Short: "List the workspace's working tree",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Workspaces.ListFiles(cmd.Context(), connect.NewRequest(&managementv1.ListFilesRequest{WorkspaceId: args[0]}))
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
		Use:   "cat <workspace> <path>",
		Short: "Read one file of the working tree",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Workspaces.ReadFile(cmd.Context(), connect.NewRequest(&managementv1.ReadFileRequest{
				WorkspaceId: args[0], Path: args[1],
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
		Use:   "write <workspace> <path>",
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
			resp, err := d.Workspaces.WriteFile(cmd.Context(), connect.NewRequest(&managementv1.WriteFileRequest{
				WorkspaceId: args[0], Path: args[1], Content: content,
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
		Use:   "rm <workspace> <path>",
		Short: "Delete one file from the working tree",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Workspaces.DeleteFile(cmd.Context(), connect.NewRequest(&managementv1.DeleteFileRequest{
				WorkspaceId: args[0], Path: args[1],
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
	return cmd
}
