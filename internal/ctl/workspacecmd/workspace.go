// Package workspacecmd is the ctl surface of workspaces: create from a
// Git repository or a local directory, re-sync the source, download
// the working tree. Reading, listing and deleting are the ordinary
// record verbs — a workspace is a record like any other.
package workspacecmd

import (
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
			req := &managementv1.CreateWorkspaceRequest{
				WorkspaceId: args[0],
				Runtime:     runtime,
				PipelineId:  pipelineId,
			}
			switch {
			case gitUrl != "" && upload != "":
				return fmt.Errorf("a workspace has one source: --git or --upload, not both")
			case gitUrl != "":
				req.Source = &managementv1.CreateWorkspaceRequest_Git{Git: &managementv1.GitSource{
					Url: gitUrl, Ref: gitRef, Subdir: subdir, CredentialSecret: credential,
				}}
			case upload != "":
				tree, err := revisioncmd.PackSource(upload)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "uploading %s (%d KB)\n", upload, len(tree)/1024)
				req.Source = &managementv1.CreateWorkspaceRequest_Snapshot{
					Snapshot: &managementv1.SnapshotSource{Source: tree},
				}
			default:
				return fmt.Errorf("a workspace needs a source: --git <url> or --upload <dir>")
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Workspaces.CreateWorkspace(cmd.Context(), connect.NewRequest(req))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "workspace %s\ntree      %s\n", resp.Msg.GetWorkspaceId(), resp.Msg.GetTreeDigest())
			if c := resp.Msg.GetGitCommit(); c != "" {
				fmt.Fprintf(cmdutil.Out, "commit    %s\n", c)
			}
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
			req := &managementv1.SyncWorkspaceRequest{WorkspaceId: args[0]}
			if syncUpload != "" {
				tree, err := revisioncmd.PackSource(syncUpload)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "uploading %s (%d KB)\n", syncUpload, len(tree)/1024)
				req.Source = tree
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Workspaces.SyncWorkspace(cmd.Context(), connect.NewRequest(req))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "tree       %s\ngeneration %d\n", resp.Msg.GetTreeDigest(), resp.Msg.GetGeneration())
			if c := resp.Msg.GetGitCommit(); c != "" {
				fmt.Fprintf(cmdutil.Out, "commit     %s\n", c)
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

	cmd.AddCommand(create, sync, download)
	return cmd
}
