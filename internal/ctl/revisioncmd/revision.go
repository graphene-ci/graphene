// Package revisioncmd is the ctl surface of the source-first contour:
// materialize a source tree, list revisions, run a draft, activate.
package revisioncmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// maxFileBytes skips accidental artifacts (built binaries) in a source
// directory — a source tree is text.
const maxFileBytes = 8 << 20

// New builds the `revision` tree.
func New(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revision",
		Short: "Source revisions: materialize, list, run, activate",
	}

	var srcPath string
	mat := &cobra.Command{
		Use:   "materialize <pipeline>",
		Short: "Build a source tree into a revision on the server",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// With no --source the PIPELINE's own working tree is built:
			// nothing is uploaded, the tree already lives on the server.
			if !cmd.Flags().Changed("source") {
				return materializeStream(cmd, f, &managementv1.MaterializeRequest{PipelineId: args[0]})
			}
			src, err := PackSource(srcPath)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "uploading %s (%d KB)\n", srcPath, len(src)/1024)
			return materializeStream(cmd, f, &managementv1.MaterializeRequest{
				PipelineId: args[0], Source: src,
			})
		},
	}
	mat.Flags().StringVarP(&srcPath, "source", "f", ".", "upload THIS directory instead of the pipeline's own tree")

	list := &cobra.Command{
		Use:   "list <pipeline>",
		Short: "List the pipeline's revisions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			// Revisions are ordinary records: the listing is the generic
			// one, narrowed to this pipeline's own. The only thing this
			// wrapper adds is WHICH of them is active — and that is a
			// property of the pipeline, read from the pipeline.
			resp, err := d.Resources.List(cmd.Context(), connect.NewRequest(&managementv1.ListRequest{
				Query: "kind=revision, owner=pipeline/" + args[0],
			}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			// Which one is active is a property of the PIPELINE — one
			// read, not one per revision: a listing does not carry
			// state, and asking for it per row is the N+1 this door
			// was built to avoid.
			activeId := ""
			if got, err := d.Resources.Get(cmd.Context(), connect.NewRequest(&managementv1.GetRequest{
				Ref: "pipeline/" + args[0],
			})); err == nil {
				var st struct {
					RevisionId string `json:"revisionId"`
				}
				_ = json.Unmarshal(got.Msg.GetResource().GetState(), &st)
				activeId = st.RevisionId
			}
			fmt.Fprintf(cmdutil.Out, "REVISION\tACTIVE\tPHASE\n")
			for _, r := range resp.Msg.GetResources() {
				id := strings.TrimPrefix(r.GetRef(), "revision/"+args[0]+".")
				active := ""
				if id != "" && id == activeId {
					active = "*"
				}
				fmt.Fprintf(cmdutil.Out, "%s\t%s\t%s\n", id, active, r.GetPhase())
			}
			return nil
		},
	}

	var params, runId string
	run := &cobra.Command{
		Use:   "run <pipeline> <revision>",
		Short: "Start a DRAFT run of one revision (active or not)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if runId == "" {
				runId = fmt.Sprintf("%s-draft-%s", args[0], time.Now().UTC().Format("20060102-150405"))
			}
			resp, err := d.Revisions.RunRevision(cmd.Context(), connect.NewRequest(&managementv1.RunRevisionRequest{
				PipelineId: args[0], RevisionId: args[1], RunId: runId, Params: []byte(params),
			}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "%s\n", resp.Msg.GetWorkflowId())
			return nil
		},
	}
	run.Flags().StringVarP(&params, "params", "p", "", "params as raw JSON")
	run.Flags().StringVar(&runId, "run-id", "", "run id (default: generated draft id)")

	activate := &cobra.Command{
		Use:   "activate <pipeline> <revision>",
		Short: "Make one revision the version automatic starts use",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			// Activation is a decision about the PIPELINE, so it is the
			// pipeline's own command — and it names the revision,
			// nothing more: what that revision built is the record's to
			// read, not the client's to carry.
			payload, err := json.Marshal(map[string]any{
				"revisionId": args[1],
			})
			if err != nil {
				return err
			}
			if _, err := d.Resources.Invoke(cmd.Context(), connect.NewRequest(&managementv1.InvokeRequest{
				Ref: "pipeline/" + args[0], Command: "activate", Payload: payload,
			})); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "revision %s activated for %s\n", args[1], args[0])
			return nil
		},
	}

	cmd.AddCommand(mat, list, run, activate)
	return cmd
}

// PackSource renders a directory (or passes a ready .tgz) as the
// upload: .git and oversized files (built binaries) stay behind.
func PackSource(path string) ([]byte, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return os.ReadFile(path) //nolint:gosec // the user's named archive
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	err = filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(path, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		base := filepath.Base(rel)
		if info.IsDir() {
			if base == ".git" || base == "node_modules" {
				return filepath.SkipDir
			}
			return tw.WriteHeader(&tar.Header{Name: rel + "/", Typeflag: tar.TypeDir, Mode: 0o755})
		}
		if !info.Mode().IsRegular() || info.Size() > maxFileBytes {
			return nil
		}
		if strings.HasPrefix(base, ".") && base != ".gitignore" {
			return nil
		}
		if err := tw.WriteHeader(&tar.Header{Name: rel, Mode: 0o644, Size: info.Size()}); err != nil {
			return err
		}
		f, err := os.Open(p) //nolint:gosec // walking the user's named tree
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// materializeStream runs one materialization and prints the build as it
// happens; the last event carries the revision.
func materializeStream(cmd *cobra.Command, f *cmdutil.Factory, req *managementv1.MaterializeRequest) error {
	d, err := f.Dial()
	if err != nil {
		return err
	}
	stream, err := d.Revisions.Materialize(cmd.Context(), connect.NewRequest(req))
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	for stream.Receive() {
		ev := stream.Msg()
		if res := ev.GetResult(); res != nil {
			if done, err := f.Emit(res); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "revision %s\nimage    %s\n", res.GetRevisionId(), res.GetImage())
			return nil
		}
		fmt.Fprintf(os.Stderr, "%-8s %s\n", ev.GetStage(), ev.GetMessage())
	}
	return stream.Err()
}
