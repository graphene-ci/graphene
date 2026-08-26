// Package ctl is graphenectl: the generic control CLI over the
// installation's RECORDS — kubectl's stance, kubectl's grammar (the
// verb first, the kind second), and kubectl's layout: one package per
// command group, the shared machinery in cmdutil, this file only
// assembles the tree.
package ctl

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/cli"
	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"github.com/graphene-ci/graphene/internal/ctl/ctxcmd"
	"github.com/graphene-ci/graphene/internal/ctl/getcmd"
	"github.com/graphene-ci/graphene/internal/ctl/lifecyclecmd"
	"github.com/graphene-ci/graphene/internal/ctl/misccmd"
	"github.com/graphene-ci/graphene/internal/ctl/observecmd"
	"github.com/graphene-ci/graphene/internal/ctl/rbaccmd"
	"github.com/graphene-ci/graphene/internal/ctl/revisioncmd"
	"github.com/graphene-ci/graphene/internal/ctl/runcmd"
	"github.com/graphene-ci/graphene/internal/ctl/sourcecmd"
)

// Version is stamped by the build.
var Version = "dev"

// NewRoot assembles the command tree.
func NewRoot() *cobra.Command {
	f := &cmdutil.Factory{}
	root := &cobra.Command{
		Use:   "graphenectl",
		Short: "The control CLI of a graphene installation",
		Long: `graphenectl manages an installation's RECORDS: resources with their
five dimensions, runs, secrets, namespaces, connection contexts. The
grammar is kubectl's — the verb first, the kind second; a target is
"<kind> <id>" or "kind/id", and a run is a record too (kind "run").

Your pipeline's own life (push, run from source) lives in the pipeline
binary itself, over the same connection contexts.`,
		Version:       Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	f.Bind(root)

	connection := &cobra.Group{ID: "connection", Title: "Connection:"}
	records := &cobra.Group{ID: "records", Title: "Records:"}
	runs := &cobra.Group{ID: "runs", Title: "Runs:"}
	installation := &cobra.Group{ID: "installation", Title: "Installation:"}
	project := &cobra.Group{ID: "project", Title: "Project:"}
	root.AddGroup(connection, records, runs, installation, project)

	add := func(group string, cmds ...*cobra.Command) {
		for _, c := range cmds {
			c.GroupID = group
			root.AddCommand(c)
		}
	}
	add("connection", ctxcmd.NewLogin(f), ctxcmd.NewCtx(f))
	add("records",
		getcmd.New(f),
		observecmd.New(f, "events", "The record's own history (dimension 2)"),
		observecmd.New(f, "logs", "The record's telemetry logs (dimension 3)"),
		observecmd.New(f, "metrics", "The record's metrics, PromQL JSON (dimension 4)"),
		observecmd.New(f, "trace", "The record's traces, Jaeger JSON (dimension 5)"),
		lifecyclecmd.NewApply(f), lifecyclecmd.NewKinds(f), lifecyclecmd.NewTree(f),
		lifecyclecmd.NewDelete(f),
		lifecyclecmd.NewTransfer(f),
		lifecyclecmd.NewInvoke(f),
	)
	add("runs", runcmd.New(f))
	add("installation", misccmd.NewSecret(f), revisioncmd.New(f), sourcecmd.New(f),
		rbaccmd.NewAccount(f), rbaccmd.NewWhoAmI(f))

	initCmd := &cobra.Command{
		Use:   "init <name>",
		Short: "Scaffold a pipeline project",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cli.Init(args, os.Stdout, os.Stderr)
		},
	}
	add("project", initCmd)
	// cobra's own `completion` and `help` join the project group.
	root.SetCompletionCommandGroupID("project")
	root.SetHelpCommandGroupID("project")
	return root
}

// Main runs graphenectl; the exit code is the return.
func Main(args []string) int {
	root := NewRoot()
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "graphenectl:", err)
		if hint := hintFor(err); hint != "" {
			fmt.Fprintln(os.Stderr, "  hint:", hint)
		}
		return 1
	}
	return 0
}

// hintFor turns the common failure modes into a next step.
func hintFor(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unauthenticated") || strings.Contains(msg, "401"):
		return "the token was rejected — check `graphenectl ctx show`, or re-run `graphenectl login`"
	case strings.Contains(msg, "permission_denied"):
		return "the token's role may not do this — an admin token might (`graphenectl ctx show` prints the role's scope)"
	case strings.Contains(msg, "connection refused") || strings.Contains(msg, "no such host"):
		return "the server is unreachable — check the context's server address (`graphenectl ctx show`)"
	}
	return ""
}
