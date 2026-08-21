// Package ctl is graphenectl: the generic control CLI over the
// installation's RECORDS — kubectl's stance and kubectl's grammar:
// the verb comes first, the kind second. It knows nothing about any
// pipeline's code; the pipeline binary manages its own pipeline (push,
// run) with the same connection contexts.
package ctl

import (
	"context"
	"fmt"
	"io"
	"strings"
	"os"

	"github.com/graphene-ci/graphene/internal/cli"
)

// Version is stamped by the build.
var Version = "dev"

const usage = `graphenectl — the control CLI of a graphene installation

Usage:
  graphenectl <verb> <kind> [id] [flags]
  graphenectl <command> <subcommand> [args] [flags]

Connection:
  login        verify a token, save the context, and switch to it
  ctx          contexts: list, show, current, use, set, delete, rename

Records (a target is "<kind> <id>" or "kind/id"; a run is kind "run"):
  get          list records of a kind, or read one in full
  events       the record's own history          (dimension 2)
  logs         the record's telemetry logs       (dimension 3)
  metrics      the record's metrics, PromQL JSON (dimension 4)
  trace        the record's traces, Jaeger JSON  (dimension 5)
  tree         the ownership tree under an owner
  delete       signal deletion; --wait blocks until gone
  transfer     give a record to a new owner; --keep bounds a stand stay
  invoke       send one of the record's own commands

Runs:
  run start    start a run of a pushed pipeline
  run watch    follow a run: a live tree of its resources
  run result   wait for a run and print its typed result
  run cancel   ask a run to stop; teardown still runs
  run list     list runs (same as: get run)

Installation:
  pipeline     the pipeline record: show
  secret       secrets: set, list, delete — values never print
  ns           namespaces: list, create

Project:
  init         scaffold a pipeline project
  completion   shell autocompletion: bash, zsh, fish
  version      print the version

Every read command:     -o table|wide|name|json|yaml, --jq EXPR
Every network command:  --context, --config, -n
Flags may come before or after positional arguments.
Command flags: graphenectl <command> -h.  Full guide: the docs site,
section "graphenectl".`

// Main runs graphenectl; the exit code is the return.
func Main(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	ctx := context.Background()
	var err error
	switch args[0] {
	case "login":
		err = cmdLogin(ctx, args[1:])
	case "ctx":
		err = cmdCtx(args[1:])
	case "get":
		err = cmdGet(ctx, args[1:])
	case "tree":
		err = cmdTree(ctx, args[1:])
	case "delete":
		err = cmdDelete(ctx, args[1:])
	case "transfer":
		err = cmdTransfer(ctx, args[1:])
	case "invoke":
		err = cmdInvoke(ctx, args[1:])
	case "events", "logs", "metrics", "trace":
		err = cmdObserve(ctx, args[0], args[1:])
	case "run":
		err = cmdRun(ctx, args[1:])
	case "pipeline":
		err = cmdPipeline(ctx, args[1:])
	case "secret":
		err = cmdSecret(ctx, args[1:])
	case "ns":
		err = cmdNs(ctx, args[1:])
	case "completion":
		err = cmdCompletion(args[1:])
	case "__complete":
		cmdComplete()
	case "init":
		err = cli.Init(args[1:], os.Stdout, os.Stderr)
	case "version":
		fmt.Println("graphenectl", Version)
	case "help", "-h", "--help":
		fmt.Fprintln(os.Stderr, usage)
	default:
		// One line, not the whole usage wall.
		fmt.Fprintf(os.Stderr, "graphenectl: unknown command %q — see `graphenectl help`\n", args[0])
		return 2
	}
	if err != nil {
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

// need returns the subcommand word and the rest, or an error listing
// the choices.
func need(args []string, choices string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, fmt.Errorf("want one of: %s", choices)
	}
	return args[0], args[1:], nil
}

// out is where data goes; progress goes to stderr.
var out io.Writer = os.Stdout
