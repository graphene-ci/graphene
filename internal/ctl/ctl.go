// Package ctl is graphenectl: the generic control CLI over the
// installation's RECORDS — kubectl's stance. It knows nothing about any
// pipeline's code; the pipeline binary manages its own pipeline (push,
// run) with the same connection contexts.
package ctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/graphene-ci/graphene/internal/cli"
)

// Version is stamped by the build.
var Version = "dev"

const usage = `usage: graphenectl <command> [args]

connection (shared with pipeline binaries; the file: --config, else
$GRAPHENE_CONFIG, else ~/.config/graphene/config.yaml; field overrides:
$GRAPHENE_ADDRESS/TOKEN/NAMESPACE/INSECURE — with a server and a token
in the environment no file is needed at all):
  ctx list | show | current
  ctx use <name>
  ctx set <name> --server host:port [--token-stdin] [--namespace ns]
                 [--insecure] [--base-image ref] [--use]
  ctx delete <name> | rename <old> <new>

records (five dimensions each):
  res list [-k kind] [-p phase] [--owner ref] [-l k=v]
  res get <ref>                     state: the full record
  res events <ref> [--follow]       its own history
  res logs <ref> [--follow]         telemetry
  res metrics <ref>
  res trace <ref>
  res tree <owner>
  res delete <ref>
  res transfer <ref> <new-owner> [--keep 72h]
  res invoke <ref> <command> [--data JSON]

runs (a run is a record too — the same dimensions apply):
  run list [--status S] [-l k=v]
  run get | watch | result | cancel <run-id>
  run start <pipeline> [--run-id id] [--params JSON] [--image ref] [-l k=v] [--watch]
  run events | logs | metrics | trace <run-id> [--follow]

installation:
  pipeline show <pipeline-id>
  secret set <name> [--value v]     (no --value: read from stdin)
  secret list | delete <name>
  ns list | create <name> [--retention-days n]

project:
  init <name>                       scaffold a pipeline project
  version

every read command takes -o table|json (default table); every network
command takes --context, --config, and -n (per-call namespace for
cluster-wide admin tokens).`

// Main runs graphenectl; the exit code is the return.
func Main(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	ctx := context.Background()
	var err error
	switch args[0] {
	case "ctx":
		err = cmdCtx(args[1:])
	case "res":
		err = cmdRes(ctx, args[1:])
	case "run":
		err = cmdRun(ctx, args[1:])
	case "pipeline":
		err = cmdPipeline(ctx, args[1:])
	case "secret":
		err = cmdSecret(ctx, args[1:])
	case "ns":
		err = cmdNs(ctx, args[1:])
	case "init":
		err = cli.Init(args[1:], os.Stdout, os.Stderr)
	case "version":
		fmt.Println("graphenectl", Version)
	case "help", "-h", "--help":
		fmt.Fprintln(os.Stderr, usage)
	default:
		fmt.Fprintf(os.Stderr, "graphenectl: unknown command %q\n%s\n", args[0], usage)
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "graphenectl:", err)
		return 1
	}
	return 0
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
