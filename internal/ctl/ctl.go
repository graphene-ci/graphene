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
	"os"

	"github.com/graphene-ci/graphene/internal/cli"
)

// Version is stamped by the build.
var Version = "dev"

const usage = `usage: graphenectl <verb> [kind] [id] | <command> [args]

connection (the file: --config, else $GRAPHENE_CONFIG, else
~/.config/graphene/config.yaml; field overrides on top:
$GRAPHENE_ADDRESS/TOKEN/NAMESPACE/INSECURE — with a server and a token
in the environment no file is needed at all):
  login --server host:port --token-stdin [--name ctx] [--namespace ns]
        [--insecure]           verify the token, save and switch context
  ctx list | show | current
  ctx use <name>
  ctx set <name> --server host:port [--token-stdin] [--namespace ns]
                 [--insecure] [--base-image ref] [--use]
  ctx delete <name> | rename <old> <new>

records — the verb first, the kind second (kubectl's grammar); a
target is "<kind> <id>" or "kind/id"; a run is a record too (kind
"run"). Five dimensions each:
  get all|<kind> [-l k=v] [-p phase] [--owner ref] [-w]
  get <kind> <id>                   state: the full record
  events  <kind> <id> [--follow]    its own history
  logs    <kind> <id> [--follow]    telemetry
  metrics <kind> <id>
  trace   <kind> <id>
  tree <owner-ref>
  delete   <kind> <id>
  transfer <kind> <id> <new-owner> [--keep 72h]
  invoke   <kind> <id> <command> [--data JSON]

runs — the lifecycle verbs live under run (kubectl rollout's stance):
  run start <pipeline> [--run-id id] [--params JSON] [--image ref]
            [-l k=v] [--watch]
  run watch | result | cancel <run-id>
  run list [--status S] [-l k=v] [-w]     (same as: get run)

installation:
  pipeline show <pipeline-id>
  secret set <name> [--value v]     (no --value: read from stdin)
  secret list | delete <name>
  ns list | create <name> [--retention-days n]

project:
  init <name>                       scaffold a pipeline project
  version

every read command takes -o table|json (default table) and --jq EXPR;
every network command takes --context, --config, and -n (per-call
namespace for cluster-wide admin tokens); flags parse on either side
of positionals.`

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
