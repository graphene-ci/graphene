package ctl

// Shell completion: `completion bash|zsh|fish` prints the hook, the
// hidden `__complete` answers it. Static words come from the grammar;
// kinds, ids, and context names are looked up live (best effort, a
// short timeout, silent on failure — completion must never nag).

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

const (
	bashHook = `# graphene completion — add to ~/.bashrc:
#   source <(graphenectl completion bash)
complete -o default -C 'graphenectl __complete' graphenectl
`
	zshHook = `# graphene completion — add to ~/.zshrc:
#   source <(graphenectl completion zsh)
autoload -U +X bashcompinit && bashcompinit
complete -o default -C 'graphenectl __complete' graphenectl
`
	fishHook = `# graphene completion — add to ~/.config/fish/config.fish:
#   graphenectl completion fish | source
complete -c graphenectl -f -a '(env COMP_LINE=(commandline -cp) graphenectl __complete)'
`
)

// cmdCompletion prints the shell hook.
func cmdCompletion(args []string) error {
	shell, _, err := need(args, "bash, zsh, fish")
	if err != nil {
		return err
	}
	switch shell {
	case "bash":
		fmt.Fprint(out, bashHook)
	case "zsh":
		fmt.Fprint(out, zshHook)
	case "fish":
		fmt.Fprint(out, fishHook)
	default:
		return fmt.Errorf("completion %q: want bash, zsh or fish", shell)
	}
	return nil
}

// The grammar the completer walks.
var (
	topCommands = []string{
		"get", "tree", "delete", "transfer", "invoke",
		"events", "logs", "metrics", "trace",
		"run", "login", "ctx", "pipeline", "secret", "ns",
		"init", "version", "completion", "help",
	}
	targetVerbs = map[string]bool{
		"get": true, "tree": true, "delete": true, "transfer": true, "invoke": true,
		"events": true, "logs": true, "metrics": true, "trace": true,
	}
	subcommands = map[string][]string{
		"run":        {"start", "watch", "result", "cancel", "list"},
		"ctx":        {"list", "show", "current", "use", "set", "delete", "rename"},
		"secret":     {"set", "list", "delete"},
		"ns":         {"list", "create"},
		"pipeline":   {"show"},
		"completion": {"bash", "zsh", "fish"},
	}
	commonFlagWords = []string{"--context", "--config", "-n", "-o", "--jq"}
	flagWords       = map[string][]string{
		"get":      {"-l", "-p", "--owner", "-w", "--chunk-size"},
		"transfer": {"--keep"},
		"invoke":   {"--data", "--data-file"},
		"events":   {"--follow"},
		"logs":     {"--follow"},
		"run":      {"--run-id", "--params", "--params-file", "--image", "--watch", "--plain", "--collapse", "--logs", "-l", "-p", "-w", "--chunk-size"},
		"login":    {"--server", "--token", "--token-stdin", "--name", "--namespace", "--insecure", "--base-image"},
		"ctx":      {"--server", "--token", "--token-stdin", "--namespace", "--insecure", "--base-image", "--use"},
		"secret":   {"--value", "--value-file"},
		"ns":       {"--retention-days"},
	}
	// Kinds every installation has, offered even when the server is
	// unreachable; live kinds join them.
	builtinKinds = []string{"agent", "artifact", "stand", "pipeline", "run"}
)

// cmdComplete answers one completion request. The shell's COMP_LINE
// carries the buffer; candidates go one per line.
func cmdComplete() {
	line := os.Getenv("COMP_LINE")
	if line == "" {
		return
	}
	words := strings.Fields(line)
	// A trailing space means the last word is done — complete a new one.
	if strings.HasSuffix(line, " ") {
		words = append(words, "")
	}
	if len(words) < 2 {
		return
	}
	words = words[1:] // drop the binary
	cur := words[len(words)-1]
	prior := words[:len(words)-1]
	for _, c := range candidates(prior, cur) {
		if strings.HasPrefix(c, cur) {
			fmt.Fprintln(out, c)
		}
	}
}

// candidates picks the word set for the position; positional words
// ignore flags typed in between.
func candidates(prior []string, cur string) []string {
	if strings.HasPrefix(cur, "-") {
		if len(prior) == 0 {
			return nil
		}
		return append(flagWords[prior[0]], commonFlagWords...)
	}
	pos := positionals(prior)
	if len(pos) == 0 {
		return topCommands
	}
	cmd := pos[0]
	switch {
	case targetVerbs[cmd] && len(pos) == 1:
		kinds := liveKinds()
		if cmd == "get" {
			kinds = append(kinds, "all")
		}
		return kinds
	case targetVerbs[cmd] && len(pos) == 2 && !strings.Contains(pos[1], "/"):
		return liveIds(pos[1])
	case cmd == "run" && len(pos) == 1:
		return subcommands["run"]
	case cmd == "run" && len(pos) == 2 && (pos[1] == "watch" || pos[1] == "result" || pos[1] == "cancel"):
		return liveIds("run")
	case cmd == "ctx" && len(pos) == 1:
		return subcommands["ctx"]
	case cmd == "ctx" && len(pos) == 2 && (pos[1] == "use" || pos[1] == "delete" || pos[1] == "rename"):
		return contextNames()
	default:
		if len(pos) == 1 {
			return subcommands[cmd]
		}
	}
	return nil
}

// positionals filters flags (and their values) out of the words.
func positionals(words []string) []string {
	valueFlags := map[string]bool{
		"--context": true, "--config": true, "-n": true, "-o": true, "--jq": true,
		"-l": true, "-p": true, "--owner": true, "--keep": true, "--data": true,
		"--run-id": true, "--params": true, "--image": true, "--server": true,
		"--token": true, "--name": true, "--namespace": true, "--base-image": true,
		"--value": true, "--value-file": true, "--retention-days": true,
		"--params-file": true, "--data-file": true, "--chunk-size": true, "--logs": true,
	}
	var pos []string
	skip := false
	for _, w := range words {
		switch {
		case skip:
			skip = false
		case strings.HasPrefix(w, "-"):
			if !strings.Contains(w, "=") && valueFlags[w] {
				skip = true
			}
		default:
			pos = append(pos, w)
		}
	}
	return pos
}

// contextNames reads the local config — no network.
func contextNames() []string {
	cfg, err := cliconfig.Load()
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// completionDoor dials with the resolved context, silently.
func completionDoor() (*door, context.Context, context.CancelFunc) {
	cc, _, err := cliconfig.Resolve("")
	if err != nil {
		return nil, nil, nil
	}
	d, err := dialContext(cc)
	if err != nil {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	return d, ctx, cancel
}

// liveKinds unions the built-in kinds with what the installation
// actually holds right now.
func liveKinds() []string {
	seen := map[string]bool{}
	for _, k := range builtinKinds {
		seen[k] = true
	}
	if d, ctx, cancel := completionDoor(); d != nil {
		defer cancel()
		if resp, err := d.Resources.List(ctx, connect.NewRequest(&managementv1.ListRequest{
			Selector: &managementv1.Selector{},
		})); err == nil {
			for _, r := range resp.Msg.GetResources() {
				if r.GetKind() != "" {
					seen[r.GetKind()] = true
				}
			}
		}
	}
	kinds := make([]string, 0, len(seen))
	for k := range seen {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return kinds
}

// liveIds lists the ids of one kind.
func liveIds(kind string) []string {
	d, ctx, cancel := completionDoor()
	if d == nil {
		return nil
	}
	defer cancel()
	var ids []string
	if kind == "run" {
		resp, err := d.Runs.ListRuns(ctx, connect.NewRequest(&managementv1.ListRunsRequest{}))
		if err != nil {
			return nil
		}
		for _, r := range resp.Msg.GetRuns() {
			ids = append(ids, r.GetRunId())
		}
	} else {
		resp, err := d.Resources.List(ctx, connect.NewRequest(&managementv1.ListRequest{
			Selector: &managementv1.Selector{Kind: kind},
		}))
		if err != nil {
			return nil
		}
		for _, r := range resp.Msg.GetResources() {
			if _, id, ok := strings.Cut(r.GetRef(), "/"); ok {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	return ids
}
