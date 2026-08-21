package ctl

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"
)

// cmdCtx manages the shared connection contexts, kubeconfig-style:
// list/show/current to read, use to switch, set/delete/rename to edit.
// Every verb honors --config (else $GRAPHENE_CONFIG, else the default
// path).
func cmdCtx(args []string) error {
	word, rest, err := need(args, "list, show, current, use, set, delete, rename")
	if err != nil {
		return err
	}
	switch word {
	case "list":
		return ctxList(rest)
	case "show":
		return ctxShow(rest)
	case "current":
		return ctxCurrent(rest)
	case "use":
		return ctxUse(rest)
	case "set":
		return ctxSet(rest)
	case "delete":
		return ctxDelete(rest)
	case "rename":
		return ctxRename(rest)
	default:
		return fmt.Errorf("ctx %q: want list, show, current, use, set, delete or rename", word)
	}
}

// ctxFlags is the config-file pick shared by the ctx verbs (they edit
// the file, they do not dial).
func ctxFlags(fs *flag.FlagSet) *string {
	return fs.String("config", "", "config file (default: $"+cliconfig.EnvConfig+", else ~/.config/graphene/config.yaml)")
}

func applyConfigFlag(config string) error {
	if config == "" {
		return nil
	}
	return os.Setenv(cliconfig.EnvConfig, config)
}

func ctxList(args []string) error {
	fs := flag.NewFlagSet("ctx list", flag.ExitOnError)
	config := ctxFlags(fs)
	if _, err := parseMixed(fs, args); err != nil {
		return err
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	cfg, err := cliconfig.Load()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]string, 0, len(names))
	for _, name := range names {
		mark := " "
		if name == cfg.Current {
			mark = "*"
		}
		cc := cfg.Contexts[name]
		rows = append(rows, []string{mark, name, cc.Server, cc.Namespace})
	}
	table([]string{" ", "NAME", "SERVER", "NAMESPACE"}, rows)
	return nil
}

func ctxShow(args []string) error {
	fs := flag.NewFlagSet("ctx show", flag.ExitOnError)
	config := ctxFlags(fs)
	ctxName := fs.String("context", "", "context to show (default: the resolved one)")
	if _, err := parseMixed(fs, args); err != nil {
		return err
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	// Resolve shows the EFFECTIVE connection: file + env overlays —
	// what a command would actually use. The token never prints.
	cc, name, err := cliconfig.Resolve(*ctxName)
	if err != nil {
		return err
	}
	path, _ := cliconfig.Path()
	fmt.Fprintf(out, "context   %s\nconfig    %s\nserver    %s\nnamespace %s\ninsecure  %v\ntoken     %s\n",
		name, path, cc.Server, cc.Namespace, cc.Insecure, maskToken(cc.Token))
	if cc.BaseImage != "" {
		fmt.Fprintf(out, "baseImage %s\n", cc.BaseImage)
	}
	return nil
}

func ctxCurrent(args []string) error {
	fs := flag.NewFlagSet("ctx current", flag.ExitOnError)
	config := ctxFlags(fs)
	if _, err := parseMixed(fs, args); err != nil {
		return err
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	_, name, err := cliconfig.Resolve("")
	if err != nil {
		return err
	}
	fmt.Fprintln(out, name)
	return nil
}

func ctxUse(args []string) error {
	fs := flag.NewFlagSet("ctx use", flag.ExitOnError)
	config := ctxFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: ctx use <name>")
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	cfg, err := cliconfig.Load()
	if err != nil {
		return err
	}
	if _, ok := cfg.Contexts[pos[0]]; !ok {
		return fmt.Errorf("context %q is not defined", pos[0])
	}
	cfg.Current = pos[0]
	if err := cliconfig.Save(cfg); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "current context: %s\n", pos[0])
	return nil
}

// ctxSet creates or updates a context. Only the flags that were passed
// change; an update keeps the rest. The token comes from --token or,
// safer for shells with history, --token-stdin.
func ctxSet(args []string) error {
	fs := flag.NewFlagSet("ctx set", flag.ExitOnError)
	config := ctxFlags(fs)
	server := fs.String("server", "", "the installation's door, host:port")
	token := fs.String("token", "", "access token (prefer --token-stdin)")
	tokenStdin := fs.Bool("token-stdin", false, "read the token from stdin")
	namespace := fs.String("namespace", "", "namespace the context works in")
	insecure := fs.Bool("insecure", false, "plaintext connection (dev contours)")
	baseImage := fs.String("base-image", "", "base image override for self-built workers")
	use := fs.Bool("use", false, "also make it the current context")
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: ctx set <name> [--server ...] [--token-stdin]")
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	cfg, err := cliconfig.Load()
	if err != nil {
		return err
	}
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]cliconfig.Context{}
	}
	cc, existed := cfg.Contexts[pos[0]]

	if *tokenStdin {
		raw, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		*token = strings.TrimSpace(string(raw))
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["server"] {
		cc.Server = *server
	}
	if set["token"] || set["token-stdin"] {
		cc.Token = *token
	}
	if set["namespace"] {
		cc.Namespace = *namespace
	}
	if set["insecure"] {
		cc.Insecure = *insecure
	}
	if set["base-image"] {
		cc.BaseImage = *baseImage
	}
	if cc.Server == "" {
		return fmt.Errorf("context %q needs --server", pos[0])
	}
	cfg.Contexts[pos[0]] = cc
	// The very first context becomes current — one command to a
	// working setup.
	if *use || cfg.Current == "" {
		cfg.Current = pos[0]
	}
	if err := cliconfig.Save(cfg); err != nil {
		return err
	}
	verb := "updated"
	if !existed {
		verb = "created"
	}
	fmt.Fprintf(os.Stderr, "context %s %s\n", pos[0], verb)
	return nil
}

func ctxDelete(args []string) error {
	fs := flag.NewFlagSet("ctx delete", flag.ExitOnError)
	config := ctxFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: ctx delete <name>")
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	cfg, err := cliconfig.Load()
	if err != nil {
		return err
	}
	if _, ok := cfg.Contexts[pos[0]]; !ok {
		return fmt.Errorf("context %q is not defined", pos[0])
	}
	delete(cfg.Contexts, pos[0])
	if cfg.Current == pos[0] {
		cfg.Current = ""
	}
	if err := cliconfig.Save(cfg); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "context %s deleted\n", pos[0])
	return nil
}

func ctxRename(args []string) error {
	fs := flag.NewFlagSet("ctx rename", flag.ExitOnError)
	config := ctxFlags(fs)
	pos, err := parseMixed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: ctx rename <old> <new>")
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	cfg, err := cliconfig.Load()
	if err != nil {
		return err
	}
	cc, ok := cfg.Contexts[pos[0]]
	if !ok {
		return fmt.Errorf("context %q is not defined", pos[0])
	}
	if _, taken := cfg.Contexts[pos[1]]; taken {
		return fmt.Errorf("context %q already exists", pos[1])
	}
	cfg.Contexts[pos[1]] = cc
	delete(cfg.Contexts, pos[0])
	if cfg.Current == pos[0] {
		cfg.Current = pos[1]
	}
	if err := cliconfig.Save(cfg); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "context %s -> %s\n", pos[0], pos[1])
	return nil
}

// maskToken keeps enough to recognize a token, never enough to use it.
func maskToken(token string) string {
	if token == "" {
		return "(none)"
	}
	if len(token) <= 8 {
		return "****"
	}
	return token[:4] + "…" + token[len(token)-2:]
}
