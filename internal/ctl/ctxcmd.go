package ctl

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"
)

// cmdCtx manages the shared connection contexts.
func cmdCtx(args []string) error {
	word, rest, err := need(args, "list, show, use")
	if err != nil {
		return err
	}
	cfg, err := cliconfig.Load()
	if err != nil {
		return err
	}
	switch word {
	case "list":
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
	case "show":
		cc, name, err := cliconfig.Resolve("")
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "context %s\nserver %s\nnamespace %s\ninsecure %v\n",
			name, cc.Server, cc.Namespace, cc.Insecure)
		return nil
	case "use":
		if len(rest) != 1 {
			return fmt.Errorf("usage: ctx use <name>")
		}
		if _, ok := cfg.Contexts[rest[0]]; !ok {
			return fmt.Errorf("context %q is not defined", rest[0])
		}
		cfg.Current = rest[0]
		raw, err := yaml.Marshal(cfg)
		if err != nil {
			return err
		}
		path, err := cliconfig.Path()
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "current context: %s\n", rest[0])
		return nil
	default:
		return fmt.Errorf("ctx %q: want list, show or use", word)
	}
}
