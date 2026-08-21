// Package ctxcmd manages the shared connection contexts: login as the
// one-step setup, ctx as the kubeconfig-style editor.
package ctxcmd

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// nameCompletion completes context names from the local file.
func nameCompletion(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	cfg, err := cliconfig.Load()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, cobra.ShellCompDirectiveNoFileComp
}

// NewLogin builds `login`.
func NewLogin(f *cmdutil.Factory) *cobra.Command {
	var (
		server, token, name, namespace, baseImage string
		tokenStdin, insecure                      bool
	)
	cmd := &cobra.Command{
		Use:   "login --server host:port --token-stdin",
		Short: "Verify a token, save the context, and switch to it",
		Long: `Login verifies the server and the token with a handshake BEFORE
writing anything, then saves the context and makes it current. A
namespaced token pins the context to its own namespace; a cluster-wide
token keeps your --namespace pick.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if server == "" {
				return fmt.Errorf("login needs --server host:port")
			}
			if tokenStdin {
				raw, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				token = strings.TrimSpace(string(raw))
			}
			if token == "" {
				return fmt.Errorf("login needs a token: --token-stdin (preferred) or --token")
			}
			cc := cliconfig.Context{
				Server: server, Token: token, Namespace: namespace,
				Insecure: insecure, BaseImage: baseImage,
			}
			// The handshake BEFORE anything is written: a bad server or
			// token never lands in the file.
			d, err := cmdutil.DialContext(cc)
			if err != nil {
				return err
			}
			who, err := d.Ns.Whoami(cmd.Context(), connect.NewRequest(&managementv1.WhoamiRequest{}))
			if err != nil {
				return fmt.Errorf("handshake with %s failed: %w", server, err)
			}
			role, scope := who.Msg.GetRole(), who.Msg.GetNamespace()
			if cc.Namespace == "" && scope != "*" {
				cc.Namespace = scope
			}
			ctxName := name
			if ctxName == "" {
				ctxName = server
				if host, _, ok := strings.Cut(server, ":"); ok && host != "" {
					ctxName = host
				}
			}
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]cliconfig.Context{}
			}
			cfg.Contexts[ctxName] = cc
			cfg.Current = ctxName
			if err := cliconfig.Save(cfg); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "logged in: context %s, role %s, namespace %s\n", ctxName, role, scope)
			return nil
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&server, "server", "", "the installation's door, host:port")
	fl.StringVar(&token, "token", "", "access token (prefer --token-stdin)")
	fl.BoolVar(&tokenStdin, "token-stdin", false, "read the token from stdin")
	fl.StringVar(&name, "name", "", "context name (default: the server's host)")
	fl.StringVar(&namespace, "namespace", "", "namespace to work in (default: the token's own scope)")
	fl.BoolVar(&insecure, "insecure", false, "plaintext connection (dev contours)")
	fl.StringVar(&baseImage, "base-image", "", "base image override for self-built workers")
	return cmd
}

// NewCtx builds the `ctx` command tree.
func NewCtx(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ctx",
		Short: "Contexts: list, show, current, use, set, delete, rename",
	}
	cmd.AddCommand(newList(), newShow(f), newCurrent(f), newUse(f), newSet(), newDelete(), newRename())
	return cmd
}

// applyConfig honors the persistent --config on the file-editing verbs.
func applyConfig(f *cmdutil.Factory) error {
	if f == nil || f.Config == "" {
		return nil
	}
	return os.Setenv(cliconfig.EnvConfig, f.Config)
}

func newList() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the contexts; * marks the current one",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
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
			cmdutil.Table([]string{" ", "NAME", "SERVER", "NAMESPACE"}, rows)
			return nil
		},
	}
}

func newShow(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "The EFFECTIVE connection (file + env overlays), token masked",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := applyConfig(f); err != nil {
				return err
			}
			cc, name, err := cliconfig.Resolve(f.CtxName)
			if err != nil {
				return err
			}
			path, _ := cliconfig.Path()
			fmt.Fprintf(cmdutil.Out, "context   %s\nconfig    %s\nserver    %s\nnamespace %s\ninsecure  %v\ntoken     %s\n",
				name, path, cc.Server, cc.Namespace, cc.Insecure, maskToken(cc.Token))
			if cc.BaseImage != "" {
				fmt.Fprintf(cmdutil.Out, "baseImage %s\n", cc.BaseImage)
			}
			return nil
		},
	}
}

func newCurrent(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Print the current context's name",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := applyConfig(f); err != nil {
				return err
			}
			_, name, err := cliconfig.Resolve("")
			if err != nil {
				return err
			}
			fmt.Fprintln(cmdutil.Out, name)
			return nil
		},
	}
}

func newUse(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:               "use <name>",
		Short:             "Switch the current context",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: nameCompletion,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := applyConfig(f); err != nil {
				return err
			}
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[args[0]]; !ok {
				return fmt.Errorf("context %q is not defined", args[0])
			}
			cfg.Current = args[0]
			if err := cliconfig.Save(cfg); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "current context: %s\n", args[0])
			return nil
		},
	}
}

func newSet() *cobra.Command {
	var (
		server, token, namespace, baseImage string
		tokenStdin, insecure, use           bool
	)
	cmd := &cobra.Command{
		Use:   "set <name>",
		Short: "Create or update a context (only the passed flags change)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]cliconfig.Context{}
			}
			cc, existed := cfg.Contexts[args[0]]
			if tokenStdin {
				raw, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				token = strings.TrimSpace(string(raw))
			}
			changed := cmd.Flags().Changed
			if changed("server") {
				cc.Server = server
			}
			if changed("token") || changed("token-stdin") {
				cc.Token = token
			}
			if changed("namespace") {
				cc.Namespace = namespace
			}
			if changed("insecure") {
				cc.Insecure = insecure
			}
			if changed("base-image") {
				cc.BaseImage = baseImage
			}
			if cc.Server == "" {
				return fmt.Errorf("context %q needs --server", args[0])
			}
			cfg.Contexts[args[0]] = cc
			// The very first context becomes current — one command to a
			// working setup.
			if use || cfg.Current == "" {
				cfg.Current = args[0]
			}
			if err := cliconfig.Save(cfg); err != nil {
				return err
			}
			verb := "updated"
			if !existed {
				verb = "created"
			}
			fmt.Fprintf(os.Stderr, "context %s %s\n", args[0], verb)
			return nil
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&server, "server", "", "the installation's door, host:port")
	fl.StringVar(&token, "token", "", "access token (prefer --token-stdin)")
	fl.BoolVar(&tokenStdin, "token-stdin", false, "read the token from stdin")
	fl.StringVar(&namespace, "namespace", "", "namespace the context works in")
	fl.BoolVar(&insecure, "insecure", false, "plaintext connection (dev contours)")
	fl.StringVar(&baseImage, "base-image", "", "base image override for self-built workers")
	fl.BoolVar(&use, "use", false, "also make it the current context")
	return cmd
}

func newDelete() *cobra.Command {
	return &cobra.Command{
		Use:               "delete <name>",
		Short:             "Delete a context",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: nameCompletion,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			if _, ok := cfg.Contexts[args[0]]; !ok {
				return fmt.Errorf("context %q is not defined", args[0])
			}
			delete(cfg.Contexts, args[0])
			if cfg.Current == args[0] {
				cfg.Current = ""
			}
			if err := cliconfig.Save(cfg); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "context %s deleted\n", args[0])
			return nil
		},
	}
}

func newRename() *cobra.Command {
	return &cobra.Command{
		Use:               "rename <old> <new>",
		Short:             "Rename a context",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: nameCompletion,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := cliconfig.Load()
			if err != nil {
				return err
			}
			cc, ok := cfg.Contexts[args[0]]
			if !ok {
				return fmt.Errorf("context %q is not defined", args[0])
			}
			if _, taken := cfg.Contexts[args[1]]; taken {
				return fmt.Errorf("context %q already exists", args[1])
			}
			cfg.Contexts[args[1]] = cc
			delete(cfg.Contexts, args[0])
			if cfg.Current == args[0] {
				cfg.Current = args[1]
			}
			if err := cliconfig.Save(cfg); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "context %s -> %s\n", args[0], args[1])
			return nil
		},
	}
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
