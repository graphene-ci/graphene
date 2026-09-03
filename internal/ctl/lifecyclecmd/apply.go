package lifecyclecmd

// apply and kinds: creation through one verb, and the vocabulary
// discovered rather than remembered.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// kindCompletion offers the kinds the installation knows.
func kindCompletion(f *cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return f.LiveKinds(), cobra.ShellCompDirectiveNoFileComp
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// declaration is what a file declares: one record.
type declaration struct {
	Kind   string            `yaml:"kind" json:"kind"`
	Id     string            `yaml:"id" json:"id"`
	Labels map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Spec   any               `yaml:"spec,omitempty" json:"spec,omitempty"`
}

// NewApply builds `apply`: declare a record of any kind, from flags or
// from a file.
func NewApply(f *cmdutil.Factory) *cobra.Command {
	var file, specJSON string
	var labels map[string]string
	cmd := &cobra.Command{
		Use:   "apply [kind] [id]",
		Short: "Declare a record of any kind (--spec, or -f file.yaml)",
		Long: "Declare a record. Either name the kind and id with --spec,\n" +
			"or point at a YAML/JSON file holding kind, id and spec.\n" +
			"`graphenectl kinds` lists what this installation can declare.",
		Args:              cobra.MaximumNArgs(2),
		ValidArgsFunction: kindCompletion(f),
		RunE: func(cmd *cobra.Command, args []string) error {
			// A kind and an id, no spec, a human on the other end: walk
			// the KIND's own schema. Which fields it has is the
			// installation's answer, not this client's.
			if len(args) == 2 && file == "" && specJSON == "" && cmdutil.StdinIsTerminal() {
				entry, err := f.KindEntryOf(cmd.Context(), args[0])
				if err != nil {
					return err
				}
				if schema := cmdutil.ParseSchema(entry.SpecSchema); schema != nil {
					raw, perr := cmdutil.PromptSchema(os.Stdin, args[0]+" spec", schema)
					if perr != nil {
						return perr
					}
					specJSON = string(raw)
				}
			}
			decls, err := declarations(args, file, specJSON, labels)
			if err != nil {
				return err
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			for _, decl := range decls {
				spec, err := json.Marshal(decl.Spec)
				if err != nil {
					return fmt.Errorf("%s/%s: spec: %w", decl.Kind, decl.Id, err)
				}
				if decl.Spec == nil {
					spec = nil
				}
				resp, err := d.Resources.Apply(cmd.Context(), connect.NewRequest(&managementv1.ApplyRequest{
					Kind: decl.Kind, Id: decl.Id, Spec: spec, Labels: decl.Labels,
				}))
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "%s applied\n", resp.Msg.GetRef())
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "YAML or JSON file declaring one or more records")
	cmd.Flags().StringVar(&specJSON, "spec", "", "spec as raw JSON")
	cmd.Flags().StringToStringVarP(&labels, "label", "l", nil, "label k=v (repeatable)")
	return cmd
}

// declarations resolves what to apply: a file, or the flags.
func declarations(args []string, file, specJSON string, labels map[string]string) ([]declaration, error) {
	if file != "" {
		raw, err := os.ReadFile(file) //nolint:gosec // the user's named file
		if err != nil {
			return nil, err
		}
		var out []declaration
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		for {
			var one declaration
			if err := dec.Decode(&one); err != nil {
				break
			}
			if one.Kind == "" || one.Id == "" {
				return nil, fmt.Errorf("%s: every declaration needs a kind and an id", file)
			}
			out = append(out, one)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("%s declares nothing", file)
		}
		return out, nil
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("name a kind and an id, or point at a file with -f")
	}
	decl := declaration{Kind: args[0], Id: args[1], Labels: labels}
	if specJSON != "" {
		if err := json.Unmarshal([]byte(specJSON), &decl.Spec); err != nil {
			return nil, fmt.Errorf("--spec: %w", err)
		}
	}
	return []declaration{decl}, nil
}

// NewKinds builds `kinds` — a FORMATTER over the dictionary records:
// the same data `get kind` and `get kind/<name>` read, laid out as the
// table a human wants. Nothing here is its own channel.
func NewKinds(f *cmdutil.Factory) *cobra.Command {
	var verbose bool
	cmd := &cobra.Command{
		Use:   "kinds",
		Short: "The dictionary: what this installation can declare and command",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			names := f.LiveKinds()
			if len(names) == 0 {
				return fmt.Errorf("the dictionary answered nothing — is the server reachable?")
			}
			if _, err := fmt.Fprintf(cmdutil.Out, "KIND\tORIGIN\tAPPLY\tRECORDS\tCOMMANDS\n"); err != nil {
				return err
			}
			for _, name := range names {
				e, err := f.KindEntryOf(cmd.Context(), name)
				if err != nil {
					if _, writeErr := fmt.Fprintf(cmdutil.Out, "%s\t?\t\t\t(%v)\n", name, err); writeErr != nil {
						return writeErr
					}
					continue
				}
				declarable := ""
				if e.Declarable {
					declarable = "*"
				}
				cmds := make([]string, 0, len(e.Commands))
				for _, c := range e.Commands {
					cmds = append(cmds, c.Name)
				}
				if _, err := fmt.Fprintf(cmdutil.Out, "%s\t%s\t%s\t%d\t%s\n", name, e.Origin, declarable, e.Records, strings.Join(cmds, ", ")); err != nil {
					return err
				}
				if verbose && e.Description != "" {
					if _, err := fmt.Fprintf(cmdutil.Out, "  %s\n", e.Description); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print what each kind is for")
	return cmd
}
