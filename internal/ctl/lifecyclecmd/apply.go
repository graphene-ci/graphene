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
		Args: cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
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

// NewKinds builds `kinds`: what this installation can declare and ask.
func NewKinds(f *cmdutil.Factory) *cobra.Command {
	var verbose bool
	cmd := &cobra.Command{
		Use:   "kinds",
		Short: "What this installation can declare and command",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Resources.Kinds(cmd.Context(), connect.NewRequest(&managementv1.KindsRequest{}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "KIND\tAPPLY\tCOMMANDS\n")
			for _, k := range resp.Msg.GetKinds() {
				declarable := ""
				if k.GetDeclarable() {
					declarable = "*"
				}
				names := make([]string, 0, len(k.GetCommands()))
				for _, c := range k.GetCommands() {
					names = append(names, c.GetName())
				}
				fmt.Fprintf(cmdutil.Out, "%s\t%s\t%s\n", k.GetName(), declarable, strings.Join(names, ", "))
				if verbose {
					fmt.Fprintf(cmdutil.Out, "  %s\n", k.GetDescription())
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print what each kind is for")
	return cmd
}
