// Package rbaccmd is the ctl surface of authorization: roles, their
// bindings, service accounts and their tokens — plus the one question
// every operator asks first, "what am I allowed to do here".
package rbaccmd

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// NewRole builds the `role` tree.
func NewRole(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "role",
		Short: "Roles: what a set of rules allows",
	}

	var rules []string
	var description string
	put := &cobra.Command{
		Use:   "put <name>",
		Short: "Write a role: --rule 'get,list:pipeline,run' (repeatable)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(rules) == 0 {
				return fmt.Errorf("a role needs at least one --rule 'verbs:kinds'")
			}
			req := &managementv1.PutRoleRequest{Name: args[0], Description: description}
			for _, raw := range rules {
				verbs, kinds, ok := strings.Cut(raw, ":")
				if !ok {
					return fmt.Errorf("rule %q: want 'verbs:kinds', e.g. 'get,list:pipeline,run'", raw)
				}
				req.Rules = append(req.Rules, &managementv1.Rule{
					Verbs: splitList(verbs), Kinds: splitList(kinds),
				})
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if _, err := d.Rbac.PutRole(cmd.Context(), connect.NewRequest(req)); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "role %s written\n", args[0])
			return nil
		},
	}
	put.Flags().StringArrayVar(&rules, "rule", nil, "verbs:kinds, e.g. 'get,list,watch:pipeline,run' (repeatable)")
	put.Flags().StringVar(&description, "description", "", "what this role is for")

	list := &cobra.Command{
		Use:   "list",
		Short: "List roles",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Rbac.ListRoles(cmd.Context(), connect.NewRequest(&managementv1.ListRolesRequest{}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "ROLE\tBUILTIN\tRULES\n")
			for _, r := range resp.Msg.GetRoles() {
				builtin := ""
				if r.GetBuiltin() {
					builtin = "*"
				}
				parts := make([]string, 0, len(r.GetRules()))
				for _, rule := range r.GetRules() {
					parts = append(parts, strings.Join(rule.GetVerbs(), ",")+":"+strings.Join(rule.GetKinds(), ","))
				}
				fmt.Fprintf(cmdutil.Out, "%s\t%s\t%s\n", r.GetName(), builtin, strings.Join(parts, "  "))
			}
			return nil
		},
	}
	cmd.AddCommand(put, list)
	return cmd
}

// NewBinding builds the `binding` tree.
func NewBinding(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binding",
		Short: "Bindings: who gets a role",
	}

	var role, namespace string
	var subjects []string
	put := &cobra.Command{
		Use:   "put <name>",
		Short: "Grant a role to subjects (user:alice, group:platform, sa:ci)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if role == "" || len(subjects) == 0 {
				return fmt.Errorf("a binding needs --role and at least one --subject")
			}
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if _, err := d.Rbac.PutBinding(cmd.Context(), connect.NewRequest(&managementv1.PutBindingRequest{
				Name: args[0], Role: role, Subjects: subjects, Namespace: namespace,
			})); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "binding %s: %s -> %s\n", args[0], strings.Join(subjects, ", "), role)
			return nil
		},
	}
	put.Flags().StringVar(&role, "role", "", "role to grant")
	put.Flags().StringArrayVar(&subjects, "subject", nil, "user:<sub> | group:<name> | sa:<id> (repeatable)")
	put.Flags().StringVar(&namespace, "for-namespace", "", "namespace this binding applies in ('*' for all)")

	list := &cobra.Command{
		Use:   "list",
		Short: "List bindings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Rbac.ListBindings(cmd.Context(), connect.NewRequest(&managementv1.ListBindingsRequest{}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "ROLE\tNAMESPACE\tSUBJECTS\n")
			for _, b := range resp.Msg.GetBindings() {
				fmt.Fprintf(cmdutil.Out, "%s\t%s\t%s\n", b.GetRole(), b.GetNamespace(), strings.Join(b.GetSubjects(), ", "))
			}
			return nil
		},
	}
	cmd.AddCommand(put, list)
	return cmd
}

// NewAccount builds the `account` tree.
func NewAccount(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "account",
		Short: "Service accounts: the machines of this installation",
	}

	var description string
	create := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a service account",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if _, err := d.Rbac.CreateAccount(cmd.Context(), connect.NewRequest(&managementv1.CreateAccountRequest{
				Name: args[0], Description: description,
			})); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "account %s created; grant it a role with `graphenectl binding put`\n", args[0])
			return nil
		},
	}
	create.Flags().StringVar(&description, "description", "", "what this account is for")

	var ttl time.Duration
	var comment string
	token := &cobra.Command{
		Use:   "token <account>",
		Short: "Issue a token — printed ONCE, never stored",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Rbac.IssueToken(cmd.Context(), connect.NewRequest(&managementv1.IssueTokenRequest{
				Account: args[0], TtlSeconds: int64(ttl.Seconds()), Comment: comment,
			}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "token %s issued", resp.Msg.GetTokenId())
			if e := resp.Msg.GetExpires(); e != "" {
				fmt.Fprintf(os.Stderr, ", expires %s", e)
			}
			fmt.Fprintf(os.Stderr, " — this is the only time the value is shown\n")
			fmt.Fprintln(cmdutil.Out, resp.Msg.GetToken())
			return nil
		},
	}
	token.Flags().DurationVar(&ttl, "ttl", 0, "how long the token lives (0: until revoked)")
	token.Flags().StringVar(&comment, "comment", "", "what this token is for")

	revoke := &cobra.Command{
		Use:   "revoke <account> <token-id>",
		Short: "Revoke one token",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			if _, err := d.Rbac.RevokeToken(cmd.Context(), connect.NewRequest(&managementv1.RevokeTokenRequest{
				Account: args[0], TokenId: args[1],
			})); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "token %s revoked\n", args[1])
			return nil
		},
	}
	cmd.AddCommand(create, token, revoke)
	return cmd
}

// NewWhoAmI builds `whoami`: who the caller is and what they may do.
func NewWhoAmI(f *cmdutil.Factory) *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Who this context speaks as, and what it may do",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := f.Dial()
			if err != nil {
				return err
			}
			resp, err := d.Rbac.WhoAmI(cmd.Context(), connect.NewRequest(&managementv1.WhoAmIRequest{}))
			if err != nil {
				return err
			}
			if done, err := f.Emit(resp.Msg); done || err != nil {
				return err
			}
			fmt.Fprintf(cmdutil.Out, "subject   %s\n", resp.Msg.GetSubject())
			if g := resp.Msg.GetGroups(); len(g) > 0 {
				fmt.Fprintf(cmdutil.Out, "groups    %s\n", strings.Join(g, ", "))
			}
			fmt.Fprintf(cmdutil.Out, "namespace %s\n", resp.Msg.GetNamespace())
			if r := resp.Msg.GetRoles(); len(r) > 0 {
				fmt.Fprintf(cmdutil.Out, "roles     %s\n", strings.Join(r, ", "))
			}
			allowed := resp.Msg.GetAllowed()
			sort.Strings(allowed)
			fmt.Fprintf(cmdutil.Out, "allowed   %d verb/kind pairs\n", len(allowed))
			for _, a := range allowed {
				fmt.Fprintf(cmdutil.Out, "  %s\n", a)
			}
			return nil
		},
	}
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
