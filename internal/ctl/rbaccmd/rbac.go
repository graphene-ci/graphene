// Package rbaccmd holds the two authorization surfaces that generic
// verbs cannot carry: issuing a token, whose VALUE is shown once and
// never stored, and asking who the caller is. Roles, bindings and
// accounts themselves are ordinary records — declared with apply,
// changed with invoke, read with get.
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

// NewAccount builds the `account` tree.
func NewAccount(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "account",
		Short: "Issue a service account's token — the one thing generic verbs cannot do",
	}

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

	cmd.AddCommand(token)
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
