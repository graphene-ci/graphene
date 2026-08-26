// Package rbaccmd holds the two authorization surfaces that generic
// verbs cannot carry: issuing a token, whose VALUE is shown once and
// never stored, and asking who the caller is. Roles, bindings and
// accounts themselves are ordinary records — declared with apply,
// changed with invoke, read with get.
package rbaccmd

import (
	"encoding/json"
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

func mustJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

// builtinRoles are the roles every installation starts with; they live
// in the code, so no record answers for them.
var builtinRoles = []string{"admin", "developer", "viewer", "agent", "run"}

// record is one listed record with its state.
type record struct {
	id    string
	state []byte
}

// listRecords lists a kind through the general door and reads each
// one's state — the same two verbs any client would use.
func listRecords(cmd *cobra.Command, d *cmdutil.Door, kind string) ([]record, error) {
	list, err := d.Resources.List(cmd.Context(), connect.NewRequest(&managementv1.ListRequest{
		Selector: &managementv1.Selector{Kind: kind},
	}))
	if err != nil {
		return nil, err
	}
	out := make([]record, 0, len(list.Msg.GetResources()))
	for _, res := range list.Msg.GetResources() {
		got, err := d.Resources.Get(cmd.Context(), connect.NewRequest(&managementv1.GetRequest{Ref: res.GetRef()}))
		if err != nil {
			continue
		}
		id := strings.TrimPrefix(res.GetRef(), kind+"/")
		out = append(out, record{id: id, state: got.Msg.GetResource().GetState()})
	}
	return out, nil
}
