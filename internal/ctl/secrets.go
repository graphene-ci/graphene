package ctl

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"connectrpc.com/connect"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// cmdSecret manages secrets: only names travel and only names print.
func cmdSecret(ctx context.Context, args []string) error {
	word, rest, err := need(args, "set, list, delete")
	if err != nil {
		return err
	}
	switch word {
	case "set":
		fs := flag.NewFlagSet("secret set", flag.ExitOnError)
		co := commonFlags(fs)
		value := fs.String("value", "", "secret value (omit to read from stdin)")
		pos, err := parseMixed(fs, rest)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return fmt.Errorf("usage: secret set <name>")
		}
		v := *value
		if v == "" {
			raw, err := io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			v = strings.TrimRight(string(raw), "\n")
		}
		if v == "" {
			return fmt.Errorf("empty secret value")
		}
		d, err := co.dial()
		if err != nil {
			return err
		}
		if _, err := d.Secrets.SetSecret(ctx, connect.NewRequest(&managementv1.SetSecretRequest{
			Name: pos[0], Value: v,
		})); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "secret %s set\n", pos[0])
		return nil
	case "list":
		fs := flag.NewFlagSet("secret list", flag.ExitOnError)
		co := commonFlags(fs)
		_, err := parseMixed(fs, rest)
		if err != nil {
			return err
		}
		d, err := co.dial()
		if err != nil {
			return err
		}
		resp, err := d.Secrets.ListSecrets(ctx, connect.NewRequest(&managementv1.ListSecretsRequest{}))
		if err != nil {
			return err
		}
		if done, err := co.emit(resp.Msg); done || err != nil {
			return err
		}
		for _, name := range resp.Msg.GetNames() {
			fmt.Fprintln(out, name)
		}
		return nil
	case "delete":
		fs := flag.NewFlagSet("secret delete", flag.ExitOnError)
		co := commonFlags(fs)
		pos, err := parseMixed(fs, rest)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return fmt.Errorf("usage: secret delete <name>")
		}
		d, err := co.dial()
		if err != nil {
			return err
		}
		if _, err := d.Secrets.DeleteSecret(ctx, connect.NewRequest(&managementv1.DeleteSecretRequest{Name: pos[0]})); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "secret %s deleted\n", pos[0])
		return nil
	default:
		return fmt.Errorf("secret %q: want set, list or delete", word)
	}
}

// cmdNs manages namespaces.
func cmdNs(ctx context.Context, args []string) error {
	word, rest, err := need(args, "list, create")
	if err != nil {
		return err
	}
	switch word {
	case "list":
		fs := flag.NewFlagSet("ns list", flag.ExitOnError)
		co := commonFlags(fs)
		_, err := parseMixed(fs, rest)
		if err != nil {
			return err
		}
		d, err := co.dial()
		if err != nil {
			return err
		}
		resp, err := d.Ns.ListNamespaces(ctx, connect.NewRequest(&managementv1.ListNamespacesRequest{}))
		if err != nil {
			return err
		}
		if done, err := co.emit(resp.Msg); done || err != nil {
			return err
		}
		for _, name := range resp.Msg.GetNames() {
			fmt.Fprintln(out, name)
		}
		return nil
	case "create":
		fs := flag.NewFlagSet("ns create", flag.ExitOnError)
		co := commonFlags(fs)
		retention := fs.Int("retention-days", 0, "workflow retention (0 — the server default)")
		pos, err := parseMixed(fs, rest)
		if err != nil {
			return err
		}
		if len(pos) != 1 {
			return fmt.Errorf("usage: ns create <name>")
		}
		d, err := co.dial()
		if err != nil {
			return err
		}
		if _, err := d.Ns.CreateNamespace(ctx, connect.NewRequest(&managementv1.CreateNamespaceRequest{
			Name: pos[0], RetentionDays: int32(*retention), //nolint:gosec // a small flag value
		})); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "namespace %s created\n", pos[0])
		return nil
	default:
		return fmt.Errorf("ns %q: want list or create", word)
	}
}
