package ctl

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"connectrpc.com/connect"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// cmdLogin is the one-step setup: verify the server and the token with
// a Whoami handshake, then save the context and make it current. The
// token comes from --token-stdin (preferred) or --token.
func cmdLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	config := ctxFlags(fs)
	server := fs.String("server", "", "the installation's door, host:port")
	token := fs.String("token", "", "access token (prefer --token-stdin)")
	tokenStdin := fs.Bool("token-stdin", false, "read the token from stdin")
	name := fs.String("name", "", "context name (default: the server's host)")
	namespace := fs.String("namespace", "", "namespace to work in (default: the token's own scope)")
	insecure := fs.Bool("insecure", false, "plaintext connection (dev contours)")
	baseImage := fs.String("base-image", "", "base image override for self-built workers")
	if _, err := parseMixed(fs, args); err != nil {
		return err
	}
	if err := applyConfigFlag(*config); err != nil {
		return err
	}
	if *server == "" {
		return fmt.Errorf("login needs --server host:port")
	}
	if *tokenStdin {
		raw, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		*token = strings.TrimSpace(string(raw))
	}
	if *token == "" {
		return fmt.Errorf("login needs a token: --token-stdin (preferred) or --token")
	}

	cc := cliconfig.Context{
		Server:    *server,
		Token:     *token,
		Namespace: *namespace,
		Insecure:  *insecure,
		BaseImage: *baseImage,
	}
	// The handshake BEFORE anything is written: a bad server or token
	// never lands in the file.
	d, err := dialContext(cc)
	if err != nil {
		return err
	}
	who, err := d.Ns.Whoami(ctx, connect.NewRequest(&managementv1.WhoamiRequest{}))
	if err != nil {
		return fmt.Errorf("handshake with %s failed: %w", *server, err)
	}
	role, scope := who.Msg.GetRole(), who.Msg.GetNamespace()
	// A namespaced token pins the context to its own namespace unless
	// the caller picked one; a cluster-wide token ("*") keeps the pick.
	if cc.Namespace == "" && scope != "*" {
		cc.Namespace = scope
	}

	ctxName := *name
	if ctxName == "" {
		ctxName = *server
		if host, _, ok := strings.Cut(*server, ":"); ok && host != "" {
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
	fmt.Fprintf(os.Stderr, "logged in: context %s, role %s, namespace %s\n",
		ctxName, role, scope)
	return nil
}
