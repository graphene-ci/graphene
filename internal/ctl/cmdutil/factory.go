// Package cmdutil is the shared machinery of graphenectl's commands —
// kubectl's cmdutil stance: the Factory resolves the connection and
// dials the door, the output helpers render one way everywhere, the
// live lookups feed completion. Commands stay thin.
package cmdutil

import (
	"context"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"

	"github.com/graphene-ci/graphene/internal/services"
	"github.com/graphene-ci/graphene/pkg/proto/management/v1/managementv1connect"
)

// Factory carries the persistent flags and builds connections. One
// instance backs the whole command tree.
type Factory struct {
	CtxName string
	Config  string
	Ns      string
	Output  string
	JQ      string
}

// Bind registers the persistent flags on the root command.
func (f *Factory) Bind(root *cobra.Command) {
	pf := root.PersistentFlags()
	pf.StringVar(&f.CtxName, "context", "", "connection context name")
	pf.StringVar(&f.Config, "config", "", "config file (default: $GRAPHENE_CONFIG, else ~/.config/graphene/config.yaml)")
	pf.StringVarP(&f.Ns, "namespace", "n", "", "namespace for this call (cluster-wide admin tokens)")
	pf.StringVarP(&f.Output, "output", "o", "table", "output: table | wide | name | json | yaml")
	pf.StringVar(&f.JQ, "jq", "", "jq expression over the JSON form (implies -o json)")
}

// Resolve applies the config override and picks the context, then lays
// the per-call namespace on top.
func (f *Factory) Resolve() (cliconfig.Context, error) {
	if f.Config != "" {
		// The whole resolution chain reads the file through Path() —
		// the override IS the environment variable, for this process.
		if err := os.Setenv(cliconfig.EnvConfig, f.Config); err != nil {
			return cliconfig.Context{}, err
		}
	}
	cc, _, err := cliconfig.Resolve(f.CtxName)
	if err != nil {
		return cliconfig.Context{}, err
	}
	if f.Ns != "" {
		cc.Namespace = f.Ns
	}
	return cc, nil
}

// Door is one resolved connection: the connect clients over the single
// port, authenticated by the context's token.
type Door struct {
	CC        cliconfig.Context
	Runs      managementv1connect.RunsAPIClient
	Resources managementv1connect.ResourcesAPIClient
	Observe   managementv1connect.ObserveAPIClient
	Secrets   managementv1connect.SecretsAPIClient
	Revisions managementv1connect.RevisionsAPIClient
	Source    managementv1connect.SourceAPIClient
	Rbac      managementv1connect.RbacAPIClient
	Ns        managementv1connect.NamespacesAPIClient
}

// Dial resolves and connects.
func (f *Factory) Dial() (*Door, error) {
	cc, err := f.Resolve()
	if err != nil {
		return nil, err
	}
	return DialContext(cc)
}

// DialContext builds the clients over a resolved context. The connect
// plane of the door is its HTTP/1.1 half (cmux routes raw HTTP/2 to
// gRPC), which serves connect's server streams fine.
func DialContext(cc cliconfig.Context) (*Door, error) {
	scheme := "https"
	if cc.Insecure {
		scheme = "http"
	}
	client := &http.Client{}
	base := scheme + "://" + cc.Server
	auth := connect.WithInterceptors(authInterceptor{cc: cc})
	return &Door{
		CC:        cc,
		Runs:      managementv1connect.NewRunsAPIClient(client, base, auth),
		Resources: managementv1connect.NewResourcesAPIClient(client, base, auth),
		Observe:   managementv1connect.NewObserveAPIClient(client, base, auth),
		Secrets:   managementv1connect.NewSecretsAPIClient(client, base, auth),
		Revisions: managementv1connect.NewRevisionsAPIClient(client, base, auth),
		Source:    managementv1connect.NewSourceAPIClient(client, base, auth),
		Rbac:      managementv1connect.NewRbacAPIClient(client, base, auth),
		Ns:        managementv1connect.NewNamespacesAPIClient(client, base, auth),
	}, nil
}

// authInterceptor stamps the token (and the namespace pick, for
// cluster-wide admin tokens) onto every call, unary and streaming.
type authInterceptor struct{ cc cliconfig.Context }

func (a authInterceptor) apply(h http.Header) {
	h.Set("Authorization", "Bearer "+a.cc.Token)
	if a.cc.Namespace != "" {
		h.Set(services.NamespaceHeader, a.cc.Namespace)
	}
}

// Unary calls retry on Unavailable — a server redeploy mid-request
// otherwise surfaces as a one-off "unexpected EOF". Streams are not
// retried: the watch loops re-poll on their own.
func (a authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		a.apply(req.Header())
		var res connect.AnyResponse
		var err error
		for attempt, backoff := 0, 300*time.Millisecond; ; attempt, backoff = attempt+1, backoff*2 {
			res, err = next(ctx, req)
			if err == nil || attempt == 2 || connect.CodeOf(err) != connect.CodeUnavailable {
				return res, err
			}
			select {
			case <-ctx.Done():
				return res, err
			case <-time.After(backoff):
			}
		}
	}
}

func (a authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		a.apply(conn.RequestHeader())
		return conn
	}
}

func (a authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// completionDoor dials with the resolved context, silently — the
// completion path must never nag.
func (f *Factory) completionDoor() (*Door, context.Context, context.CancelFunc) {
	cc, err := f.Resolve()
	if err != nil {
		return nil, nil, nil
	}
	d, err := DialContext(cc)
	if err != nil {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	return d, ctx, cancel
}

// StdinIsTerminal reports whether a human is on the other end.
func StdinIsTerminal() bool { return term.IsTerminal(int(os.Stdin.Fd())) }

// StdoutIsTerminal decides between live panels and plain feeds.
func StdoutIsTerminal() bool { return term.IsTerminal(int(os.Stdout.Fd())) }
