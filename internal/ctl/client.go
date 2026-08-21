package ctl

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"

	"github.com/graphene-ci/pipeline/pkg/cliconfig"

	"github.com/graphene-ci/graphene/internal/services"
	"github.com/graphene-ci/graphene/pkg/proto/management/v1/managementv1connect"
)

// door is one resolved connection: the connect clients over the single
// port, authenticated by the context's token.
type door struct {
	cc        cliconfig.Context
	base      string
	http      *http.Client
	Runs      managementv1connect.RunsAPIClient
	Resources managementv1connect.ResourcesAPIClient
	Observe   managementv1connect.ObserveAPIClient
	Secrets   managementv1connect.SecretsAPIClient
	Ns        managementv1connect.NamespacesAPIClient
}

// commonFlags registers the flags every command shares and returns
// getters for them.
func commonFlags(fs *flag.FlagSet) (ctxName *string, output *string) {
	ctxName = fs.String("context", "", "connection context name")
	output = fs.String("o", "table", "output: table | json")
	return
}

// dial resolves the context and builds the clients. The connect plane
// of the door is its HTTP/1.1 half (cmux routes raw HTTP/2 to gRPC),
// which serves connect's server streams fine.
func dial(ctxName string) (*door, error) {
	cc, name, err := cliconfig.Resolve(ctxName)
	if err != nil {
		return nil, err
	}
	scheme := "https"
	if cc.Insecure {
		scheme = "http"
	}
	client := &http.Client{}
	base := scheme + "://" + cc.Server
	auth := connect.WithInterceptors(authInterceptor{cc: cc})
	d := &door{
		cc:        cc,
		base:      base,
		http:      client,
		Runs:      managementv1connect.NewRunsAPIClient(client, base, auth),
		Resources: managementv1connect.NewResourcesAPIClient(client, base, auth),
		Observe:   managementv1connect.NewObserveAPIClient(client, base, auth),
		Secrets:   managementv1connect.NewSecretsAPIClient(client, base, auth),
		Ns:        managementv1connect.NewNamespacesAPIClient(client, base, auth),
	}
	_ = name
	return d, nil
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

func (a authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		a.apply(req.Header())
		return next(ctx, req)
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

// parseMixed parses flags wherever they stand: stdlib flag stops at
// the first positional, so interleaved "secret set demo --value x"
// would silently drop the flag. Returns the positionals in order.
func parseMixed(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return pos, nil
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
}

// labelFlag collects repeatable -l k=v selectors.
type labelFlag struct{ m map[string]string }

func (l *labelFlag) String() string { return "" }

func (l *labelFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q: want k=v", s)
	}
	if l.m == nil {
		l.m = map[string]string{}
	}
	l.m[k] = v
	return nil
}
