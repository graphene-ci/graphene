// Package link implements the core link port: TLS dial-out, stdio pipes
// for ssh-spawned kernels, and relay chains — plus the assembly of a gRPC
// client connection over any of them.
package link

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
)

// Connect builds a gRPC client connection riding the given link.
//
// tlsConfig nil means the CHANNEL is trusted by construction (stdio inside
// an ssh session, tests): the bearer token then relies on the channel for
// confidentiality, exactly like it does on TLS. TCP links must pass a
// config with RootCAs and ServerName of the control kernel.
func Connect(target string, l corelink.Link, token string, tlsConfig *tls.Config) (*grpc.ClientConn, error) {
	creds := insecure.NewCredentials()
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}

	conn, err := grpc.NewClient("passthrough:///"+target,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return l.Dial(ctx)
		}),
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(bearer(token)),
	)
	if err != nil {
		return nil, fmt.Errorf("link: connect: %w", err)
	}

	return conn, nil
}

// bearer sends the token with every rpc.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

// RequireTransportSecurity is false because trusted non-TLS channels
// (stdio over ssh) are legitimate carriers; see Connect.
func (bearer) RequireTransportSecurity() bool { return false }
