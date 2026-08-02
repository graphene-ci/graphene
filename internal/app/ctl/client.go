// Package ctl is the client side of the resource API: connecting to a
// kernel and rendering resources for humans.
//
// It is the same API the kernels themselves speak — ctl has no privileged
// path, it is simply another bearer of a token.
package ctl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
	"github.com/graphene-ci/graphene/internal/infrastructure/link"
)

// ErrBadCA — the pinned CA file carries no usable certificate.
var ErrBadCA = errors.New("ctl: ca file contains no usable certificate")

// Target says which kernel to talk to and how.
//
// A unix socket is the local case: the filesystem is the channel, so no
// TLS is layered inside — the token is still required, exactly as on TCP.
type Target struct {
	Address string
	Socket  string
	CAFile  string
	Token   string
}

// Client is a connected kernel API client.
type Client struct {
	conn      *grpc.ClientConn
	Resources graphenepbv1.ResourceServiceClient
	Blobs     graphenepbv1.BlobServiceClient
}

// Connect dials the target.
func Connect(target Target) (*Client, error) {
	transport, name, err := target.transport()
	if err != nil {
		return nil, err
	}

	tlsConfig, err := target.tlsConfig()
	if err != nil {
		return nil, err
	}

	conn, err := link.Connect(name, transport, target.Token, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("ctl: connect: %w", err)
	}

	return &Client{
		conn:      conn,
		Resources: graphenepbv1.NewResourceServiceClient(conn),
		Blobs:     graphenepbv1.NewBlobServiceClient(conn),
	}, nil
}

// Close releases the connection.
func (c *Client) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("ctl: close: %w", err)
	}

	return nil
}

func (t Target) transport() (corelink.Link, string, error) {
	if t.Socket != "" {
		return unixLink{path: t.Socket}, t.Socket, nil
	}

	if t.Address == "" {
		return nil, "", errNoTarget
	}

	return link.TCP(t.Address), t.Address, nil
}

func (t Target) tlsConfig() (*tls.Config, error) {
	if t.Socket != "" || t.CAFile == "" {
		return nil, nil //nolint:nilnil // no TLS is a valid, explicit outcome
	}

	pem, err := os.ReadFile(t.CAFile)
	if err != nil {
		return nil, fmt.Errorf("ctl: read ca file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%w: %s", ErrBadCA, t.CAFile)
	}

	return &tls.Config{
		RootCAs:    pool,
		ServerName: serverName(t.Address),
		MinVersion: tls.VersionTLS12,
	}, nil
}

// serverName is the host the certificate is verified against; an address
// without a host (":9000") verifies as localhost, which is what a locally
// minted certificate carries.
func serverName(addr string) string {
	host, _, found := strings.Cut(addr, ":")
	if !found || host == "" {
		return "localhost"
	}

	return host
}

var errNoTarget = errors.New("ctl: no kernel address or socket given")

// unixLink dials a unix socket.
type unixLink struct {
	path string
}

func (l unixLink) Dial(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "unix", l.path)
	if err != nil {
		return nil, fmt.Errorf("ctl: dial socket %s: %w", l.path, err)
	}

	return conn, nil
}
