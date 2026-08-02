package link_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	graphenepbv1 "github.com/graphene-ci/graphenepb/v1"

	corelink "github.com/graphene-ci/graphene/internal/core/link"
	"github.com/graphene-ci/graphene/internal/core/registry"
	"github.com/graphene-ci/graphene/internal/core/service"
	"github.com/graphene-ci/graphene/internal/infrastructure/auth/static"
	"github.com/graphene-ci/graphene/internal/infrastructure/link"
	"github.com/graphene-ci/graphene/internal/infrastructure/server"
	"github.com/graphene-ci/graphene/internal/infrastructure/store/bbolt"
)

const adminToken = "admin-token"

// newControl assembles the real control-kernel gRPC server (auth included).
func newControl(t *testing.T, creds credentials.TransportCredentials) *grpc.Server {
	t.Helper()

	st, err := bbolt.Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = st.Close() })

	source := static.New(static.Entry{Token: adminToken, Credentials: static.Admin("root")})

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(server.UnaryAuth(source)),
		grpc.StreamInterceptor(server.StreamAuth(source)),
	}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}

	srv := grpc.NewServer(opts...)
	graphenepbv1.RegisterResourceServiceServer(srv, service.NewResources(st, registry.New(st)))
	t.Cleanup(srv.Stop)

	return srv
}

// assertAlive proves the pipe carries real rpcs end to end.
func assertAlive(t *testing.T, l corelink.Link, tlsConfig *tls.Config) {
	t.Helper()

	conn, err := link.Connect("control", l, adminToken, tlsConfig)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	client := graphenepbv1.NewResourceServiceClient(conn)

	defs, err := client.ListDefinitions(ctx, &graphenepbv1.ListDefinitionsRequest{})
	if err != nil {
		t.Fatalf("rpc through link: %v", err)
	}

	if len(defs.GetDefinitions()) != 0 {
		t.Fatalf("fresh store has %d definitions", len(defs.GetDefinitions()))
	}
}

func TestStdioPipe(t *testing.T) {
	t.Parallel()

	// The two ends of an ssh session, without the ssh: the control side
	// serves the shared gRPC server over a single-conn listener, the
	// worker side dials through its half of the pipe.
	workerEnd, controlEnd := net.Pipe()
	srv := newControl(t, nil)

	go func() { _ = srv.Serve(link.SingleConnListener(controlEnd)) }()

	single := link.Single(workerEnd)
	assertAlive(t, single, nil)

	// The pipe is single-use by design: the same link cannot be re-dialed.
	if _, err := single.Dial(context.Background()); err == nil {
		t.Fatal("second dial of a single-use link succeeded")
	}
}

func TestRelayChain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Control on plain TCP.
	controlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := newControl(t, nil)

	go func() { _ = srv.Serve(controlLis) }()

	// relay1 → control, relay2 → relay1: the client knows only relay2.
	relay1 := listen(t)
	go func() { _ = link.ServeRelay(ctx, relay1, link.TCP(controlLis.Addr().String()), "t1") }()

	relay2 := listen(t)
	go func() { _ = link.ServeRelay(ctx, relay2, link.Via(relay1.Addr().String(), "t1"), "t2") }()

	assertAlive(t, link.Via(relay2.Addr().String(), "t2"), nil)
}

func TestRelayRejectsBadToken(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	relay := listen(t)
	go func() { _ = link.ServeRelay(ctx, relay, link.TCP("127.0.0.1:1"), "good") }()

	if _, err := link.Via(relay.Addr().String(), "bad").Dial(ctx); err == nil {
		t.Fatal("relay accepted a bad token")
	}
}

func TestTCPWithTLS(t *testing.T) {
	t.Parallel()

	cert, pool := selfSigned(t)

	controlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := newControl(t, credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}))

	go func() { _ = srv.Serve(controlLis) }()

	assertAlive(t, link.TCP(controlLis.Addr().String()), &tls.Config{
		RootCAs:    pool,
		ServerName: "graphene-control",
		MinVersion: tls.VersionTLS13,
	})
}

func listen(t *testing.T) net.Listener {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = lis.Close() })

	return lis
}

// selfSigned mints the control kernel's certificate the way first-start
// will: self-signed, pinned by the client through a CA pool.
func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "graphene-control"},
		DNSNames:     []string{"graphene-control"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}
