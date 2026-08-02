// Package tls mints and reuses the control kernel's certificate material.
//
// On first start (mode=auto) a self-signed CA and a server certificate are
// created under the configured directory; clients pin the CA — the k3s
// model, so a fresh kernel serves TLS with no operator ceremony. Operators
// who bring their own material use mode=files and never touch this.
package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// File names of the auto-minted material. The CA certificate is what
// clients pin (`graphene kernel ca` prints it).
// errCorruptCA — the stored CA material does not parse.
var errCorruptCA = errors.New("tls: corrupt ca material")

const (
	caCertFile     = "ca.crt"
	caKeyFile      = "ca.key"
	serverCertFile = "server.crt"
	serverKeyFile  = "server.key"

	tlsDirMode  = 0o700
	certMode    = 0o644
	keyMode     = 0o600
	caLifetime  = 10 * 365 * 24 * time.Hour
	srvLifetime = 2 * 365 * 24 * time.Hour
)

// Ensure loads the self-signed material from dir, minting the CA and the
// server certificate on first start. Subsequent starts reuse them, so the
// CA a client pinned stays valid.
// loopbackV4 is 127.0.0.1 as a certificate SAN entry (net.IP cannot be const).
//
//nolint:gochecknoglobals,mnd // see above; the octets are the loopback address
var loopbackV4 = net.IPv4(127, 0, 0, 1)

func Ensure(dir string, dnsNames []string) (tls.Certificate, error) {
	if err := os.MkdirAll(dir, tlsDirMode); err != nil {
		return tls.Certificate{}, fmt.Errorf("tls: tls dir: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, serverCertFile), filepath.Join(dir, serverKeyFile))
	if err == nil {
		return cert, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return tls.Certificate{}, fmt.Errorf("tls: load server cert: %w", err)
	}

	return mintAutoCert(dir, dnsNames)
}

func mintAutoCert(dir string, dnsNames []string) (tls.Certificate, error) {
	caCert, caKey, err := mintCA(dir)
	if err != nil {
		return tls.Certificate{}, err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls: server key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "graphene-control"},
		DNSNames:     dedupe(append([]string{"localhost", "graphene-control"}, dnsNames...)),
		// Loopback addresses are part of the certificate: a client that
		// reaches the kernel by IP (a local operator, a kernel on the same
		// host) must not be forced to override the server name.
		IPAddresses: []net.IP{loopbackV4, net.IPv6loopback},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(srvLifetime),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls: sign server cert: %w", err)
	}

	if err := writeCertKey(dir, serverCertFile, serverKeyFile, der, key); err != nil {
		return tls.Certificate{}, err
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("tls: parse server cert: %w", err)
	}

	return tls.Certificate{Certificate: [][]byte{der, caCert.Raw}, PrivateKey: key, Leaf: leaf}, nil
}

func mintCA(dir string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPath, keyPath := filepath.Join(dir, caCertFile), filepath.Join(dir, caKeyFile)

	if certPEM, err := os.ReadFile(certPath); err == nil {
		keyPEM, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("tls: read ca key: %w", err)
		}

		return parseCA(certPEM, keyPEM)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("tls: ca key: %w", err)
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "graphene-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(caLifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("tls: create ca: %w", err)
	}

	if err := writeCertKey(dir, caCertFile, caKeyFile, der, key); err != nil {
		return nil, nil, err
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("tls: parse ca: %w", err)
	}

	return cert, key, nil
}

func parseCA(certPEM, keyPEM []byte) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certBlock, _ := pem.Decode(certPEM)
	keyBlock, _ := pem.Decode(keyPEM)

	if certBlock == nil || keyBlock == nil {
		return nil, nil, errCorruptCA
	}

	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("tls: parse ca: %w", err)
	}

	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("tls: parse ca key: %w", err)
	}

	return cert, key, nil
}

func writeCertKey(dir, certName, keyName string, der []byte, key *ecdsa.PrivateKey) error {
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(filepath.Join(dir, certName), certPEM, certMode); err != nil {
		return fmt.Errorf("tls: write %s: %w", certName, err)
	}

	raw, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("tls: marshal key: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: raw})
	if err := os.WriteFile(filepath.Join(dir, keyName), keyPEM, keyMode); err != nil {
		return fmt.Errorf("tls: write %s: %w", keyName, err)
	}

	return nil
}

// ClientConfig builds the client side: trust exactly the pinned CA, or
// nothing at all when no CA is given (a channel that is trusted by
// construction — a unix socket, an ssh session — where the bearer token
// still carries the authentication).
func ClientConfig(caFile, serverName string) (*tls.Config, error) {
	if caFile == "" {
		return nil, nil //nolint:nilnil // no TLS is a valid, explicit outcome
	}

	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read ca file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("%w: %s", ErrBadCA, caFile)
	}

	if serverName == "" {
		serverName = defaultServerName
	}

	return &tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// ServerNameFor is the host a certificate is verified against: an address
// without a host verifies as the locally minted name.
func ServerNameFor(addr string) string {
	host, _, found := strings.Cut(addr, ":")
	if !found || host == "" {
		return defaultServerName
	}

	return host
}

// ErrBadCA — the pinned file carries no usable certificate.
var ErrBadCA = errors.New("tls: ca file contains no usable certificate")

const defaultServerName = "localhost"

// CACertPEM reads the pinnable CA certificate (`graphene kernel ca`).
func CACertPEM(dir string) ([]byte, error) {
	pemBytes, err := os.ReadFile(filepath.Join(dir, caCertFile))
	if err != nil {
		return nil, fmt.Errorf("tls: read ca certificate: %w", err)
	}

	return pemBytes, nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128) //nolint:mnd // 128-bit serial space

	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("tls: serial: %w", err)
	}

	return serial, nil
}

func dedupe(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))

	for _, v := range values {
		if v == "" {
			continue
		}

		if _, ok := seen[v]; ok {
			continue
		}

		seen[v] = struct{}{}

		out = append(out, v)
	}

	return out
}
