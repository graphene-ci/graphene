// Package link is how one kernel is reached by another, safely.
//
// EVERY TCP CONNECTION IN THIS SYSTEM IS ENCRYPTED, and there is no
// setting to turn it off. A kernel is reached with a bearer credential,
// and a bearer credential on a plaintext socket is not a credential —
// anybody on the path becomes whoever it names. So the two arrived
// together and stay together.
//
// A unix socket is not this. There the filesystem is the boundary and the
// door is the claim; encrypting a socket that only one machine can open
// would be ceremony. What crosses a network is what is protected here.
//
// The kernel is recognized by its KEY and not by a chain of signatures.
// See pin.go for why: a fleet is a list somebody wrote down, so "is this
// the kernel I was told about" is the question, and a hash is the whole
// answer to it.
package link

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
	"os"
	"path/filepath"
	"time"
)

const (
	dirMode  = 0o700
	certMode = 0o644
	// keyMode is the whole of how a kernel's key is protected: it is a
	// file only its owner can read, on a machine that kernel runs on. A
	// passphrase would mean somebody typing one at every start, which
	// means it would be in a file beside the key.
	keyMode = 0o600

	certName = "link.crt"
	keyName  = "link.key"

	// life is long because expiry solves nothing here. A pin is what
	// says which kernel this is, and it does not expire; the date exists
	// because certificates have one and because a TLS stack checks it.
	life = 20 * 365 * 24 * time.Hour
	// serialBits is the randomness in a serial number. It is not used to
	// identify anything — the pin does that — and is random because a
	// constant one makes some libraries unhappy.
	serialBits = 128
)

// Errors opening a kernel's key material.
var (
	// ErrNotAKey — the file is there and is not what it should be.
	ErrNotAKey  = errors.New("the key file does not hold a private key")
	ErrNotACert = errors.New("the certificate file does not hold a certificate")
)

// Identity is a kernel's key, its certificate, and the pin that names it.
//
// One per kernel, made at the first start and kept afterwards. It is not
// a secret a person handles: the pin is what gets copied around, and the
// key never leaves the machine that made it.
type Identity struct {
	certificate tls.Certificate
	pin         Pin
}

// Open loads a kernel's key material, making it the first time.
//
// Made rather than asked for, because a kernel that would not start until
// somebody supplied a certificate would be a kernel nobody installs — and
// the fallback everybody would reach for is the plaintext switch this
// package exists to not have.
func Open(dir string) (Identity, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return Identity{}, fmt.Errorf("link: %s: %w", dir, err)
	}

	certPath, keyPath := filepath.Join(dir, certName), filepath.Join(dir, keyName)

	found, err := load(certPath, keyPath)
	if err == nil {
		return found, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return Identity{}, err
	}

	return create(certPath, keyPath)
}

// load reads key material that is already there.
func load(certPath, keyPath string) (Identity, error) {
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		// Not there is passed on as it is, because the caller tells that
		// one apart: it means this kernel has never started, and the
		// answer to it is to make key material rather than to fail.
		if errors.Is(err, os.ErrNotExist) {
			return Identity{}, fmt.Errorf("%w", err)
		}

		return Identity{}, fmt.Errorf("link: %s: %w", certPath, err)
	}

	return identityOf(certificate)
}

// create mints a kernel's key material and writes it down.
//
// The certificate carries no name — no host, no address, no IP. Nothing
// checks one: a pin says which kernel this is, and a name in a
// certificate would be a second answer to that question, wrong the day
// the kernel moves.
func create(certPath, keyPath string) (Identity, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Identity{}, fmt.Errorf("link: make a key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), serialBits))
	if err != nil {
		return Identity{}, fmt.Errorf("link: serial: %w", err)
	}

	now := time.Now()

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "graphene kernel"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(life),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return Identity{}, fmt.Errorf("link: make a certificate: %w", err)
	}

	if err := writeCert(certPath, der); err != nil {
		return Identity{}, err
	}

	if err := writeKey(keyPath, key); err != nil {
		return Identity{}, err
	}

	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return Identity{}, fmt.Errorf("link: %s: %w", certPath, err)
	}

	return identityOf(certificate)
}

func writeCert(path string, der []byte) error {
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	if err := os.WriteFile(path, encoded, certMode); err != nil {
		return fmt.Errorf("link: write %s: %w", path, err)
	}

	return nil
}

func writeKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("link: encode the key: %w", err)
	}

	encoded := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})

	if err := os.WriteFile(path, encoded, keyMode); err != nil {
		return fmt.Errorf("link: write %s: %w", path, err)
	}

	return nil
}

// identityOf works out the pin of key material just loaded.
func identityOf(certificate tls.Certificate) (Identity, error) {
	if len(certificate.Certificate) == 0 {
		return Identity{}, ErrNotACert
	}

	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return Identity{}, fmt.Errorf("link: %w: %w", ErrNotACert, err)
	}

	pin, err := PinOf(leaf)
	if err != nil {
		return Identity{}, err
	}

	certificate.Leaf = leaf

	return Identity{certificate: certificate, pin: pin}, nil
}

// serving is the configuration itself, for the same reason reaching is:
// a handshake is the only honest test of a check that runs during one.
func (i Identity) serving() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{i.certificate},
		MinVersion:   minimumVersion,
		ClientAuth:   tls.NoClientCert,
	}
}

// Pin is what to tell whoever points at this kernel.
func (i Identity) Pin() Pin { return i.pin }

// IsZero reports key material that was never opened.
func (i Identity) IsZero() bool { return i.pin.IsZero() }

// PinIn reads the pin of the kernel whose key material is kept in a
// directory, WITHOUT making any.
//
// It is how a client on the same machine learns what to expect: the
// certificate is right there and public, so making somebody copy a
// fingerprint they could read would be ceremony. A directory with nothing
// in it is a kernel that has not started, which is the caller's answer to
// give and not this function's.
func PinIn(dir string) (Pin, error) {
	encoded, err := os.ReadFile(filepath.Join(dir, certName))
	if err != nil {
		return "", fmt.Errorf("link: %w", err)
	}

	block, _ := pem.Decode(encoded)
	if block == nil {
		return "", ErrNotACert
	}

	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("link: %w: %w", ErrNotACert, err)
	}

	return PinOf(leaf)
}
