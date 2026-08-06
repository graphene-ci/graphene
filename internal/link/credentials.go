package link

import (
	"crypto/tls"
	"errors"
	"fmt"

	"google.golang.org/grpc/credentials"
)

// minimumVersion is 1.3 and there is no setting for 1.2. Every side of
// every link here is this program, built at the same time as the other
// side or close to it, so the compatibility that older versions buy is
// compatibility with nobody.
const minimumVersion = tls.VersionTLS13

// ErrWrongKernel — the far side answered with a key nobody pinned.
//
// It is one error and not two, on purpose: a caller cannot tell "the
// address is wrong" from "somebody is in the middle", and pretending to
// would be a guess about which.
var ErrWrongKernel = errors.New("the kernel at that address is not the one pinned")

// Serving is what a kernel listens with.
//
// No client certificate is asked for. Who is calling is decided by the
// credential they present to the kernel, in one place, out of one store —
// and a second identity carried by the transport would be a second answer
// to the same question.
func (i Identity) Serving() credentials.TransportCredentials {
	return credentials.NewTLS(i.serving())
}

// Reaching is what a client dials one with.
//
// The standard verification is turned OFF and replaced, rather than
// added to, and the difference matters: this is not "trust anything", it
// is a DIFFERENT check that is stricter than the usual one. The usual
// check asks whether some authority vouched for a name; this one asks
// whether the key is the exact key that was named. A kernel's certificate
// is its own, signed by nobody, and carries no name — so the usual check
// has nothing to do and would refuse it.
func Reaching(pinned Pin) (credentials.TransportCredentials, error) {
	if pinned.IsZero() {
		return nil, ErrNoPin
	}

	return credentials.NewTLS(reaching(pinned)), nil
}

// reaching is the configuration itself, so the check can be tested
// against a real handshake rather than through a transport.
func reaching(pinned Pin) *tls.Config {
	return &tls.Config{
		MinVersion: minimumVersion,
		//nolint:gosec // not skipped: replaced below, by a stricter check
		InsecureSkipVerify: true,
		// VerifyConnection and NOT VerifyPeerCertificate. The second one
		// is the obvious place and is WRONG here: a resumed TLS 1.3
		// session does not run it, so a client that had once connected
		// would stop checking which kernel it was connected to. This one
		// runs on every handshake, resumed or not.
		VerifyConnection: verifyPin(pinned),
	}
}

// ErrNoPin — somebody was told to reach a kernel without being told which
// one.
//
// Refused rather than trusted-on-first-use. The first connection is
// exactly the one an attacker wants to be in the middle of, and a system
// that quietly remembers whoever answered first has spent its security on
// saving one line of configuration.
var ErrNoPin = errors.New("reaching a kernel needs its pin; ask it to print one")

// verifyPin is the whole check: the key the far side used, hashed, must
// be the key that was named.
func verifyPin(pinned Pin) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return ErrWrongKernel
		}

		found, err := PinOf(state.PeerCertificates[0])
		if err != nil {
			return fmt.Errorf("%w: %w", ErrWrongKernel, err)
		}

		if !found.Eq(pinned) {
			return fmt.Errorf("%w: expected %s, answered %s", ErrWrongKernel, pinned, found)
		}

		return nil
	}
}
