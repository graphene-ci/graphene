package link

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Rotation is how a kernel's key is replaced without a window in which
// nothing can reach it.
//
// A pin names a key, so replacing the key means everything pointing at
// this kernel has to be told a new one — and the order matters. Minting
// the key first and telling people afterwards leaves every client
// refusing the kernel until the last one is edited. Telling people first
// is impossible unless the pin can be KNOWN before it is served.
//
// So it is two steps, and clients can hold more than one pin:
//
//	graphened rekey prepare   makes the next key and prints its pin.
//	                          Nothing serves it yet.
//	<add that pin beside the old one, wherever this kernel is pointed at>
//	graphened rekey commit    starts serving it, at the next start.
//	<drop the old pin at leisure>
//
// At no point is there a moment where a correctly configured client
// cannot connect, and at no point is a key trusted that nobody was told
// about.
const (
	nextCertName = "next.crt"
	nextKeyName  = "next.key"
)

// Errors rotating a key.
var (
	// ErrNoNextKey — commit was asked for and nothing was prepared.
	ErrNoNextKey = errors.New("no key has been prepared; run `rekey prepare` first")
)

// Prepare mints the key this kernel will serve NEXT, and hands back its
// pin.
//
// Idempotent: a second call returns the pin of the key already prepared
// rather than making another. Somebody halfway through distributing a pin
// should not have it change under them because they ran the command
// twice.
func Prepare(dir string) (Pin, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return "", fmt.Errorf("link: %s: %w", dir, err)
	}

	certPath, keyPath := filepath.Join(dir, nextCertName), filepath.Join(dir, nextKeyName)

	if found, err := load(certPath, keyPath); err == nil {
		return found.Pin(), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	made, err := create(certPath, keyPath)
	if err != nil {
		return "", err
	}

	return made.Pin(), nil
}

// Pending is the pin of a prepared key, if there is one.
func Pending(dir string) (Pin, bool, error) {
	found, err := load(filepath.Join(dir, nextCertName), filepath.Join(dir, nextKeyName))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}

	if err != nil {
		return "", false, err
	}

	return found.Pin(), true, nil
}

// Commit makes the prepared key the one this kernel serves.
//
// The key is moved into place BEFORE the certificate, and the order is
// the one that survives being interrupted: a certificate that no longer
// matches its key is a kernel that will not start and says so, while the
// reverse is a kernel that starts and serves a key nobody was told about.
//
// It takes effect at the next start. Swapping the key under a running
// listener would drop every connection at a moment nobody chose, and a
// kernel is restarted by whoever is watching rather than by a command
// that was about certificates.
func Commit(dir string) (Pin, error) {
	pending, found, err := Pending(dir)
	if err != nil {
		return "", err
	}

	if !found {
		return "", ErrNoNextKey
	}

	if err := os.Rename(filepath.Join(dir, nextKeyName), filepath.Join(dir, keyName)); err != nil {
		return "", fmt.Errorf("link: install the key: %w", err)
	}

	if err := os.Rename(filepath.Join(dir, nextCertName), filepath.Join(dir, certName)); err != nil {
		return "", fmt.Errorf("link: install the certificate: %w", err)
	}

	return pending, nil
}
