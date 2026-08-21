package secrets

// The value store's file backend: every namespace's secrets in ONE
// AES-GCM-sealed file on the server's volume — the dev installation's
// persistence. Postgres replaces it for production behind the same
// surface; external managers (Vault, OpenBao, KMS) arrive as adapters.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// filePersister seals the whole secret map into one file.
type filePersister struct {
	path string
	aead cipher.AEAD
}

// newFilePersister builds the sealer; keyHex is the installation's
// 32-byte master key, hex-encoded.
func newFilePersister(path, keyHex string) (*filePersister, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("secrets key: want 64 hex chars (32 bytes)")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &filePersister{path: path, aead: aead}, nil
}

// load reads and opens the sealed map; a missing file is an empty map.
func (f *filePersister) load() (map[string]map[string]string, error) {
	raw, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return map[string]map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	ns := f.aead.NonceSize()
	if len(raw) < ns {
		return nil, fmt.Errorf("secrets file %s is truncated", f.path)
	}
	plain, err := f.aead.Open(nil, raw[:ns], raw[ns:], nil)
	if err != nil {
		return nil, fmt.Errorf("secrets file %s: %w (wrong key?)", f.path, err)
	}
	out := map[string]map[string]string{}
	if err := json.Unmarshal(plain, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// save seals and atomically replaces the file.
func (f *filePersister) save(m map[string]map[string]string) error {
	plain, err := json.Marshal(m)
	if err != nil {
		return err
	}
	nonce := make([]byte, f.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	sealed := append(nonce, f.aead.Seal(nil, nonce, plain, nil)...) //nolint:makezero // the nonce prefix is the format
	if err := os.MkdirAll(filepath.Dir(f.path), 0o700); err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, sealed, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}
