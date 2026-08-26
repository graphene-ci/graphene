package auth

// The three contours of identity: a person from the OIDC provider, a
// service account of this installation, and a minted token of one run
// or agent. Whatever the contour, the result is the same Identity the
// authorization layer decides on.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/graphene-ci/graphene/internal/authz"
)

// Minter issues and verifies the SHORT-LIVED tokens of runs and
// agents. They are signed, not stored: verification needs no lookup,
// and a token dies with its deadline (a run's token also dies with the
// run, which the door checks separately).
type Minter struct {
	key []byte
}

// NewMinter builds the minter over the installation's signing key.
func NewMinter(key string) *Minter {
	return &Minter{key: []byte(key)}
}

// minted is the payload of a signed token.
type minted struct {
	// Sub is the subject in wire form ("sa:run/x", "sa:agent/edge-1").
	Sub string `json:"sub"`
	// Ns is the namespace the token acts in.
	Ns string `json:"ns"`
	// Role names the role bound to this token.
	Role string `json:"role"`
	// Exp is the unix deadline.
	Exp int64 `json:"exp"`
}

// Mint issues a token for one subject, bound to one role, for a
// bounded time.
func (m *Minter) Mint(subject authz.Subject, namespace, role string, ttl time.Duration) (string, error) {
	if len(m.key) == 0 {
		return "", fmt.Errorf("no signing key: set the installation's auth key")
	}
	payload, err := json.Marshal(minted{
		Sub:  subject.String(),
		Ns:   namespace,
		Role: role,
		Exp:  time.Now().Add(ttl).Unix(),
	})
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(payload)
	return "gm1." + body + "." + m.sign(body), nil
}

// Verify checks a minted token and returns who it speaks for.
// ExpiryOf reports when a MINTED token expires; zero for anything
// else (a static token has no expiry to rotate ahead of).
func (m *Minter) ExpiryOf(token string) time.Time {
	rest, ok := strings.CutPrefix(token, "gm1.")
	if !ok || len(m.key) == 0 {
		return time.Time{}
	}
	body, sig, ok := strings.Cut(rest, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(m.sign(body))) {
		return time.Time{}
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return time.Time{}
	}
	var p minted
	if json.Unmarshal(raw, &p) != nil {
		return time.Time{}
	}
	return time.Unix(p.Exp, 0)
}

func (m *Minter) Verify(token string) (authz.Identity, string, bool) {
	rest, ok := strings.CutPrefix(token, "gm1.")
	if !ok || len(m.key) == 0 {
		return authz.Identity{}, "", false
	}
	body, sig, ok := strings.Cut(rest, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(m.sign(body))) {
		return authz.Identity{}, "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return authz.Identity{}, "", false
	}
	var p minted
	if err := json.Unmarshal(raw, &p); err != nil {
		return authz.Identity{}, "", false
	}
	if time.Now().Unix() > p.Exp {
		return authz.Identity{}, "", false
	}
	sub, err := authz.ParseSubject(p.Sub)
	if err != nil {
		return authz.Identity{}, "", false
	}
	return authz.Identity{Subject: sub, Namespace: p.Ns}, p.Role, true
}

func (m *Minter) sign(body string) string {
	mac := hmac.New(sha256.New, m.key)
	mac.Write([]byte(body))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Identity is the authenticated caller in authorization terms, plus
// the role a minted token carries with it (empty for people and
// service accounts — their roles come from bindings).
type Identity struct {
	authz.Identity
	// BoundRole is the role a minted token carries; bindings are not
	// consulted for it.
	BoundRole string
}

type identityKey struct{}

// IdentityFrom returns the identity an interceptor attached.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	i, ok := ctx.Value(identityKey{}).(Identity)
	return i, ok
}

// WithIdentity attaches an identity (interceptors and tests).
func WithIdentity(ctx context.Context, i Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, i)
}
