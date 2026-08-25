package auth

// People come from an identity provider: Graphene verifies the
// provider's id_token and reads the caller out of its claims. No
// passwords, no sessions, no account store for humans — an
// installation that has an OIDC provider has users, one that does not
// works with service accounts.

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/graphene-ci/graphene/internal/authz"
)

// OIDC verifies id_tokens of one provider.
type OIDC struct {
	// Issuer is the provider's issuer URL; a token from anywhere else
	// is refused.
	Issuer string
	// Audience is this installation's client id.
	Audience string
	// GroupsClaim names the claim carrying group membership
	// ("groups" by default) — it is what a binding grants rights to.
	GroupsClaim string
	// UsernameClaim names the claim used as the subject ("sub" by
	// default; "email" and "preferred_username" are common).
	UsernameClaim string

	Client *http.Client

	mu       sync.RWMutex
	keys     map[string]*rsa.PublicKey
	fetched  time.Time
	jwksURI  string
	discover time.Time
}

// keyTTL bounds how long a fetched key set is trusted before it is
// re-read — key rotation must not need a restart.
const keyTTL = time.Hour

// Verify checks one id_token and returns the caller it names.
func (o *OIDC) Verify(ctx context.Context, token string) (authz.Identity, error) {
	if o == nil || o.Issuer == "" {
		return authz.Identity{}, fmt.Errorf("no identity provider is configured")
	}
	header, payload, signed, sig, err := splitJWT(token)
	if err != nil {
		return authz.Identity{}, err
	}
	key, err := o.key(ctx, header.Kid)
	if err != nil {
		return authz.Identity{}, err
	}
	if err := verifyRS256(key, signed, sig); err != nil {
		return authz.Identity{}, err
	}
	if payload.Iss != o.Issuer {
		return authz.Identity{}, fmt.Errorf("token issuer %q is not this installation's provider", payload.Iss)
	}
	if o.Audience != "" && !payload.hasAudience(o.Audience) {
		return authz.Identity{}, fmt.Errorf("token is not for this installation")
	}
	if payload.Exp > 0 && time.Now().Unix() > payload.Exp {
		return authz.Identity{}, fmt.Errorf("token has expired")
	}
	name := payload.claim(o.usernameClaim())
	if name == "" {
		return authz.Identity{}, fmt.Errorf("token carries no %s claim", o.usernameClaim())
	}
	return authz.Identity{
		Subject: authz.Subject{Kind: authz.SubjectUser, Name: name},
		Groups:  payload.groups(o.groupsClaim()),
	}, nil
}

func (o *OIDC) usernameClaim() string {
	if o.UsernameClaim != "" {
		return o.UsernameClaim
	}
	return "sub"
}

func (o *OIDC) groupsClaim() string {
	if o.GroupsClaim != "" {
		return o.GroupsClaim
	}
	return "groups"
}

// key returns the provider's signing key, discovering and caching the
// key set as needed.
func (o *OIDC) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	o.mu.RLock()
	key, ok := o.keys[kid]
	fresh := time.Since(o.fetched) < keyTTL
	o.mu.RUnlock()
	if ok && fresh {
		return key, nil
	}
	if err := o.refresh(ctx); err != nil {
		return nil, err
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if key, ok := o.keys[kid]; ok {
		return key, nil
	}
	return nil, fmt.Errorf("the provider has no signing key %q", kid)
}

func (o *OIDC) refresh(ctx context.Context) error {
	client := o.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	uri := o.jwksURI
	if uri == "" || time.Since(o.discover) > keyTTL {
		var doc struct {
			JwksURI string `json:"jwks_uri"`
		}
		if err := getJSON(ctx, client, strings.TrimSuffix(o.Issuer, "/")+"/.well-known/openid-configuration", &doc); err != nil {
			return fmt.Errorf("provider discovery: %w", err)
		}
		if doc.JwksURI == "" {
			return fmt.Errorf("provider discovery: no jwks_uri")
		}
		uri = doc.JwksURI
	}
	var set struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := getJSON(ctx, client, uri, &set); err != nil {
		return fmt.Errorf("provider keys: %w", err)
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" {
			continue
		}
		n, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			continue
		}
		e, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			continue
		}
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	}
	if len(keys) == 0 {
		return fmt.Errorf("provider keys: none usable")
	}
	o.mu.Lock()
	o.keys, o.fetched, o.jwksURI, o.discover = keys, time.Now(), uri, time.Now()
	o.mu.Unlock()
	return nil
}

func getJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
