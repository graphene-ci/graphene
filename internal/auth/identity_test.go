package auth

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/graphene-ci/graphene/internal/authz"
)

// A minted token is signed, bounded in time, and speaks for exactly
// one subject: forging it, outliving it or reusing another
// installation's key must all fail.
func TestMintedToken(t *testing.T) {
	m := NewMinter("installation-key")
	sub := authz.Subject{Kind: authz.SubjectServiceAccount, Name: "run/x"}
	token, err := m.Mint(sub, "default", "run", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	id, role, ok := m.Verify(token)
	if !ok {
		t.Fatal("a freshly minted token must verify")
	}
	if id.Subject != sub || id.Namespace != "default" || role != "run" {
		t.Fatalf("token speaks for the wrong caller: %+v role=%s", id, role)
	}
	// Another installation's key must not verify it.
	if _, _, ok := NewMinter("other-key").Verify(token); ok {
		t.Fatal("a token must not verify under a foreign key")
	}
	// A tampered payload must not verify.
	if _, _, ok := m.Verify(token[:len(token)-2] + "xy"); ok {
		t.Fatal("a tampered token must not verify")
	}
	// An expired token must not verify.
	expired, err := m.Mint(sub, "default", "run", -time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := m.Verify(expired); ok {
		t.Fatal("an expired token must not verify")
	}
}

// The OIDC contour must refuse everything that is not a properly
// signed token of the configured provider.
func TestOIDCVerify(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	o := &OIDC{Issuer: "https://idp.example", Audience: "graphene"}
	o.keys = map[string]*rsa.PublicKey{"k1": &key.PublicKey}
	o.fetched = time.Now()

	sign := func(claims map[string]any) string {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"k1"}`))
		body, _ := json.Marshal(claims)
		payload := base64.RawURLEncoding.EncodeToString(body)
		sum := sha256.Sum256([]byte(header + "." + payload))
		sig, _ := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
		return header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(sig)
	}

	good := sign(map[string]any{
		"iss": "https://idp.example", "aud": "graphene", "sub": "alice",
		"groups": []any{"platform", "oncall"}, "exp": time.Now().Add(time.Hour).Unix(),
	})
	id, err := o.Verify(t.Context(), good)
	if err != nil {
		t.Fatalf("a valid token must verify: %v", err)
	}
	if id.Subject.Name != "alice" || len(id.Groups) != 2 {
		t.Fatalf("wrong caller: %+v", id)
	}

	for name, token := range map[string]string{
		"foreign issuer": sign(map[string]any{"iss": "https://evil.example", "aud": "graphene", "sub": "a", "exp": time.Now().Add(time.Hour).Unix()}),
		"wrong audience": sign(map[string]any{"iss": "https://idp.example", "aud": "other", "sub": "a", "exp": time.Now().Add(time.Hour).Unix()}),
		"expired":        sign(map[string]any{"iss": "https://idp.example", "aud": "graphene", "sub": "a", "exp": time.Now().Add(-time.Hour).Unix()}),
		"not a jwt":      "definitely-not-a-token",
	} {
		if _, err := o.Verify(t.Context(), token); err == nil {
			t.Fatalf("%s must be refused", name)
		}
	}

	// An unsigned "alg: none" token is the classic forgery.
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","kid":"k1"}`))
	body, _ := json.Marshal(map[string]any{"iss": "https://idp.example", "aud": "graphene", "sub": "root", "exp": time.Now().Add(time.Hour).Unix()})
	none := header + "." + base64.RawURLEncoding.EncodeToString(body) + "."
	if _, err := o.Verify(t.Context(), none); err == nil {
		t.Fatal("an unsigned token must be refused")
	}
}
