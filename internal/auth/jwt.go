package auth

// The little JWT reader the OIDC contour needs: RS256 only, because
// that is what every provider issues and every extra algorithm is one
// more way to be wrong.

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

type jwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type jwtPayload struct {
	Iss string          `json:"iss"`
	Aud json.RawMessage `json:"aud"`
	Exp int64           `json:"exp"`
	raw map[string]any
}

// splitJWT parses the three parts and returns what verification needs.
func splitJWT(token string) (jwtHeader, jwtPayload, string, []byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("not a JWT")
	}
	headerRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("JWT header: %w", err)
	}
	var header jwtHeader
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("JWT header: %w", err)
	}
	if header.Alg != "RS256" {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("JWT algorithm %q is not accepted; use RS256", header.Alg)
	}
	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("JWT payload: %w", err)
	}
	var payload jwtPayload
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("JWT payload: %w", err)
	}
	_ = json.Unmarshal(payloadRaw, &payload.raw)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return jwtHeader{}, jwtPayload{}, "", nil, fmt.Errorf("JWT signature: %w", err)
	}
	return header, payload, parts[0] + "." + parts[1], sig, nil
}

func verifyRS256(key *rsa.PublicKey, signed string, sig []byte) error {
	sum := sha256.Sum256([]byte(signed))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return fmt.Errorf("token signature does not verify")
	}
	return nil
}

// claim reads one string claim.
func (p jwtPayload) claim(name string) string {
	v, _ := p.raw[name].(string)
	return v
}

// groups reads the group claim, which providers render as a list or a
// single string.
func (p jwtPayload) groups(name string) []string {
	switch v := p.raw[name].(type) {
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	default:
		return nil
	}
}

// hasAudience reports whether the token names this audience; the claim
// is a string or a list, depending on the provider.
func (p jwtPayload) hasAudience(want string) bool {
	var one string
	if json.Unmarshal(p.Aud, &one) == nil {
		return one == want
	}
	var many []string
	if json.Unmarshal(p.Aud, &many) == nil {
		for _, a := range many {
			if a == want {
				return true
			}
		}
	}
	return false
}
