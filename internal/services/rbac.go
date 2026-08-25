package services

// The RBAC doors: roles, bindings and service accounts. Handing out
// rights is itself a right (create on kind role / rolebinding /
// serviceaccount), so the same authorization guards this plane —
// nobody widens their own access without already being allowed to.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/gopherex/xlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/rbacflow"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// IssueToken mints a token for an account. The value is returned here
// and nowhere else; the record keeps its hash.
func (m *Management) IssueToken(ctx context.Context, creq *connect.Request[managementv1.IssueTokenRequest]) (*connect.Response[managementv1.IssueTokenResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindServiceAccount)
	if err != nil {
		return nil, err
	}
	if req.GetAccount() == "" {
		return nil, status.Error(codes.InvalidArgument, "an account is required")
	}
	value, err := randomToken()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	tokenId, err := randomId()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	expires := ""
	if ttl := req.GetTtlSeconds(); ttl > 0 {
		expires = time.Now().Add(time.Duration(ttl) * time.Second).UTC().Format(time.RFC3339)
	}
	if err := b.Worker.IssueAccountToken(ctx, req.GetAccount(), rbacflow.IssueTokenCmd{
		Id: tokenId, Hash: rbacflow.HashToken(value), Expires: expires, Comment: req.GetComment(),
	}); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "account %s: %v", req.GetAccount(), err)
	}
	m.audit(ctx, b, "serviceaccount/"+req.GetAccount(), authz.VerbCreate)
	m.Log.Info("token issued",
		xlog.String("namespace", b.Namespace),
		xlog.String("account", req.GetAccount()),
		xlog.String("token", tokenId))
	return connect.NewResponse(&managementv1.IssueTokenResponse{
		TokenId: tokenId, Token: value, Expires: expires,
	}), nil
}

// WhoAmI answers who the caller is and what they may do — what a UI
// needs to decide which buttons exist at all.
func (m *Management) WhoAmI(ctx context.Context, _ *connect.Request[managementv1.WhoAmIRequest]) (*connect.Response[managementv1.WhoAmIResponse], error) {
	namespace, err := callerNamespace(ctx)
	if err != nil {
		return nil, err
	}
	id, boundRole, err := identityOf(ctx, namespace)
	if err != nil {
		return nil, err
	}
	out := &managementv1.WhoAmIResponse{
		Subject:   id.Subject.String(),
		Groups:    id.Groups,
		Namespace: namespace,
	}
	if boundRole != "" {
		out.Roles = []string{boundRole}
	}
	// The allowed set is computed by asking the same question the doors
	// ask — no second implementation to drift.
	for _, verb := range authz.AllVerbs {
		for _, kind := range authz.AllKinds {
			if m.Authz == nil {
				continue
			}
			if d := m.Authz.Allow(ctx, id, boundRole, verb, kind); d.Allowed {
				out.Allowed = append(out.Allowed, fmt.Sprintf("%s %s", verb, kind))
			}
		}
	}
	return connect.NewResponse(out), nil
}

// forgetPermissions drops the cached decision set so a change of
// rights lands at once rather than after the cache expires.
func (m *Management) forgetPermissions(namespace string) {
	if m.Authz != nil {
		m.Authz.Forget(namespace)
	}
}

// randomToken mints 32 bytes of secret.
func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// randomId names a token so it can be revoked.
func randomId() (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// authenticate resolves a bearer token that is NOT one of the
// configured static ones: a minted token of a run or an agent, a
// service account's issued token, or an identity provider's id_token.
func (m *Management) authenticate(ctx context.Context, token, namespace string) (auth.Identity, bool) {
	if token == "" {
		return auth.Identity{}, false
	}
	if namespace == "" {
		namespace = "default"
	}
	// A minted token carries its own subject, namespace and role.
	if m.Minter != nil {
		if id, role, ok := m.Minter.Verify(token); ok {
			return auth.Identity{Identity: id, BoundRole: role}, true
		}
	}
	// A service account's token: found by hash, never by value.
	if b, err := m.Bundles.Get(namespace); err == nil {
		if account, ok := b.Worker.AccountForToken(ctx, rbacflow.HashToken(token)); ok {
			return auth.Identity{Identity: authz.Identity{
				Subject:   authz.Subject{Kind: authz.SubjectServiceAccount, Name: account},
				Namespace: namespace,
			}}, true
		}
	}
	// A person: the provider signed for them.
	if m.OIDC != nil {
		if id, err := m.OIDC.Verify(ctx, token); err == nil {
			id.Namespace = namespace
			return auth.Identity{Identity: id}, true
		}
	}
	return auth.Identity{}, false
}
