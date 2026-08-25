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
	"sort"
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

// PutRole writes a role — rules are validated by the record itself, so
// a typo cannot become a permission.
func (m *Management) PutRole(ctx context.Context, creq *connect.Request[managementv1.PutRoleRequest]) (*connect.Response[managementv1.PutRoleResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindRole)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "a role needs a name")
	}
	rules := make(authz.Rules, 0, len(req.GetRules()))
	for _, r := range req.GetRules() {
		rule := authz.Rule{}
		for _, v := range r.GetVerbs() {
			rule.Verbs = append(rule.Verbs, authz.Verb(v))
		}
		for _, k := range r.GetKinds() {
			rule.Kinds = append(rule.Kinds, authz.Kind(k))
		}
		rules = append(rules, rule)
	}
	if err := rules.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := b.Worker.DeclareRole(ctx, req.GetName(), rbacflow.RoleSpec{Rules: rules, Description: req.GetDescription()}); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "role %s: %v", req.GetName(), err)
	}
	m.forgetPermissions(b.Namespace)
	m.audit(ctx, b, "role/"+req.GetName(), authz.VerbCreate)
	m.Log.Info("role written", xlog.String("namespace", b.Namespace), xlog.String("role", req.GetName()))
	return connect.NewResponse(&managementv1.PutRoleResponse{}), nil
}

// ListRoles lists the namespace's roles, built-ins included.
func (m *Management) ListRoles(ctx context.Context, _ *connect.Request[managementv1.ListRolesRequest]) (*connect.Response[managementv1.ListRolesResponse], error) {
	b, err := m.allow(ctx, authz.VerbList, authz.KindRole)
	if err != nil {
		return nil, err
	}
	roles, err := b.Worker.Roles(ctx, b.Namespace)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	builtins := authz.Builtins()
	names := make([]string, 0, len(roles))
	for name := range roles {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*managementv1.ListRolesResponse_Role, 0, len(names))
	for _, name := range names {
		role := &managementv1.ListRolesResponse_Role{Name: name}
		if _, ok := builtins[name]; ok {
			role.Builtin = true
		}
		for _, r := range roles[name] {
			rule := &managementv1.Rule{}
			for _, v := range r.Verbs {
				rule.Verbs = append(rule.Verbs, string(v))
			}
			for _, k := range r.Kinds {
				rule.Kinds = append(rule.Kinds, string(k))
			}
			role.Rules = append(role.Rules, rule)
		}
		out = append(out, role)
	}
	return connect.NewResponse(&managementv1.ListRolesResponse{Roles: out}), nil
}

// PutBinding grants a role to subjects.
func (m *Management) PutBinding(ctx context.Context, creq *connect.Request[managementv1.PutBindingRequest]) (*connect.Response[managementv1.PutBindingResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindRoleBinding)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" || req.GetRole() == "" {
		return nil, status.Error(codes.InvalidArgument, "a binding needs a name and a role")
	}
	subjects := make([]authz.Subject, 0, len(req.GetSubjects()))
	for _, raw := range req.GetSubjects() {
		sub, err := authz.ParseSubject(raw)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		subjects = append(subjects, sub)
	}
	namespace := req.GetNamespace()
	if namespace == "" {
		namespace = b.Namespace
	}
	spec := rbacflow.BindingSpec{Role: req.GetRole(), Subjects: subjects, Namespace: namespace}
	if err := spec.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := b.Worker.DeclareBinding(ctx, req.GetName(), spec); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "binding %s: %v", req.GetName(), err)
	}
	m.forgetPermissions(b.Namespace)
	m.audit(ctx, b, "rolebinding/"+req.GetName(), authz.VerbCreate)
	m.Log.Info("binding written",
		xlog.String("namespace", b.Namespace),
		xlog.String("binding", req.GetName()),
		xlog.String("role", req.GetRole()))
	return connect.NewResponse(&managementv1.PutBindingResponse{}), nil
}

// ListBindings lists the namespace's bindings.
func (m *Management) ListBindings(ctx context.Context, _ *connect.Request[managementv1.ListBindingsRequest]) (*connect.Response[managementv1.ListBindingsResponse], error) {
	b, err := m.allow(ctx, authz.VerbList, authz.KindRoleBinding)
	if err != nil {
		return nil, err
	}
	bindings, err := b.Worker.Bindings(ctx, b.Namespace)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*managementv1.ListBindingsResponse_Binding, 0, len(bindings))
	for _, bind := range bindings {
		subjects := make([]string, 0, len(bind.Subjects))
		for _, s := range bind.Subjects {
			subjects = append(subjects, s.String())
		}
		out = append(out, &managementv1.ListBindingsResponse_Binding{
			Role: bind.Role, Subjects: subjects, Namespace: bind.Namespace,
		})
	}
	return connect.NewResponse(&managementv1.ListBindingsResponse{Bindings: out}), nil
}

// CreateAccount creates a service account — a machine of this
// installation.
func (m *Management) CreateAccount(ctx context.Context, creq *connect.Request[managementv1.CreateAccountRequest]) (*connect.Response[managementv1.CreateAccountResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbCreate, authz.KindServiceAccount)
	if err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "an account needs a name")
	}
	if err := b.Worker.DeclareAccount(ctx, req.GetName(), rbacflow.AccountSpec{Description: req.GetDescription()}); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "account %s: %v", req.GetName(), err)
	}
	return connect.NewResponse(&managementv1.CreateAccountResponse{}), nil
}

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

// RevokeToken forgets one token; the account keeps working with the
// rest.
func (m *Management) RevokeToken(ctx context.Context, creq *connect.Request[managementv1.RevokeTokenRequest]) (*connect.Response[managementv1.RevokeTokenResponse], error) {
	req := creq.Msg
	b, err := m.allow(ctx, authz.VerbDelete, authz.KindServiceAccount)
	if err != nil {
		return nil, err
	}
	if err := b.Worker.RevokeAccountToken(ctx, req.GetAccount(), req.GetTokenId()); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "account %s: %v", req.GetAccount(), err)
	}
	m.audit(ctx, b, "serviceaccount/"+req.GetAccount(), authz.VerbDelete)
	return connect.NewResponse(&managementv1.RevokeTokenResponse{}), nil
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
