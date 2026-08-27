package services

// Every door asks the same question before it acts: may THIS caller do
// THIS verb on THIS kind, here. The answer comes from the namespace's
// roles and bindings — records like everything else.
//
// Static tokens of the configuration keep working: each maps onto a
// built-in role, so an installation does not lose its agents the day
// authorization arrives.

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/nsbundle"
	"github.com/graphene-ci/graphene/internal/nsflow"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// identityOf builds the authorization identity of the caller: a minted
// token or an OIDC user brings its own, a configured static token is
// mapped onto the matching built-in role.
func identityOf(ctx context.Context, namespace string) (authz.Identity, string, error) {
	if id, ok := auth.IdentityFrom(ctx); ok {
		out := id.Identity
		out.Namespace = namespace
		return out, id.BoundRole, nil
	}
	p, ok := auth.FromContext(ctx)
	if !ok {
		return authz.Identity{}, "", status.Error(codes.Unauthenticated, "no principal")
	}
	// A configured token IS a service account, named after what it is.
	name := string(p.Role)
	if p.AgentId != "" {
		name = "agent/" + string(p.AgentId)
	}
	return authz.Identity{
		Subject:   authz.Subject{Kind: authz.SubjectServiceAccount, Name: name},
		Namespace: namespace,
	}, string(p.Role), nil
}

// allow resolves the caller's namespace, checks the right, and returns
// the bundle to act in.
func (m *Management) allow(ctx context.Context, verb authz.Verb, kind authz.Kind) (*nsbundle.Bundle, error) {
	namespace, err := callerNamespace(ctx)
	if err != nil {
		return nil, err
	}
	b, err := m.Bundles.Get(namespace)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("namespace %s: %v", namespace, err))
	}
	id, boundRole, err := identityOf(ctx, namespace)
	if err != nil {
		return nil, err
	}
	resolver := m.Authz
	if resolver == nil {
		// No resolver wired: fall back to the built-ins over the
		// caller's own role, so a partially configured installation
		// still enforces something rather than nothing.
		if rules, ok := authz.Builtins()[boundRole]; ok && rules.Allows(verb, kind) {
			return m.route(b, kind)
		}
		return nil, status.Errorf(codes.PermissionDenied, "%s may not %s %s", id.Subject, verb, kind)
	}
	if d := resolver.Allow(ctx, id, boundRole, verb, kind); !d.Allowed {
		return nil, status.Errorf(codes.PermissionDenied, "%s", d.Reason)
	}
	return m.route(b, kind)
}

// route sends a call to the namespace whose records answer it. A
// system kind describes the INSTALLATION, so its records live in one
// place — but the permission to touch it was decided a moment ago
// against the CALLER's namespace, which is what scopes a role.
func (m *Management) route(b *nsbundle.Bundle, kind authz.Kind) (*nsbundle.Bundle, error) {
	if !authz.IsSystem(kind) {
		return b, nil
	}
	sys, err := m.Bundles.Get(nsflow.SystemNamespace)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("system namespace: %v", err))
	}
	return sys, nil
}

// callerNamespace is the namespace the call acts in: the token's own,
// or the one an installation-wide admin picked with the header.
func callerNamespace(ctx context.Context) (string, error) {
	if p, ok := auth.FromContext(ctx); ok {
		return namespaceFor(ctx, p)
	}
	if id, ok := auth.IdentityFrom(ctx); ok {
		// An OIDC user, a minted token or a service account names its
		// own namespace — and may PICK another with the header: the
		// switch is free because every call still authorizes against
		// the target namespace's bindings.
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if hdr := md.Get(NamespaceHeader); len(hdr) > 0 && hdr[0] != "" {
				return hdr[0], nil
			}
		}
		if id.Namespace != "" {
			return id.Namespace, nil
		}
		return "default", nil
	}
	return "", status.Error(codes.Unauthenticated, "no principal")
}

// mintRunToken issues the token a starting run will carry: scoped to
// that one run, bound to the "run" role, and dead when the run is.
// An installation without a signing key falls back to its configured
// token, which the runner supplies itself.
func mintRunToken(b *nsbundle.Bundle, runId id.RunId) string {
	if b.MintRunToken == nil {
		return ""
	}
	return b.MintRunToken(b.Namespace, string(runId))
}

// auditEvent is one line of a record's audit: who changed it and how.
// It lands in the record's OWN history, next to what actually
// happened — not in a separate journal that can drift from it.
type auditEvent struct {
	// Audit marks the note so a reader can tell it from a domain
	// milestone.
	Audit string `json:"audit"`
	// Actor is the caller in subject form ("user:alice", "sa:ci").
	Actor string `json:"actor"`
	Verb  string `json:"verb"`
	At    string `json:"at"`
}

// audit records a MUTATION on one record. Reads are never audited:
// every note costs history budget, and a read that changed nothing is
// not worth a milestone.
func (m *Management) audit(ctx context.Context, b *nsbundle.Bundle, ref string, verb authz.Verb) {
	if ref == "" {
		return
	}
	id, _, err := identityOf(ctx, b.Namespace)
	if err != nil {
		return
	}
	b.Worker.Note(context.WithoutCancel(ctx), ref, auditEvent{
		Audit: "who",
		Actor: id.Subject.String(),
		Verb:  string(verb),
		At:    time.Now().UTC().Format(time.RFC3339),
	})
}
