// Package services implements the server's API planes over the
// namespace bundles: the management plane (CLI/UI/cloud) and the worker
// plane (running user code). The namespace always comes from the
// caller's token; a cluster-wide admin ("*") picks one with the
// x-graphene-namespace metadata header.
package services

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/nsbundle"
)

// NamespaceHeader picks the namespace for cluster-wide admin tokens.
const NamespaceHeader = "x-graphene-namespace"

// scope resolves the caller's namespace and checks the role.
func scope(ctx context.Context, roles ...auth.Role) (string, error) {
	p, ok := auth.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "no principal")
	}
	allowed := false
	for _, r := range roles {
		if p.Role == r {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", status.Errorf(codes.PermissionDenied, "role %s may not call this", p.Role)
	}
	namespace := p.Namespace
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if hdr := md.Get(NamespaceHeader); len(hdr) > 0 && hdr[0] != "" {
			if !p.In(hdr[0]) {
				return "", status.Errorf(codes.PermissionDenied, "token has no access to namespace %q", hdr[0])
			}
			namespace = hdr[0]
		}
	}
	if namespace == "*" {
		namespace = "default"
	}
	if namespace == "" {
		return "", status.Error(codes.InvalidArgument, "no namespace")
	}
	return namespace, nil
}

// bundleFor resolves the caller's bundle.
func bundleFor(ctx context.Context, bundles *nsbundle.Manager, roles ...auth.Role) (*nsbundle.Bundle, error) {
	namespace, err := scope(ctx, roles...)
	if err != nil {
		return nil, err
	}
	b, err := bundles.Get(namespace)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("namespace %s: %v", namespace, err))
	}
	return b, nil
}
