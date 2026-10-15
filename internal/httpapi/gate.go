package httpapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/graphene-ci/graphene/internal/authz"
)

// Gate decides for the tokens the static authenticator does not know —
// a service account's issued token, an identity provider's id_token —
// with the namespace's roles and bindings, the way every API door does.
type Gate interface {
	// AuthorizeHTTP answers whether the token names anyone at all
	// (authenticated) and, if it does, whether that caller may do ONE OF
	// the asks in namespace — nil when allowed. No asks: being someone
	// is enough.
	AuthorizeHTTP(ctx context.Context, token, namespace string, asks []Ask) (authenticated bool, err error)
}

// Ask is one verb on one kind; a route accepts any of its asks.
type Ask struct {
	Verb authz.Verb
	Kind authz.Kind
}

// rule names what a request asks for: the namespace it acts in and the
// asks that would allow it; ok=false refuses it outright.
type rule func(r *http.Request) (namespace string, asks []Ask, ok bool)

// NamespaceHeader picks the namespace of a call that does not name one in
// its path — the same header the API doors read.
const NamespaceHeader = "X-Graphene-Namespace"

// registryRule maps a registry call onto the records it touches. A
// repository is "<namespace>/<pipeline>", so the namespace comes from the
// PATH — a token cannot reach another namespace's images by naming a
// header. Reading an image is reading the pipeline; writing one is
// creating a revision or updating the pipeline. The bare /v2/ is the
// registry's ping: docker login asks it, and being someone answers it.
// The catalog lists every namespace's repositories: not for a binding.
func registryRule(r *http.Request) (string, []Ask, bool) {
	rest := strings.TrimPrefix(r.URL.Path, "/v2/")
	if rest == "" {
		return "", nil, true
	}
	namespace, repo, found := strings.Cut(rest, "/")
	if !found || namespace == "" || repo == "" || strings.HasPrefix(namespace, "_") {
		return "", nil, false
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return namespace, []Ask{{authz.VerbGet, authz.KindPipeline}}, true
	default:
		return namespace, []Ask{{authz.VerbCreate, authz.KindRevision}, {authz.VerbUpdate, authz.KindPipeline}}, true
	}
}

// agentBinaryRule: the agent binary is read by whoever may read agents in
// the namespace the call names (default when it names none).
func agentBinaryRule(r *http.Request) (string, []Ask, bool) {
	namespace := r.Header.Get(NamespaceHeader)
	if namespace == "" {
		namespace = "default"
	}
	return namespace, []Ask{{authz.VerbGet, authz.KindAgent}}, true
}
