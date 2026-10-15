package services

import (
	"context"
	"fmt"

	"github.com/graphene-ci/graphene/internal/auth"
	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/httpapi"
)

// boundMethods are the native gRPC methods a binding may open, and what
// each asks of the caller's namespace. They are the worker plane's
// pipeline and run surface — what a pipeline binary's `push` and `run`
// do, and what a system integrating Graphene does in their place. The
// rest of the door — the Temporal proxy, the agent plane, secrets, blobs,
// capabilities, OTLP intake — stays with configured and minted tokens:
// a binding there would hand out the run's or the machine's own powers.
var boundMethods = map[string][]httpapi.Ask{
	"/graphene.workerplane.v1.ManifestAPI/PublishManifest": {{Verb: authz.VerbCreate, Kind: authz.KindRevision}, {Verb: authz.VerbUpdate, Kind: authz.KindPipeline}},
	"/graphene.workerplane.v1.ManifestAPI/GetPipeline":     {{Verb: authz.VerbGet, Kind: authz.KindPipeline}},
	"/graphene.workerplane.v1.RunsAPI/StartRun":            {{Verb: authz.VerbRun, Kind: authz.KindPipeline}},
	"/graphene.workerplane.v1.RunsAPI/GetRun":              {{Verb: authz.VerbGet, Kind: authz.KindRun}},
	"/graphene.workerplane.v1.RunsAPI/WatchRun":            {{Verb: authz.VerbGet, Kind: authz.KindRun}},
	"/graphene.workerplane.v1.RunsAPI/RunResult":           {{Verb: authz.VerbGet, Kind: authz.KindRun}},
}

// AuthorizeGRPC is the native gRPC door's question for a token the static
// authenticator does not know: the same identities and bindings as every
// other door, on the methods open to bindings. The principal it grants is
// RoleBound in that one namespace — the handlers of those methods admit
// it, nothing else does.
func (m *Management) AuthorizeGRPC(ctx context.Context, token, namespace, fullMethod string) (auth.Principal, bool, error) {
	if _, ok := m.authenticate(ctx, token, namespace); !ok {
		return auth.Principal{}, false, nil
	}
	asks, open := boundMethods[fullMethod]
	if !open {
		return auth.Principal{}, true, fmt.Errorf("%s is not open to a binding: it takes a run, agent or administrator token", fullMethod)
	}
	if _, err := m.AuthorizeHTTP(ctx, token, namespace, asks); err != nil {
		return auth.Principal{}, true, err
	}
	return auth.Principal{Role: auth.RoleBound, Namespace: namespace}, true, nil
}
