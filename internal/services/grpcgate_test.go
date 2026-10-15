package services

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/graphene-ci/graphene/internal/authz"
	"github.com/graphene-ci/graphene/internal/httpapi"
)

// Only the worker plane's pipeline and run surface is open to bindings;
// publishing asks for what the registry push asks for.
func TestBoundMethodsAreThePipelineAndRunSurface(t *testing.T) {
	require.Len(t, boundMethods, 6)
	require.Equal(t, []httpapi.Ask{{Verb: authz.VerbCreate, Kind: authz.KindRevision}, {Verb: authz.VerbUpdate, Kind: authz.KindPipeline}},
		boundMethods["/graphene.workerplane.v1.ManifestAPI/PublishManifest"])
	for _, closed := range []string{
		"/graphene.workerplane.v1.SecretsAPI/GetSecret",
		"/graphene.workerplane.v1.BlobsAPI/PutBlob",
		"/graphene.workerplane.v1.CapabilitiesAPI/PublishCapability",
		"/graphene.workerplane.v1.EventsAPI/Emit",
		"/graphene.agent.v1.AgentAPI/Session",
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
		"/opentelemetry.proto.collector.logs.v1.LogsService/Export",
	} {
		_, open := boundMethods[closed]
		require.False(t, open, closed)
	}
}
