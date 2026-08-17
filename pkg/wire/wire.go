// Package wire holds conventions shared between graphene components (the
// server, the pipeline library, and the agent): queue names and search
// attribute keys. Pure functions over identifiers — no behavior.
package wire

import (
	"go.temporal.io/sdk/temporal"

	"github.com/graphene-ci/graphene/pkg/id"
)

// ServerQueue is the task queue of the graphene server worker: system
// entity workflows and their activities run here.
const ServerQueue = "graphene-server"

// MachineRunQueue names the task queue served by the user-code container
// hosted on a machine for one run. The container is scoped to
// (machine × run) and owned by the run: its version is the run's image
// version, and the run's end tears it down.
func MachineRunQueue(m id.MachineId, r id.RunId) string {
	return "machine/" + string(m) + "/run/" + string(r)
}

// RunQueue names the task queue of a run's main worker (managed container
// or inplace binary): the run workflow and machine-independent activities.
func RunQueue(r id.RunId) string {
	return "run/" + string(r)
}

// Search attribute keys used across the system in addition to the ones
// registered by temporal-entity (EntityKind, EntityPhase).
var (
	// SearchAttrOwner carries the OwnerRef of an entity for listing and
	// cascade queries ("find everything owned by run/X").
	SearchAttrOwner = temporal.NewSearchAttributeKeyKeyword("EntityOwner")
)
