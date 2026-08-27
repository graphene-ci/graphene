// Package labels holds graphene's SYSTEM markers: the reserved
// "graphene.io/" keys the server writes on records, mirrored into
// visibility so any of them is a query.
//
// The ownership tree already answers "what belongs to what" — a
// revision's owner is its pipeline, an agent's owner is its run. These
// markers carry what the tree cannot: the CROSS references. A run's
// owner is its pipeline, but which revision it executes, which image
// it runs and what started it are not edges of any tree — and those
// are exactly the questions a person asks ("everything that came out
// of this revision", "every run of this image").
package labels

// Namespace of every system marker; user labels may never use it.
const Prefix = "graphene.io/"

const (
	// Pipeline is the project a record belongs to. Present even where
	// the owner already implies it: a listing shows labels, not
	// ancestry, and a person filtering by project should not have to
	// walk the tree.
	Pipeline = Prefix + "pipeline"

	// Run is the run that created a record — created-by, stable across
	// ownership transfers (a resource parked on a stand keeps it).
	Run = Prefix + "run"

	// Revision is the immutable build a run EXECUTES. The tree says a
	// run belongs to a pipeline; only this says which version of the
	// code actually ran.
	Revision = Prefix + "revision"

	// Source is the record a revision was built FROM
	// ("gitsource/main", "managedsource/fix"). A pipeline may have
	// several, and a revision comes from exactly one.
	Source = Prefix + "source"

	// SourceDigest pins the tree the revision was built from: two
	// revisions with the same digest are the same code.
	SourceDigest = Prefix + "source-digest"

	// Commit is the Git commit behind a revision, when its source was
	// a checkout — the answer to "what shipped from this repository".
	Commit = Prefix + "commit"

	// Image is the worker OCI reference a run executes.
	Image = Prefix + "image"

	// Trigger marks WHAT started a run: "manual" for the doors,
	// "<kind>:<name>" ("cron:nightly", "webhook:gh-push") for the
	// arbiter's automatic starts.
	Trigger = Prefix + "trigger"

	// Agent is the machine a resource lives on — set on records whose
	// existence is tied to one agent.
	Agent = Prefix + "agent"

	// Stand is the stand a resource was parked on, kept after the
	// transfer so "what is parked here" survives the move.
	Stand = Prefix + "stand"

	// Origin is where a managed source was copied from
	// ("gitsource/main"): provenance as a query, not just as spec.
	Origin = Prefix + "origin"

	// Declared marks HOW a record of a brought kind came to be. Absent
	// on records a pipeline's own code declares; "external" on records
	// declared through the generic apply door — the manifest does not
	// describe those, so they must be tellable apart.
	Declared = Prefix + "declared"
)

// DeclaredExternal is the Declared value of an apply-door record.
const DeclaredExternal = "external"

// TriggerManual is the doors' value for Trigger.
const TriggerManual = "manual"

// Merge folds system markers onto a record's labels without letting an
// empty value write an empty key: a marker that is not known is
// absent, never blank.
func Merge(into map[string]string, marks map[string]string) map[string]string {
	if into == nil {
		into = map[string]string{}
	}
	for k, v := range marks {
		if v == "" {
			continue
		}
		into[k] = v
	}
	return into
}
