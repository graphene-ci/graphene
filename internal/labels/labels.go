// Package labels holds graphene's system label keys that live on the
// SERVER side (pipeline/pkg/wire owns the ones user code touches).
package labels

// Trigger marks WHAT started a run: "manual" for the doors,
// "<kind>:<name>" ("cron:nightly", "webhook:gh-push") for the arbiter's
// automatic starts. Mirrored into visibility with the other labels.
const Trigger = "graphene.io/trigger"

// TriggerManual is the doors' value.
const TriggerManual = "manual"
