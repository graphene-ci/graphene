// Command graphenectl is the generic control CLI of an installation:
// records with their five dimensions, runs, secrets, namespaces,
// connection contexts, and project scaffolding. A pipeline's own life
// (push, run) lives in the pipeline binary itself.
package main

import (
	"os"

	"github.com/graphene-ci/graphene/internal/ctl"
)

var version = "dev"

func main() {
	ctl.Version = version
	os.Exit(ctl.Main(os.Args[1:]))
}
