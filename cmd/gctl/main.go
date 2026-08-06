// Command gctl talks to a graphene kernel.
package main

import (
	"os"

	"github.com/graphene-ci/graphene/cmd/gctl/commands"
)

func main() { os.Exit(commands.Execute()) }
