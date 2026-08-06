// Command graphened is a graphene kernel.
package main

import (
	"os"

	"github.com/graphene-ci/graphene/cmd/graphened/commands"
)

func main() { os.Exit(commands.Execute()) }
