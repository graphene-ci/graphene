// Command graphene-server is the graphene control plane: the single door
// of an installation. It will host the server worker with the system
// resource flows (registered from the pipeline library), implement their
// Ops, serve the API, and run the managed execution path. The wiring
// lands next.
package main

import (
	"fmt"
	"os"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("graphene-server", version)
		return
	}
	fmt.Fprintln(os.Stderr, "graphene-server: not implemented yet")
	os.Exit(1)
}
