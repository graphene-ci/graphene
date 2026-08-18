// Command graphene is the user-facing CLI of an installation: scaffold a
// pipeline project (init); the full control surface — runs, resources,
// secrets — lands next.
package main

import (
	"fmt"
	"os"

	"github.com/graphene-ci/graphene/internal/cli"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = cli.Init(os.Args[2:], os.Stdout, os.Stderr)
	case "version":
		fmt.Println("graphene", version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "graphene: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "graphene:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: graphene <command>

commands:
  init <name>   scaffold a pipeline project (main.go, Dockerfile,
                Makefile, graphene.yaml)
  version       print the version
  help          this text`)
}
