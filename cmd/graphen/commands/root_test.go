package commands_test

import (
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphen/commands"
)

func TestCommandTree(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path         []string
		flag         string
		defaultValue string
	}{
		{[]string{"kernel", "run"}, "config", ""},
		{[]string{"ci", "init"}, "lang", "go"},
		{[]string{"ci", "init"}, "path", ""},
		{[]string{"ci", "build"}, "config", "./.graphen-ci/.graphen-ci.yaml"},
		{[]string{"ci", "plan"}, "config", "./.graphen-ci/.graphen-ci.yaml"},
		{[]string{"ci", "run"}, "config", "./.graphen-ci/.graphen-ci.yaml"},
		{[]string{"ci", "run"}, "watch", "false"},
		{[]string{"block", "init"}, "lang", "go"},
		{[]string{"block", "init"}, "path", ""},
		{[]string{"block", "gen"}, "config", ""},
		{[]string{"block", "build"}, "config", ""},
	}

	for _, test := range tests {
		command, _, err := commands.Root().Find(test.path)
		if err != nil {
			t.Fatalf("find %v: %v", test.path, err)
		}

		flag := command.Flags().Lookup(test.flag)
		if flag == nil {
			t.Fatalf("%v: flag %q not found", test.path, test.flag)
		}

		if flag.DefValue != test.defaultValue {
			t.Fatalf(
				"%v: flag %q default = %q, want %q",
				test.path,
				test.flag,
				flag.DefValue,
				test.defaultValue,
			)
		}
	}
}
