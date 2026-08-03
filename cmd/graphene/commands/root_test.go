package commands_test

import (
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphene/commands"
)

func TestCommandTree(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path         []string
		flag         string
		defaultValue string
	}{
		{[]string{"kernel", "run"}, "config", ""},
		{[]string{"kernel", "install"}, "scope", "user"},
		{[]string{"ctl", "get"}, "output", "table"},
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
