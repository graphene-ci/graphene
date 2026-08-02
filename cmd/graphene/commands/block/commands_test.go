package block_test

import (
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphene/commands/block"
)

func TestCommandsValidateFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		invalid func() error
		valid   func() error
	}{
		{
			name:    "init",
			invalid: func() error { return block.Init(nil) },
			valid: func() error {
				return block.Init(&block.InitFlags{
					Lang: "go",
					Path: "./someBlock",
				})
			},
		},
		{
			name:    "gen",
			invalid: func() error { return block.Gen(nil) },
			valid: func() error {
				return block.Gen(&block.GenFlags{
					Config: "./.graphene-block.yaml",
				})
			},
		},
		{
			name:    "build",
			invalid: func() error { return block.Build(nil) },
			valid: func() error {
				return block.Build(&block.BuildFlags{
					Config: "./.graphene-block.yaml",
				})
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if err := test.invalid(); err == nil {
				t.Fatal("command with invalid flags returned no error")
			}

			if err := test.valid(); err != nil {
				t.Fatalf("command with valid flags: %v", err)
			}
		})
	}
}
