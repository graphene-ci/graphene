package ci_test

import (
	"testing"

	"github.com/graphene-ci/graphene/cmd/graphene/commands/ci"
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
			invalid: func() error { return ci.Init(nil) },
			valid: func() error {
				return ci.Init(&ci.InitFlags{
					Lang: "go",
					Path: "./.graphene-ci",
				})
			},
		},
		{
			name:    "build",
			invalid: func() error { return ci.Build(nil) },
			valid: func() error {
				return ci.Build(&ci.BuildFlags{
					Config: "./.graphene-ci/.graphene-ci.yaml",
				})
			},
		},
		{
			name:    "plan",
			invalid: func() error { return ci.Plan(nil) },
			valid: func() error {
				return ci.Plan(&ci.PlanFlags{
					Config: "./.graphene-ci/.graphene-ci.yaml",
				})
			},
		},
		{
			name:    "run",
			invalid: func() error { return ci.Run(nil) },
			valid: func() error {
				return ci.Run(&ci.RunFlags{
					Config: "./.graphene-ci/.graphene-ci.yaml",
					Watch:  true,
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
