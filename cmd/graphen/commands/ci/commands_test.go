package ci

import "testing"

func TestCommandsValidateFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		invalid func() error
		valid   func() error
	}{
		{
			name:    "init",
			invalid: func() error { return Init(nil) },
			valid: func() error {
				return Init(&InitFlags{
					Lang: "go",
					Path: "./.graphen-ci",
				})
			},
		},
		{
			name:    "build",
			invalid: func() error { return Build(nil) },
			valid: func() error {
				return Build(&BuildFlags{
					Config: "./.graphen-ci/.graphen-ci.yaml",
				})
			},
		},
		{
			name:    "plan",
			invalid: func() error { return Plan(nil) },
			valid: func() error {
				return Plan(&PlanFlags{
					Config: "./.graphen-ci/.graphen-ci.yaml",
				})
			},
		},
		{
			name:    "run",
			invalid: func() error { return Run(nil) },
			valid: func() error {
				return Run(&RunFlags{
					Config: "./.graphen-ci/.graphen-ci.yaml",
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
