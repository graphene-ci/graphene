package block

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
					Path: "./someBlock",
				})
			},
		},
		{
			name:    "gen",
			invalid: func() error { return Gen(nil) },
			valid: func() error {
				return Gen(&GenFlags{
					Config: "./.graphen-block.yaml",
				})
			},
		},
		{
			name:    "build",
			invalid: func() error { return Build(nil) },
			valid: func() error {
				return Build(&BuildFlags{
					Config: "./.graphen-block.yaml",
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
