package secret_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/graphene-ci/graphene/internal/app/secret"
)

func TestValueSources(t *testing.T) {
	// No t.Parallel: t.Setenv below is incompatible with parallel tests.
	file := filepath.Join(t.TempDir(), "v")
	if err := os.WriteFile(file, []byte(" from-file \n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("GRAPHEN_TEST_VALUE", "from-env")

	cases := map[string]struct {
		value secret.Value
		want  string
		err   error
	}{
		"inline": {value: secret.Value{Inline: "x"}, want: "x"},
		"file":   {value: secret.Value{File: file}, want: "from-file"},
		"env":    {value: secret.Value{Env: "GRAPHEN_TEST_VALUE"}, want: "from-env"},
		"unset":  {value: secret.Value{}, err: secret.ErrValueUnset},
		"ambiguous": {
			value: secret.Value{Inline: "x", Env: "GRAPHEN_TEST_VALUE"},
			err:   secret.ErrValueAmbiguous,
		},
		"missing env": {value: secret.Value{Env: "GRAPHEN_TEST_ABSENT"}, err: secret.ErrValueEmpty},
	}

	//nolint:paralleltest // the parent uses t.Setenv; subtests must stay sequential
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := tc.value.Resolve()
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("got err %v, want %v", err, tc.err)
				}

				return
			}

			if err != nil || got != tc.want {
				t.Fatalf("got %q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}
