package gateway_test

import (
	"strings"
	"testing"

	"github.com/graphene-ci/graphene/internal/infrastructure/gateway"
)

// A unix socket address has a hard length limit, and exceeding it makes
// bind return EINVAL — which prints as "invalid argument" and says
// nothing about length. A kernel installed under a long data directory
// would refuse to run anything, with an error nobody could act on.
func TestALongPathSaysWhatIsWrong(t *testing.T) {
	t.Parallel()

	deep := "/tmp/" + strings.Repeat("long-enough-to-matter/", 6)

	_, err := gateway.OverClient(deep, nil).Open("probe")
	if err == nil {
		t.Fatal("a path past the limit was accepted")
	}

	if !strings.Contains(err.Error(), "longer than the operating system allows") {
		t.Fatalf("the error does not say what is wrong: %v", err)
	}
}
