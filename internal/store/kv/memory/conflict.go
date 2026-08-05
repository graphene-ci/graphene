package memory

import (
	"fmt"

	"github.com/graphene-ci/graphene/internal/store/kv"
	"github.com/graphene-ci/graphene/internal/types/revision"
)

// conflict says which of the two ways a guarded write failed.
//
// Both are revision.ErrConflict, because both mean the same thing to a
// caller: the world is not what it read, so whatever it decided from that
// read has to be decided again. The text is the difference, and it is for
// whoever has to work out WHY the read was stale — "it was created while
// I was deciding" and "it moved on while I was deciding" send you looking
// in different places.
func conflict(key kv.Key, expect revision.Revision, current kv.Entry, found bool) error {
	if !found {
		return fmt.Errorf("%w: %s does not exist, expected it at %s",
			revision.ErrConflict, key, expect)
	}

	if expect.IsZero() {
		return fmt.Errorf("%w: %s already exists, at %s",
			revision.ErrConflict, key, current.Revision)
	}

	return fmt.Errorf("%w: %s is at %s, expected %s",
		revision.ErrConflict, key, current.Revision, expect)
}
