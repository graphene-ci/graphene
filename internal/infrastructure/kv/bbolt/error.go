package bbolt

import "errors"

// ErrCorrupt — bytes in the file that are not a record this package
// wrote.
//
// It is its own sentinel because it is the one failure nothing a caller
// did causes and nothing a caller does fixes. Everything else the store
// refuses is about the request; this is about the file, and the answer to
// it is a backup rather than a retry.
var ErrCorrupt = errors.New("bbolt: stored record is corrupt")
