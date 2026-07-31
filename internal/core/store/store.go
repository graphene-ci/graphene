// Package store defines the port for the control kernel's truth storage:
// an MVCC key-value space with CAS writes, prefix scans and revision-based
// watch — the etcd model, backend-agnostic.
//
// The core never talks to a concrete database; implementations live in
// internal/infrastructure/store/* (bbolt today, etcd/postgres tomorrow) and
// must pass the conformance suite in storetest.
//
// Revision model (etcd-style):
//   - every committed write bumps the global store revision by one;
//   - an entry's Revision is the store revision of its last write
//     (mod-revision): monotonic per key, and the CAS token;
//   - the write log is addressable by store revision — Watch resumes from
//     any revision without gaps; retention/compaction is backend policy.
package store

import (
	"bytes"
	"context"
	"errors"
)

var (
	// ErrNotFound — no entry under the key.
	ErrNotFound = errors.New("store: not found")
	// ErrRevisionMismatch — CAS guard failed: the entry's current revision
	// differs from the expected one (or it exists while 0 was expected).
	// The caller re-reads and decides; never retried blindly.
	ErrRevisionMismatch = errors.New("store: revision mismatch")
	// ErrCompacted — the requested watch revision is older than the
	// retained log; the watcher must re-list and re-watch from now.
	ErrCompacted = errors.New("store: revision compacted")
)

// Entry is one stored record.
type Entry struct {
	Key   []byte
	Value []byte
	// Revision is the store revision of the last write to this key
	// (mod-revision): the CAS token for Put/Delete.
	Revision uint64
	// CreatedRevision is the store revision at which this key was created —
	// the incarnation identity: delete+recreate under the same key yields a
	// new CreatedRevision.
	CreatedRevision uint64
}

// EventType distinguishes watch events.
type EventType byte

const (
	EventPut    EventType = 1
	EventDelete EventType = 2
)

// Event is one element of the watch stream.
type Event struct {
	Type EventType
	// Entry.Value is empty for EventDelete; Entry.Revision is the store
	// revision of this very write.
	Entry Entry
	// StoreRevision equals Entry.Revision; kept explicit as the resume
	// cursor (WatchRequest.from_store_revision semantics).
	StoreRevision uint64
}

// Store is the port. All methods are safe for concurrent use.
type Store interface {
	// Get returns the current entry or ErrNotFound.
	Get(ctx context.Context, key []byte) (Entry, error)

	// Put writes value under key guarded by CAS: expectedRevision must be
	// the entry's current Revision, or 0 for "must not exist" (create).
	// Returns the new revision (== the global store revision of the write).
	Put(ctx context.Context, key, value []byte, expectedRevision uint64) (uint64, error)

	// Delete removes the entry, guarded the same way as Put.
	// Returns the store revision of the delete.
	Delete(ctx context.Context, key []byte, expectedRevision uint64) (uint64, error)

	// Scan lists entries whose key starts with prefix, in key order,
	// at most limit (0 = no limit), strictly after startAfter (nil = from
	// the beginning). The returned cursor is the last key, to be passed as
	// the next startAfter; nil cursor = exhausted.
	Scan(ctx context.Context, prefix []byte, limit int, startAfter []byte) ([]Entry, []byte, error)

	// Watch streams events for keys under prefix.
	//
	// fromRevision semantics (mirrors the wire contract):
	//   - 0: first the current state as synthetic EventPut per entry
	//     (snapshot), then live events;
	//   - >0: replay logged events with revision > fromRevision, then live.
	//     ErrCompacted if that part of the log is gone.
	//
	// The channel closes when ctx is done, the store closes, or the
	// consumer is too slow to keep up — in every case the consumer is
	// expected to re-Watch from its last seen StoreRevision.
	Watch(ctx context.Context, prefix []byte, fromRevision uint64) (<-chan Event, error)

	// Revision returns the current global store revision.
	Revision(ctx context.Context) (uint64, error)

	Close() error
}

// Key segment separators. A full key is:
//
//	kind 0x1E seg1 0x1F seg2 0x1F ... segN 0x1F
//
// Every segment is terminated (including the last): the encoding of
// (kind, p...) is then a strict byte-prefix of the encoding of
// (kind, p..., q...) and of nothing else — prefix Scan/Watch match whole
// segments, never "app" matching "app2". Segments must not contain the
// separator bytes; that is validated by the definition layer, not here.
const (
	sepKind    = 0x1E
	sepSegment = 0x1F
)

// EncodeKey builds the stored key for kind + full path.
func EncodeKey(kind string, path ...string) []byte {
	var b bytes.Buffer
	b.WriteString(kind)
	b.WriteByte(sepKind)

	for _, seg := range path {
		b.WriteString(seg)
		b.WriteByte(sepSegment)
	}

	return b.Bytes()
}

// EncodePrefix builds a scan/watch prefix: same encoding — a full key of a
// shorter path IS the prefix of all its descendants (and itself).
func EncodePrefix(kind string, path ...string) []byte {
	return EncodeKey(kind, path...)
}

// DecodeKey splits a stored key back into kind and path segments.
func DecodeKey(key []byte) (string, []string) {
	idx := bytes.IndexByte(key, sepKind)
	if idx < 0 {
		return string(key), nil
	}

	kind := string(key[:idx])
	rest := key[idx+1:]

	var path []string

	for len(rest) > 0 {
		j := bytes.IndexByte(rest, sepSegment)
		if j < 0 {
			path = append(path, string(rest))

			break
		}

		path = append(path, string(rest[:j]))
		rest = rest[j+1:]
	}

	return kind, path
}
