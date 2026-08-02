// Package bbolt is the embedded store adapter: the same storage engine
// that backs etcd, without the cluster — a single file, pure Go. Implements
// the core store port with etcd-style MVCC: a "current" bucket holds the
// latest entries, a "log" bucket holds every write keyed by global
// revision; watch replays the log and then follows live commits.
package bbolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/graphene-ci/graphene/internal/core/store"
)

// ErrClosed is returned by Watch on a store that has been closed.
var ErrClosed = errors.New("bbolt: store closed")

// Bucket names are effectively constants; []byte cannot be const in Go.
//
//nolint:gochecknoglobals // see above
var (
	bucketMeta    = []byte("meta")
	bucketCurrent = []byte("cur")
	bucketLog     = []byte("log")

	metaRevision = []byte("revision")
)

const (
	fileMode = 0o600

	// Current bucket value layout: modRevision | createdRevision | payload.
	curHeaderLen = revLen * 2
	// Log bucket value layout: type | createdRevision | keyLen | key | payload.
	logHeaderLen = 1 + revLen + keyLenLen

	revLen    = 8
	keyLenLen = 4

	watchBuffer = 256
)

// Store implements store.Store over a bbolt file.
type Store struct {
	db *bolt.DB

	mu     sync.Mutex // guards subs and commit→publish ordering
	subs   map[*subscriber]struct{}
	closed bool
}

type subscriber struct {
	prefix []byte
	ch     chan store.Event
	// lastSent deduplicates the replay/live boundary: events with
	// revision <= lastSent are dropped.
	lastSent uint64
}

// Open creates or opens the store file.
func Open(path string) (*Store, error) {
	database, err := bolt.Open(path, fileMode, nil)
	if err != nil {
		return nil, fmt.Errorf("bbolt: open: %w", err)
	}

	err = database.Update(func(txn *bolt.Tx) error {
		for _, bucket := range [][]byte{bucketMeta, bucketCurrent, bucketLog} {
			if _, err := txn.CreateBucketIfNotExists(bucket); err != nil {
				return fmt.Errorf("create bucket %q: %w", bucket, err)
			}
		}

		return nil
	})
	if err != nil {
		_ = database.Close()

		return nil, fmt.Errorf("bbolt: init: %w", err)
	}

	return &Store{db: database, subs: make(map[*subscriber]struct{})}, nil
}

// Close shuts the store down and closes every watch channel.
func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true

	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
	s.mu.Unlock()

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("bbolt: close: %w", err)
	}

	return nil
}

// --- encoding -----------------------------------------------------------

func encodeCurrent(rev, createdRev uint64, value []byte) []byte {
	out := make([]byte, curHeaderLen+len(value))
	binary.BigEndian.PutUint64(out, rev)
	binary.BigEndian.PutUint64(out[revLen:], createdRev)
	copy(out[curHeaderLen:], value)

	return out
}

func decodeCurrent(raw []byte) (uint64, uint64, []byte) {
	return binary.BigEndian.Uint64(raw[:revLen]),
		binary.BigEndian.Uint64(raw[revLen:curHeaderLen]),
		raw[curHeaderLen:]
}

func encodeLogValue(typ store.EventType, createdRev uint64, key, value []byte) []byte {
	if len(key) > math.MaxUint32 {
		panic("bbolt: key longer than 4GiB")
	}

	out := make([]byte, logHeaderLen+len(key)+len(value))
	out[0] = byte(typ)
	binary.BigEndian.PutUint64(out[1:], createdRev)
	binary.BigEndian.PutUint32(out[1+revLen:], uint32(len(key))) //nolint:gosec // guarded by the MaxUint32 check above
	copy(out[logHeaderLen:], key)
	copy(out[logHeaderLen+len(key):], value)

	return out
}

func decodeLogValue(rev uint64, raw []byte) store.Event {
	typ := store.EventType(raw[0])
	createdRev := binary.BigEndian.Uint64(raw[1 : 1+revLen])
	keyLen := binary.BigEndian.Uint32(raw[1+revLen : logHeaderLen])
	key := raw[logHeaderLen : logHeaderLen+keyLen]
	value := raw[logHeaderLen+keyLen:]

	return store.Event{
		Type: typ,
		Entry: store.Entry{
			Key:             bytes.Clone(key),
			Value:           bytes.Clone(value),
			Revision:        rev,
			CreatedRevision: createdRev,
		},
		StoreRevision: rev,
	}
}

func revKey(rev uint64) []byte {
	out := make([]byte, revLen)
	binary.BigEndian.PutUint64(out, rev)

	return out
}

// --- reads --------------------------------------------------------------

// Get returns the current entry or store.ErrNotFound.
func (s *Store) Get(_ context.Context, key []byte) (store.Entry, error) {
	var entry store.Entry

	err := s.db.View(func(txn *bolt.Tx) error {
		raw := txn.Bucket(bucketCurrent).Get(key)
		if raw == nil {
			return store.ErrNotFound
		}

		rev, createdRev, value := decodeCurrent(raw)
		entry = store.Entry{
			Key:             bytes.Clone(key),
			Value:           bytes.Clone(value),
			Revision:        rev,
			CreatedRevision: createdRev,
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Entry{}, store.ErrNotFound
		}

		return store.Entry{}, fmt.Errorf("bbolt: get: %w", err)
	}

	return entry, nil
}

// Revision returns the current global store revision.
func (s *Store) Revision(_ context.Context) (uint64, error) {
	var rev uint64

	err := s.db.View(func(txn *bolt.Tx) error {
		rev = currentRevision(txn)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("bbolt: revision: %w", err)
	}

	return rev, nil
}

func currentRevision(txn *bolt.Tx) uint64 {
	raw := txn.Bucket(bucketMeta).Get(metaRevision)
	if raw == nil {
		return 0
	}

	return binary.BigEndian.Uint64(raw)
}

// Scan lists entries under prefix in key order; see the port contract.
func (s *Store) Scan(_ context.Context, prefix []byte, limit int, startAfter []byte) ([]store.Entry, []byte, error) {
	var (
		entries []store.Entry
		cursor  []byte
	)

	err := s.db.View(func(txn *bolt.Tx) error {
		iter := txn.Bucket(bucketCurrent).Cursor()

		var key, raw []byte
		if startAfter == nil {
			key, raw = iter.Seek(prefix)
		} else {
			key, raw = iter.Seek(startAfter)
			if bytes.Equal(key, startAfter) {
				key, raw = iter.Next()
			}
		}

		for ; key != nil && bytes.HasPrefix(key, prefix); key, raw = iter.Next() {
			rev, createdRev, value := decodeCurrent(raw)
			entries = append(entries, store.Entry{
				Key:             bytes.Clone(key),
				Value:           bytes.Clone(value),
				Revision:        rev,
				CreatedRevision: createdRev,
			})

			if limit > 0 && len(entries) == limit {
				// More may follow: hand out a cursor.
				if next, _ := iter.Next(); next != nil && bytes.HasPrefix(next, prefix) {
					cursor = bytes.Clone(key)
				}

				return nil
			}
		}

		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("bbolt: scan: %w", err)
	}

	return entries, cursor, nil
}

// --- writes -------------------------------------------------------------

// Put writes value under key guarded by CAS; see the port contract.
func (s *Store) Put(_ context.Context, key, value []byte, expectedRevision uint64) (uint64, error) {
	return s.write(store.EventPut, key, value, expectedRevision)
}

// Delete removes the entry guarded by CAS; see the port contract.
func (s *Store) Delete(_ context.Context, key []byte, expectedRevision uint64) (uint64, error) {
	return s.write(store.EventDelete, key, nil, expectedRevision)
}

func (s *Store) write(typ store.EventType, key, value []byte, expectedRevision uint64) (uint64, error) {
	// Publish under the same lock that guards subscriber registration:
	// a watcher either sees this commit in its log replay or receives it
	// live — never neither (see Watch).
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		newRev, createdRev uint64

		eventValue = value
	)

	err := s.db.Update(func(txn *bolt.Tx) error {
		return s.writeTx(txn, typ, key, value, expectedRevision, &newRev, &createdRev, &eventValue)
	})

	switch {
	case errors.Is(err, store.ErrRevisionMismatch), errors.Is(err, store.ErrNotFound):
		return 0, err //nolint:wrapcheck // port sentinel errors pass through as-is
	case err != nil:
		return 0, fmt.Errorf("bbolt: write: %w", err)
	}

	event := store.Event{
		Type: typ,
		Entry: store.Entry{
			Key:             bytes.Clone(key),
			Value:           bytes.Clone(eventValue),
			Revision:        newRev,
			CreatedRevision: createdRev,
		},
		StoreRevision: newRev,
	}
	for sub := range s.subs {
		if !sub.publish(&event) {
			// Slow consumer: close and drop — it must re-Watch from its
			// last seen revision. Closing here is safe: publish happens
			// only under s.mu, and the sub is removed before unlock.
			close(sub.ch)
			delete(s.subs, sub)
		}
	}

	return newRev, nil
}

func (s *Store) writeTx(
	txn *bolt.Tx,
	typ store.EventType,
	key, value []byte,
	expectedRevision uint64,
	newRev, createdRev *uint64,
	eventValue *[]byte,
) error {
	current := txn.Bucket(bucketCurrent)

	var (
		haveRev   uint64
		prevValue []byte
	)

	if raw := current.Get(key); raw != nil {
		haveRev, *createdRev, prevValue = decodeCurrent(raw)
	}

	if haveRev != expectedRevision {
		return store.ErrRevisionMismatch
	}

	if typ == store.EventDelete && haveRev == 0 {
		return store.ErrNotFound
	}

	*newRev = currentRevision(txn) + 1
	if haveRev == 0 {
		*createdRev = *newRev // fresh incarnation
	}

	if err := txn.Bucket(bucketMeta).Put(metaRevision, revKey(*newRev)); err != nil {
		return fmt.Errorf("meta put: %w", err)
	}

	if typ == store.EventPut {
		if err := current.Put(key, encodeCurrent(*newRev, *createdRev, value)); err != nil {
			return fmt.Errorf("current put: %w", err)
		}
	} else if err := current.Delete(key); err != nil {
		return fmt.Errorf("current delete: %w", err)
	}

	logged := value
	if typ == store.EventDelete {
		// prev_kv semantics: the delete event carries the last value.
		logged = bytes.Clone(prevValue)
		*eventValue = logged
	}

	if err := txn.Bucket(bucketLog).Put(revKey(*newRev), encodeLogValue(typ, *createdRev, key, logged)); err != nil {
		return fmt.Errorf("log put: %w", err)
	}

	return nil
}

// --- watch --------------------------------------------------------------

// publish delivers the event if it matches; false = overflow (caller
// closes and removes the subscriber). Always called under Store.mu.
func (sub *subscriber) publish(event *store.Event) bool {
	if !bytes.HasPrefix(event.Entry.Key, sub.prefix) || event.StoreRevision <= sub.lastSent {
		return true
	}

	select {
	case sub.ch <- *event:
		sub.lastSent = event.StoreRevision

		return true
	default:
		return false
	}
}

// Watch streams events under prefix; see the port contract for the
// fromRevision semantics.
func (s *Store) Watch(ctx context.Context, prefix []byte, fromRevision uint64) (<-chan store.Event, error) {
	sub := &subscriber{
		prefix: bytes.Clone(prefix),
		ch:     make(chan store.Event, watchBuffer),
	}

	// Registration and backlog collection happen under the commit lock:
	// every write is either in the backlog we are about to send or will be
	// published live to the already-registered subscriber. lastSent forbids
	// duplicates on the boundary.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()

		return nil, ErrClosed
	}

	backlog, head, err := s.collectBacklog(prefix, fromRevision)
	if err != nil {
		s.mu.Unlock()

		return nil, err
	}

	// The sync marker closes the catch-up phase: everything up to head is
	// in the backlog, everything after arrives live.
	backlog = append(backlog, store.Event{Type: store.EventSync, StoreRevision: head})

	s.subs[sub] = struct{}{}
	s.mu.Unlock()

	out := make(chan store.Event)
	go s.pump(ctx, sub, backlog, out)

	return out, nil
}

func (s *Store) collectBacklog(prefix []byte, fromRevision uint64) ([]store.Event, uint64, error) {
	var (
		backlog []store.Event
		head    uint64
	)

	err := s.db.View(func(txn *bolt.Tx) error {
		head = currentRevision(txn)

		if fromRevision == 0 {
			// Snapshot of the current state as synthetic PUTs.
			iter := txn.Bucket(bucketCurrent).Cursor()
			for key, raw := iter.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, raw = iter.Next() {
				rev, createdRev, value := decodeCurrent(raw)
				backlog = append(backlog, store.Event{
					Type: store.EventPut,
					Entry: store.Entry{
						Key:             bytes.Clone(key),
						Value:           bytes.Clone(value),
						Revision:        rev,
						CreatedRevision: createdRev,
					},
					StoreRevision: rev,
				})
			}

			return nil
		}

		// Replay the log strictly after fromRevision.
		iter := txn.Bucket(bucketLog).Cursor()
		for key, raw := iter.Seek(revKey(fromRevision + 1)); key != nil; key, raw = iter.Next() {
			event := decodeLogValue(binary.BigEndian.Uint64(key), raw)
			if bytes.HasPrefix(event.Entry.Key, prefix) {
				backlog = append(backlog, event)
			}
		}

		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("bbolt: watch backlog: %w", err)
	}

	return backlog, head, nil
}

func (s *Store) pump(ctx context.Context, sub *subscriber, backlog []store.Event, out chan<- store.Event) {
	defer func() {
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
		close(out)
	}()

	for _, event := range backlog {
		select {
		case out <- event:
			// Snapshot events may repeat in live (same revision):
			// remember the max to dedupe.
			s.mu.Lock()
			if event.StoreRevision > sub.lastSent {
				sub.lastSent = event.StoreRevision
			}
			s.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case event, ok := <-sub.ch:
			if !ok {
				return
			}

			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
