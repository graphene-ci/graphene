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
	"fmt"
	"sync"

	bolt "go.etcd.io/bbolt"

	"github.com/graphene-ci/graphene/internal/core/store"
)

var (
	bucketMeta    = []byte("meta")
	bucketCurrent = []byte("cur")
	bucketLog     = []byte("log")

	metaRevision = []byte("revision")
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
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("bbolt: open: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketCurrent, bucketLog} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bbolt: init: %w", err)
	}
	return &Store{db: db, subs: make(map[*subscriber]struct{})}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true
	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
	s.mu.Unlock()
	return s.db.Close()
}

// --- encoding -----------------------------------------------------------

// current bucket value: 8B mod revision | 8B created revision | payload
func encodeCurrent(rev, createdRev uint64, value []byte) []byte {
	out := make([]byte, 16+len(value))
	binary.BigEndian.PutUint64(out, rev)
	binary.BigEndian.PutUint64(out[8:], createdRev)
	copy(out[16:], value)
	return out
}

func decodeCurrent(raw []byte) (rev, createdRev uint64, value []byte) {
	return binary.BigEndian.Uint64(raw[:8]), binary.BigEndian.Uint64(raw[8:16]), raw[16:]
}

// log bucket key: 8B revision; value: 1B type | 8B createdRev | 4B keyLen | key | payload
func encodeLogValue(typ store.EventType, createdRev uint64, key, value []byte) []byte {
	out := make([]byte, 1+8+4+len(key)+len(value))
	out[0] = byte(typ)
	binary.BigEndian.PutUint64(out[1:], createdRev)
	binary.BigEndian.PutUint32(out[9:], uint32(len(key)))
	copy(out[13:], key)
	copy(out[13+len(key):], value)
	return out
}

func decodeLogValue(rev uint64, raw []byte) store.Event {
	typ := store.EventType(raw[0])
	createdRev := binary.BigEndian.Uint64(raw[1:9])
	klen := binary.BigEndian.Uint32(raw[9:13])
	key := raw[13 : 13+klen]
	value := raw[13+klen:]
	return store.Event{
		Type: typ,
		Entry: store.Entry{
			Key:             append([]byte(nil), key...),
			Value:           append([]byte(nil), value...),
			Revision:        rev,
			CreatedRevision: createdRev,
		},
		StoreRevision: rev,
	}
}

func revKey(rev uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, rev)
	return out
}

// --- reads --------------------------------------------------------------

func (s *Store) Get(_ context.Context, key []byte) (store.Entry, error) {
	var entry store.Entry
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketCurrent).Get(key)
		if raw == nil {
			return store.ErrNotFound
		}
		rev, createdRev, value := decodeCurrent(raw)
		entry = store.Entry{
			Key:             append([]byte(nil), key...),
			Value:           append([]byte(nil), value...),
			Revision:        rev,
			CreatedRevision: createdRev,
		}
		return nil
	})
	return entry, err
}

func (s *Store) Revision(_ context.Context) (uint64, error) {
	var rev uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		rev = currentRevision(tx)
		return nil
	})
	return rev, err
}

func currentRevision(tx *bolt.Tx) uint64 {
	raw := tx.Bucket(bucketMeta).Get(metaRevision)
	if raw == nil {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func (s *Store) Scan(_ context.Context, prefix []byte, limit int, startAfter []byte) ([]store.Entry, []byte, error) {
	var entries []store.Entry
	var cursor []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketCurrent).Cursor()
		var k, v []byte
		if startAfter == nil {
			k, v = c.Seek(prefix)
		} else {
			k, v = c.Seek(startAfter)
			if bytes.Equal(k, startAfter) {
				k, v = c.Next()
			}
		}
		for ; k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			rev, createdRev, value := decodeCurrent(v)
			entries = append(entries, store.Entry{
				Key:             append([]byte(nil), k...),
				Value:           append([]byte(nil), value...),
				Revision:        rev,
				CreatedRevision: createdRev,
			})
			if limit > 0 && len(entries) == limit {
				// More may follow: hand out a cursor.
				if next, _ := c.Next(); next != nil && bytes.HasPrefix(next, prefix) {
					cursor = append([]byte(nil), k...)
				}
				return nil
			}
		}
		return nil
	})
	return entries, cursor, err
}

// --- writes -------------------------------------------------------------

func (s *Store) Put(_ context.Context, key, value []byte, expectedRevision uint64) (uint64, error) {
	return s.write(store.EventPut, key, value, expectedRevision)
}

func (s *Store) Delete(_ context.Context, key []byte, expectedRevision uint64) (uint64, error) {
	return s.write(store.EventDelete, key, nil, expectedRevision)
}

func (s *Store) write(typ store.EventType, key, value []byte, expectedRevision uint64) (uint64, error) {
	var newRev uint64
	var event store.Event

	// Publish under the same lock that guards subscriber registration:
	// a watcher either sees this commit in its log replay or receives it
	// live — never neither (see Watch).
	s.mu.Lock()
	defer s.mu.Unlock()

	var createdRev uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bucketCurrent)

		var haveRev uint64
		if raw := cur.Get(key); raw != nil {
			haveRev, createdRev, _ = decodeCurrent(raw)
		}
		if haveRev != expectedRevision {
			return store.ErrRevisionMismatch
		}
		if typ == store.EventDelete && haveRev == 0 {
			return store.ErrNotFound
		}

		newRev = currentRevision(tx) + 1
		if haveRev == 0 {
			createdRev = newRev // fresh incarnation
		}
		if err := tx.Bucket(bucketMeta).Put(metaRevision, revKey(newRev)); err != nil {
			return err
		}
		if typ == store.EventPut {
			if err := cur.Put(key, encodeCurrent(newRev, createdRev, value)); err != nil {
				return err
			}
		} else {
			if err := cur.Delete(key); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketLog).Put(revKey(newRev), encodeLogValue(typ, createdRev, key, value))
	})
	if err != nil {
		return 0, err
	}

	event = store.Event{
		Type: typ,
		Entry: store.Entry{
			Key:             append([]byte(nil), key...),
			Value:           append([]byte(nil), value...),
			Revision:        newRev,
			CreatedRevision: createdRev,
		},
		StoreRevision: newRev,
	}
	for sub := range s.subs {
		if !sub.publish(event) {
			// Slow consumer: close and drop — it must re-Watch from its
			// last seen revision. Closing here is safe: publish happens
			// only under s.mu, and the sub is removed before unlock.
			close(sub.ch)
			delete(s.subs, sub)
		}
	}
	return newRev, nil
}

// --- watch --------------------------------------------------------------

const watchBuffer = 256

// publish delivers the event if it matches; false = overflow (caller
// closes and removes the subscriber). Always called under Store.mu.
func (sub *subscriber) publish(ev store.Event) bool {
	if !bytes.HasPrefix(ev.Entry.Key, sub.prefix) || ev.StoreRevision <= sub.lastSent {
		return true
	}
	select {
	case sub.ch <- ev:
		sub.lastSent = ev.StoreRevision
		return true
	default:
		return false
	}
}

func (s *Store) Watch(ctx context.Context, prefix []byte, fromRevision uint64) (<-chan store.Event, error) {
	sub := &subscriber{
		prefix: append([]byte(nil), prefix...),
		ch:     make(chan store.Event, watchBuffer),
	}

	// Registration and backlog collection happen under the commit lock:
	// every write is either in the backlog we are about to send or will be
	// published live to the already-registered subscriber. lastSent forbids
	// duplicates on the boundary.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("bbolt: store closed")
	}
	var backlog []store.Event
	err := s.db.View(func(tx *bolt.Tx) error {
		if fromRevision == 0 {
			// Snapshot of the current state as synthetic PUTs.
			c := tx.Bucket(bucketCurrent).Cursor()
			for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
				rev, createdRev, value := decodeCurrent(v)
				backlog = append(backlog, store.Event{
					Type: store.EventPut,
					Entry: store.Entry{
						Key:             append([]byte(nil), k...),
						Value:           append([]byte(nil), value...),
						Revision:        rev,
						CreatedRevision: createdRev,
					},
					StoreRevision: rev,
				})
			}
			return nil
		}
		// Replay the log strictly after fromRevision.
		c := tx.Bucket(bucketLog).Cursor()
		for k, v := c.Seek(revKey(fromRevision + 1)); k != nil; k, v = c.Next() {
			rev := binary.BigEndian.Uint64(k)
			ev := decodeLogValue(rev, v)
			if bytes.HasPrefix(ev.Entry.Key, prefix) {
				backlog = append(backlog, ev)
			}
		}
		return nil
	})
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.subs[sub] = struct{}{}
	s.mu.Unlock()

	out := make(chan store.Event)
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.subs, sub)
			s.mu.Unlock()
			close(out)
		}()
		for _, ev := range backlog {
			select {
			case out <- ev:
				// Snapshot events may repeat in live (same revision):
				// remember the max to dedupe.
				s.mu.Lock()
				if ev.StoreRevision > sub.lastSent {
					sub.lastSent = ev.StoreRevision
				}
				s.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
		for {
			select {
			case ev, ok := <-sub.ch:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
