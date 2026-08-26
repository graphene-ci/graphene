package telemetry

// The hub is the LIVE half of dimensions 3-5. The door is already the
// collector — every signal of every worker passes through it — so the
// door fans them out to live subscribers as they arrive, and a follow
// stream is a push, never a poll.
//
// The hub does NOT re-describe telemetry: the payload stays the OTLP
// message it arrived as, cut down to the records of one subject. A
// private model would be a forever-chasing copy of OTel (histogram
// buckets, span links, exemplars...); the hub's whole job is routing,
// so an envelope carries ONLY the routing keys next to the raw
// payload. Victoria keeps the history; the hub keeps nothing but the
// moment.

import (
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

// Envelope is one routed telemetry unit: the records of ONE subject
// from one export, still in their standard form.
type Envelope struct {
	// Type is "log" | "metric" | "span".
	Type string
	// Namespace is the caller's, stamped by the door — never the
	// payload's claim.
	Namespace string
	// Keys is the matching surface: the correlation attributes found
	// on the resource and the records ("graphene.entity",
	// "graphene.run", "graphene.agent").
	Keys map[string]string
	// Payload is the OTLP message (ExportLogsServiceRequest,
	// ExportMetricsServiceRequest or ExportTraceServiceRequest) holding
	// only this subject's records.
	Payload proto.Message
}

// Matches reports whether the envelope answers the selector — the
// SAME address language the Victoria drivers query by.
func (e Envelope) Matches(sel Selector) bool {
	if sel.Namespace != "" && e.Namespace != sel.Namespace {
		return false
	}
	return e.Keys[sel.Attribute] == sel.Value
}

// subBuffer bounds one subscriber. Telemetry is not a transaction
// log: a slow consumer loses the OLDEST envelopes and is told how
// many.
const subBuffer = 256

// Subscription is one live listener.
type Subscription struct {
	hub     *Hub
	id      int
	sel     Selector
	types   map[string]bool
	ch      chan Envelope
	dropped atomic.Int64
}

// C is the envelope stream.
func (s *Subscription) C() <-chan Envelope { return s.ch }

// Dropped counts envelopes lost to a slow consumer since the last
// call (the counter resets on read, so a stream reports increments).
func (s *Subscription) Dropped() int64 { return s.dropped.Swap(0) }

// Close detaches the subscription.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if _, ok := s.hub.subs[s.id]; ok {
		delete(s.hub.subs, s.id)
		close(s.ch)
	}
}

// Hub fans envelopes out to live subscriptions.
type Hub struct {
	mu   sync.Mutex
	subs map[int]*Subscription
	next int
	// drops counts every shed envelope, hub-wide — the hub's own
	// observability.
	drops atomic.Int64
}

// Stats reports the hub's own state: live subscriptions and total
// envelopes shed since start.
func (h *Hub) Stats() (subscriptions int, dropped int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs), h.drops.Load()
}

// NewHub builds an empty hub.
func NewHub() *Hub { return &Hub{subs: map[int]*Subscription{}} }

// Subscribe attaches a listener for the selector; types narrows to
// signal types ("log", "metric", "span"), empty takes all.
func (h *Hub) Subscribe(sel Selector, types ...string) *Subscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	sub := &Subscription{hub: h, id: h.next, sel: sel, ch: make(chan Envelope, subBuffer)}
	if len(types) > 0 {
		sub.types = map[string]bool{}
		for _, t := range types {
			sub.types[t] = true
		}
	}
	h.subs[h.next] = sub
	h.next++
	return sub
}

// Publish routes one envelope to every matching subscription. It
// never blocks: forwarding to storage and serving live listeners must
// not hold each other hostage. A full subscriber sheds its OLDEST
// envelope so the stream stays current.
func (h *Hub) Publish(env Envelope) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, sub := range h.subs {
		if sub.types != nil && !sub.types[env.Type] {
			continue
		}
		if !env.Matches(sub.sel) {
			continue
		}
		select {
		case sub.ch <- env:
		default:
			select {
			case <-sub.ch:
				sub.dropped.Add(1)
				h.drops.Add(1)
			default:
			}
			select {
			case sub.ch <- env:
			default:
				sub.dropped.Add(1)
				h.drops.Add(1)
			}
		}
	}
}
