package telemetry

import (
	"testing"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func env(ns, entity string) Envelope {
	return Envelope{Type: "log", Namespace: ns,
		Keys:    map[string]string{"graphene.entity": entity},
		Payload: &collogspb.ExportLogsServiceRequest{}}
}

// A subscription sees exactly its subject, in its namespace.
func TestHubRouting(t *testing.T) {
	h := NewHub()
	sub := h.Subscribe(Selector{Namespace: "default", Attribute: "graphene.entity", Value: "docker/db"}, "log")
	defer sub.Close()
	h.Publish(env("default", "docker/db"))
	h.Publish(env("default", "docker/other"))
	h.Publish(env("elsewhere", "docker/db"))
	if got := len(sub.ch); got != 1 {
		t.Fatalf("want exactly the matching envelope, got %d", got)
	}
}

// A slow consumer loses the OLDEST and is told how many; the stream
// itself keeps flowing.
func TestHubShedsOldest(t *testing.T) {
	h := NewHub()
	sub := h.Subscribe(Selector{Namespace: "n", Attribute: "graphene.entity", Value: "x"})
	defer sub.Close()
	for range subBuffer + 5 {
		h.Publish(env("n", "x"))
	}
	if len(sub.ch) != subBuffer {
		t.Fatalf("buffer must stay full, got %d", len(sub.ch))
	}
	if d := sub.Dropped(); d != 5 {
		t.Fatalf("want 5 dropped, got %d", d)
	}
	if d := sub.Dropped(); d != 0 {
		t.Fatalf("dropped must reset on read, got %d", d)
	}
}

// Close detaches: publishing after close must not panic or deliver.
func TestHubClose(t *testing.T) {
	h := NewHub()
	sub := h.Subscribe(Selector{Namespace: "n", Attribute: "graphene.entity", Value: "x"})
	sub.Close()
	sub.Close() // idempotent
	h.Publish(env("n", "x"))
	if _, ok := <-sub.C(); ok {
		t.Fatal("a closed subscription must not deliver")
	}
}
