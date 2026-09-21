package cmdutil

import (
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// A map's own order differs from call to call; a table cell must not.
func TestLabelsCellIsOrdered(t *testing.T) {
	labels := map[string]string{"test": "simple", "customer": "demo", "graphene.io/run": "r1", "cert": "none"}
	want := "cert=none,customer=demo,graphene.io/run=r1,test=simple"
	for range 50 {
		if got := LabelsCell(labels); got != want {
			t.Fatalf("LabelsCell = %q, want %q", got, want)
		}
	}
	if got := LabelsCell(UserLabels(labels)); got != "cert=none,customer=demo,test=simple" {
		t.Fatalf("UserLabels kept a system label: %q", got)
	}
	if got := LabelsCell(nil); got != "" {
		t.Fatalf("no labels = %q", got)
	}
}

func TestNearestKinds(t *testing.T) {
	kinds := []string{"agent", "artifact", "docker", "docker-network", "docker-volume", "run", "secret", "stand"}
	for word, want := range map[string][]string{
		"agnt":    {"agent"},
		"agents":  {"agent"},
		"dockr":   {"docker"},
		"docker-": {"docker", "docker-volume", "docker-network"},
		"zzzzzz":  {},
	} {
		if got := nearest(word, kinds); !reflect.DeepEqual(got, want) {
			t.Errorf("nearest(%q) = %v, want %v", word, got, want)
		}
	}
}

func TestNoRecordSpeaksOfRunsByTheirId(t *testing.T) {
	if got := NoRecord("run/nightly-1").Error(); got != "no run nightly-1" {
		t.Fatalf("%q", got)
	}
	if got := NoRecord("agent/db-1").Error(); got != "no record agent/db-1" {
		t.Fatalf("%q", got)
	}
}

func TestCoarseDurations(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Second:                  "0s",
		42 * time.Second:              "42s",
		108 * time.Second:             "1m48s",
		3*time.Hour + 7*time.Minute:   "3h7m",
		50*time.Hour + 30*time.Minute: "2d2h",
	} {
		if got := coarse(d); got != want {
			t.Errorf("coarse(%v) = %q, want %q", d, got, want)
		}
	}
	start := timestamppb.New(time.Unix(1000, 0))
	if got := Took(start, timestamppb.New(time.Unix(1108, 0))); got != "1m48s" {
		t.Fatalf("Took = %q", got)
	}
	if got := Took(nil, nil); got != "" {
		t.Fatalf("Took(nil) = %q", got)
	}
}
