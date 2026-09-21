package ui

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// withColor runs a test body with styling forced on or off.
func withColor(t *testing.T, mode string) {
	t.Helper()
	prev := enabled
	Configure(mode)
	t.Cleanup(func() { enabled = prev })
}

// Styling takes no columns: a colored cell aligns like a plain one.
func TestStyledCellsAlign(t *testing.T) {
	withColor(t, ColorAlways)
	if got := Len(Green("ready")); got != 5 {
		t.Fatalf("Len(styled) = %d, want 5", got)
	}
	if got := Len(Pad(Red("x"), 4)); got != 4 {
		t.Fatalf("Pad width = %d, want 4", got)
	}
	table := NewTable("REF", "PHASE")
	table.Row(Ref("agent/db-1"), Phase("ready"))
	table.Row(Ref("docker/pg"), Phase("creating"))
	var out bytes.Buffer
	if err := table.Render(&out, 0); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(Strip(out.String()), "\n"), "\n")
	want := []string{"REF         PHASE", "agent/db-1  ready", "docker/pg   creating"}
	if strings.Join(lines, "|") != strings.Join(want, "|") {
		t.Fatalf("table:\n%q\nwant\n%q", lines, want)
	}
}

// Off a terminal nothing is styled: a script never strips escape codes.
func TestNoColorIsPlain(t *testing.T) {
	withColor(t, ColorNever)
	for _, s := range []string{Phase("failed"), Ref("agent/db-1"), Bold("x"), Gray("y")} {
		if strings.Contains(s, "\x1b") {
			t.Fatalf("escape code with color off: %q", s)
		}
	}
}

// The flex column gives way to the terminal; the others never do.
func TestTableFlexColumnIsCut(t *testing.T) {
	withColor(t, ColorNever)
	table := NewTable("RUN", "LABELS").Flex(1)
	table.Row("nightly-1", "customer=demo,cert=none,test=simple,team=perf")
	var out bytes.Buffer
	if err := table.Render(&out, 30); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if Len(line) > 30 {
			t.Fatalf("line wider than the terminal: %q", line)
		}
	}
	if !strings.Contains(out.String(), "nightly-1") || !strings.Contains(out.String(), "…") {
		t.Fatalf("the fixed column must stay whole, the flex one be marked cut:\n%s", out.String())
	}
}

func TestTableBreaksGroups(t *testing.T) {
	withColor(t, ColorNever)
	table := NewTable("REF")
	table.Break() // before the first row: no leading blank line
	table.Row("agent/a")
	table.Break()
	table.Row("docker/b")
	var out bytes.Buffer
	if err := table.Render(&out, 0); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "REF\nagent/a\n\ndocker/b\n" {
		t.Fatalf("%q", got)
	}
}

func TestFieldsSkipEmpty(t *testing.T) {
	withColor(t, ColorNever)
	var out bytes.Buffer
	if err := Fields(&out, [2]string{"ref", "agent/db-1"}, [2]string{"owner", ""}, [2]string{"phase", "ready"}); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "ref:    agent/db-1\nphase:  ready\n" {
		t.Fatalf("%q", got)
	}
}

func TestSpark(t *testing.T) {
	if got := Spark([]float64{0, 1, 2, 3, 4, 5, 6, 7}, 8); got != "▁▂▃▄▅▆▇█" {
		t.Fatalf("ramp = %q", got)
	}
	if got := Spark([]float64{5, 5, 5}, 8); got != "▁▁▁" {
		t.Fatalf("flat = %q", got)
	}
	if got := Spark(nil, 8); got != "" {
		t.Fatalf("empty = %q", got)
	}
	// More points than columns fold by averaging — never more columns.
	long := make([]float64, 1000)
	for i := range long {
		long[i] = float64(i)
	}
	if got := Len(Spark(long, 24)); got != 24 {
		t.Fatalf("resampled width = %d", got)
	}
}

func TestChartShape(t *testing.T) {
	withColor(t, ColorNever)
	from := time.Date(2026, 9, 18, 13, 0, 0, 0, time.Local)
	rows := Chart([]float64{1, 2, 4}, from, from.Add(time.Minute), 30, 4, nil)
	if len(rows) != 5 {
		t.Fatalf("rows = %d, want 4 plot rows + the time axis", len(rows))
	}
	if !strings.HasPrefix(strings.TrimSpace(rows[0]), "4 ┤") || !strings.HasPrefix(strings.TrimSpace(rows[3]), "1 ┤") {
		t.Fatalf("axis labels:\n%s", strings.Join(rows, "\n"))
	}
	// The top row holds only the highest third; the floor is never empty.
	top := rows[0][strings.Index(rows[0], "┤")+len("┤"):]
	if strings.TrimSpace(top) == "" || strings.HasPrefix(top, "█") {
		t.Fatalf("top row: %q", top)
	}
	if strings.Contains(rows[3], " ┤ ") {
		t.Fatalf("floor row has a gap: %q", rows[3])
	}
	if !strings.Contains(rows[4], "13:00:00") || !strings.Contains(rows[4], "13:01:00") {
		t.Fatalf("time axis: %q", rows[4])
	}
	if got := Chart(nil, from, from, 30, 4, nil); got != nil {
		t.Fatalf("empty series drew %v", got)
	}
}

func TestNumbersAndUnits(t *testing.T) {
	for v, want := range map[float64]string{0: "0", 3: "3", 2493.4: "2493", 25910: "25.9k", 3.2e6: "3.2M", 0.0042: "4.2m"} {
		if got := Number(v); got != want {
			t.Errorf("Number(%v) = %q, want %q", v, got, want)
		}
	}
	if got := Bytes(72 * 1024 * 1024); got != "72MiB" {
		t.Errorf("Bytes = %q", got)
	}
	for d, want := range map[time.Duration]string{
		225 * time.Microsecond: "225µs", 57300 * time.Microsecond: "57.3ms",
		6910 * time.Millisecond: "6.91s", 108 * time.Second: "1m48s",
	} {
		if got := Duration(d); got != want {
			t.Errorf("Duration(%v) = %q, want %q", d, got, want)
		}
	}
}

// A bar sits where the span sat; a span too short to see still gets a cell.
func TestSpanBar(t *testing.T) {
	if got := Span(0, time.Second, time.Second, 10); got != "━━━━━━━━━━" {
		t.Fatalf("whole = %q", got)
	}
	if got := Span(500*time.Millisecond, time.Microsecond, time.Second, 10); got != "     ━    " {
		t.Fatalf("instant = %q", got)
	}
	if got := Span(990*time.Millisecond, time.Second, time.Second, 10); Len(got) != 10 {
		t.Fatalf("overflow = %q", got)
	}
}

// A record's spec is whatever its author made it: deep and long parts fold,
// empty ones disappear, and the caller is told that something was folded.
func TestBlockFolds(t *testing.T) {
	withColor(t, ColorNever)
	var out bytes.Buffer
	folded, err := Block(&out, "spec", []byte(`{"name":"pg","empty":{},"none":"","config":{"image":"postgres:16","host":{"mounts":[{"a":1}]}},"env":[1,2,3,4,5,6,7,8]}`))
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"spec:\n", "  name: pg\n", "    image: postgres:16\n", "      mounts: [… 1 items]\n", "  … 2 more\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "empty") || strings.Contains(got, "none") {
		t.Errorf("empty fields printed:\n%s", got)
	}
	if !folded {
		t.Error("folding was not reported")
	}
	out.Reset()
	if folded, _ := Block(&out, "spec", []byte(`{}`)); folded || out.Len() != 0 {
		t.Errorf("an empty document printed %q", out.String())
	}
}
