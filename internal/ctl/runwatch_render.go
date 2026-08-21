package ctl

// The live panel: the frame redraws in place (cursor up + clear), the
// terminal keeps the last frame when the watch ends. Long lines clip
// to the terminal width so the frame height stays exact.

import (
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/term"
)

// panelRenderer redraws a block of lines in place.
type panelRenderer struct {
	lastLines int
}

func newPanelRenderer() *panelRenderer { return &panelRenderer{} }

// draw replaces the previous frame with this one.
func (p *panelRenderer) draw(lines []string) {
	width := 120
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 20 {
		width = w
	}
	if p.lastLines > 0 {
		fmt.Fprintf(out, "\x1b[%dA\x1b[J", p.lastLines)
	}
	for _, line := range lines {
		fmt.Fprintln(out, clip(line, width))
	}
	p.lastLines = len(lines)
}

// clip bounds one line to the terminal width, counting runes (close
// enough for the frame's height bookkeeping).
func clip(s string, width int) string {
	runes := []rune(s)
	if len(runes) <= width {
		return s
	}
	return string(runes[:width-1]) + "…"
}

// frameLines renders the current model as one panel frame.
func (v *runWatchView) frameLines() []string {
	v.mu.Lock()
	defer v.mu.Unlock()

	status := v.status
	if status == "" {
		status = "starting"
	}
	lines := []string{fmt.Sprintf("run %s   %s   %s",
		v.runId, status, time.Since(v.start).Truncate(time.Second))}
	lines = append(lines, "│")

	runRef := "run/" + v.runId
	// The resource nodes in tree order (the run node renders as the
	// bottom strip instead).
	for _, ref := range v.order {
		if ref == runRef {
			continue
		}
		node, ok := v.nodes[ref]
		if !ok || node.gone {
			continue
		}
		head := fmt.Sprintf("%s─ %-36s %-10s %s",
			strings.Repeat("  ", node.depth-1)+"├",
			ref, phaseWord(node.phase), time.Since(node.firstSee).Truncate(time.Second))
		if node.attempt > 1 && node.phase != "ready" {
			head += fmt.Sprintf("   ↻ attempt %d", node.attempt)
		}
		lines = append(lines, head)
		if v.opts.collapse && node.phase == "ready" {
			continue
		}
		indent := strings.Repeat("  ", node.depth-1) + "│   "
		for _, ev := range node.events {
			lines = append(lines, indent+ev)
		}
		for _, lg := range node.logs {
			lines = append(lines, indent+lg)
		}
	}

	// The run's own strip: its events and the orchestrator's logs.
	if run, ok := v.nodes[runRef]; ok && (len(run.events) > 0 || len(run.logs) > 0) {
		lines = append(lines, strings.Repeat("─", 58))
		for _, ev := range run.events {
			lines = append(lines, "run  "+ev)
		}
		for _, lg := range run.logs {
			lines = append(lines, "     "+lg)
		}
	}
	return lines
}

func phaseWord(phase string) string {
	if phase == "" {
		return "•"
	}
	return phase
}
