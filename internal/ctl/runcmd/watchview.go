// Package runcmd carries the run lifecycle verbs and the rich watch.
package runcmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/term"

	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
)

// watchOptions tune the rendering.
type watchOptions struct {
	// plain prints an append-only feed (the non-TTY form, forced by
	// --plain).
	plain bool
	// collapse folds ready nodes to one line.
	collapse bool
	// logs: "none" | "tail" | "all".
	logs string
}

const (
	logsNone = "none"
	logsTail = "tail"
	logsAll  = "all"

	eventsKept  = 2
	logTailKept = 2
	logAllKept  = 200
)

// nodeState is one resource (or the run itself) in the view.
type nodeState struct {
	ref      string
	phase    string
	owner    string
	firstSee time.Time
	attempt  int32
	events   []string
	logs     []string
	// cursors of the incremental pulls
	afterEventId int64
	logsSince    int64
	depth        int
	gone         bool
}

// runWatchView drives one watch.
type runWatchView struct {
	d     *cmdutil.Door
	runId string
	opts  watchOptions

	mu     sync.Mutex
	status string
	order  []string // render order: tree walk
	nodes  map[string]*nodeState
	start  time.Time
	// plainSink prints feed lines immediately (plain mode).
	plainSink func(ref, line string)
}

// richWatch follows the run as a live resource tree until a terminal
// status; returns the terminal status.
func richWatch(ctx context.Context, d *cmdutil.Door, runId string, opts watchOptions) (string, error) {
	v := &runWatchView{
		d: d, runId: runId, opts: opts,
		nodes: map[string]*nodeState{},
		start: time.Now(),
	}
	runNode := &nodeState{ref: "run/" + runId, firstSee: time.Now()}
	v.nodes[runNode.ref] = runNode

	var render *panelRenderer
	if opts.plain {
		v.plainSink = func(ref, line string) {
			fmt.Fprintf(cmdutil.Out, "%s  %-28s %s\n", time.Now().Format("15:04:05"), ref, line)
		}
	} else {
		render = newPanelRenderer()
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	frame := time.NewTicker(500 * time.Millisecond)
	defer frame.Stop()
	v.tick(ctx) // the first pull before the first frame

	for {
		select {
		case <-ctx.Done():
			return v.currentStatus(), ctx.Err()
		case <-ticker.C:
			v.tick(ctx)
			if s := v.currentStatus(); terminalStatus(s) {
				// One last pull picks up the closing events, then the
				// final frame stays on screen.
				v.tick(ctx)
				if render != nil {
					render.draw(v.frameLines())
				}
				return s, nil
			}
		case <-frame.C:
			if render != nil {
				render.draw(v.frameLines())
			}
		}
	}
}

func terminalStatus(s string) bool {
	switch s {
	case "", "Running":
		return false
	}
	return true
}

func (v *runWatchView) currentStatus() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.status
}

// tick pulls one round: status, tree, per-node events and logs.
func (v *runWatchView) tick(ctx context.Context) {
	pullCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if resp, err := v.d.Runs.GetRun(pullCtx, connect.NewRequest(&managementv1.GetRunRequest{RunId: v.runId})); err == nil {
		v.mu.Lock()
		if resp.Msg.GetStatus() != v.status {
			v.status = resp.Msg.GetStatus()
			if v.plainSink != nil {
				v.plainSink("run/"+v.runId, "status "+v.status)
			}
		}
		v.mu.Unlock()
	}

	v.pullTree(pullCtx)

	v.mu.Lock()
	refs := make([]string, 0, len(v.nodes))
	for ref := range v.nodes {
		refs = append(refs, ref)
	}
	v.mu.Unlock()
	sort.Strings(refs)
	for _, ref := range refs {
		v.pullEvents(pullCtx, ref)
		if v.opts.logs != logsNone {
			v.pullLogs(pullCtx, ref)
		}
	}
}

// pullTree refreshes the node set and the render order from the
// ownership tree; the run node always leads.
func (v *runWatchView) pullTree(ctx context.Context) {
	resp, err := v.d.Resources.Tree(ctx, connect.NewRequest(&managementv1.TreeRequest{Owner: "run/" + v.runId}))
	if err != nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	seen := map[string]bool{"run/" + v.runId: true}
	order := []string{"run/" + v.runId}
	var walk func(node *managementv1.TreeNode, depth int)
	walk = func(node *managementv1.TreeNode, depth int) {
		r := node.GetResource()
		ref := r.GetRef()
		seen[ref] = true
		st, ok := v.nodes[ref]
		if !ok {
			st = &nodeState{ref: ref, firstSee: time.Now(), depth: depth}
			v.nodes[ref] = st
			if v.plainSink != nil {
				v.plainSink(ref, "appeared ("+r.GetPhase()+")")
			}
		}
		if r.GetPhase() != st.phase {
			if st.phase != "" && v.plainSink != nil {
				v.plainSink(ref, st.phase+" → "+r.GetPhase())
			}
			st.phase = r.GetPhase()
		}
		st.owner = r.GetOwner()
		st.depth = depth
		st.gone = false
		order = append(order, ref)
		for _, child := range node.GetChildren() {
			walk(child, depth+1)
		}
	}
	for _, root := range resp.Msg.GetRoots() {
		walk(root, 1)
	}
	for ref, st := range v.nodes {
		if !seen[ref] && !st.gone {
			st.gone = true
			if v.plainSink != nil {
				v.plainSink(ref, "gone")
			}
		}
	}
	v.order = order
}

// pullEvents drains the node's new history events after the cursor.
func (v *runWatchView) pullEvents(ctx context.Context, ref string) {
	v.mu.Lock()
	node := v.nodes[ref]
	after := node.afterEventId
	v.mu.Unlock()
	stream, err := v.d.Observe.Events(ctx, connect.NewRequest(&managementv1.EventsRequest{
		Ref: ref, AfterEventId: after,
	}))
	if err != nil {
		return
	}
	for stream.Receive() {
		ev := stream.Msg()
		line := renderEvent(ev)
		v.mu.Lock()
		node.afterEventId = ev.GetEventId()
		if ev.GetAttempt() > node.attempt {
			node.attempt = ev.GetAttempt()
		}
		if line != "" {
			node.events = append(node.events, line)
			if len(node.events) > eventsKept {
				node.events = node.events[len(node.events)-eventsKept:]
			}
			if v.plainSink != nil {
				v.plainSink(ref, line)
			}
		}
		v.mu.Unlock()
	}
}

// renderEvent compresses one history event to a feed line; internal
// noise renders empty and is skipped.
func renderEvent(ev *managementv1.Event) string {
	kind := ev.GetKind()
	if strings.HasPrefix(kind, "internal-") {
		return ""
	}
	line := "⚡ " + kind
	if ev.GetSubject() != "" {
		line += "  " + ev.GetSubject()
	}
	if ev.GetAgent() != "" {
		line += "  @" + ev.GetAgent()
	}
	if ev.GetAttempt() > 1 {
		line += fmt.Sprintf("  (attempt %d)", ev.GetAttempt())
	}
	if ev.GetError() != "" {
		line = "✗ " + strings.TrimPrefix(line, "⚡ ") + " — " + firstLine(ev.GetError())
	}
	return line
}

// pullLogs drains the node's new log records since the cursor.
func (v *runWatchView) pullLogs(ctx context.Context, ref string) {
	v.mu.Lock()
	node := v.nodes[ref]
	since := node.logsSince
	v.mu.Unlock()
	stream, err := v.d.Observe.Logs(ctx, connect.NewRequest(&managementv1.LogsRequest{
		Ref: ref, SinceUnixNano: since,
	}))
	if err != nil {
		return
	}
	kept := logTailKept
	if v.opts.logs == logsAll {
		kept = logAllKept
	}
	for stream.Receive() {
		rec := stream.Msg().GetRecord()
		if rec == nil {
			continue
		}
		v.mu.Lock()
		if rec.GetTimeUnixNano() > node.logsSince {
			node.logsSince = rec.GetTimeUnixNano()
		}
		line := "· " + firstLine(rec.GetBody())
		node.logs = append(node.logs, line)
		if len(node.logs) > kept {
			node.logs = node.logs[len(node.logs)-kept:]
		}
		if v.plainSink != nil {
			v.plainSink(ref, line)
		}
		v.mu.Unlock()
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return s
}

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
		fmt.Fprintf(cmdutil.Out, "\x1b[%dA\x1b[J", p.lastLines)
	}
	for _, line := range lines {
		fmt.Fprintln(cmdutil.Out, clip(line, width))
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
