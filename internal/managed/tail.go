// The orchestrator tailer: stdout/stderr of managed run containers
// become OTLP log records through the server's own collector path —
// the raw "inside" of the run worker, correlated by run. Best effort
// by design: telemetry never disturbs the container, lost lines lose
// diagnostics, not work.
package managed

import (
	"bufio"
	"context"
	"io"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/gopherex/xlog"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/graphene-ci/pipeline/pkg/id"
)

// LogSink receives the tailed batches; namespace-stamping and backend
// delivery are the sink's business (the server's OTLP collector).
type LogSink func(ctx context.Context, namespace string, req *collogspb.ExportLogsServiceRequest)

const (
	tailFlushEvery = time.Second
	tailBatchCap   = 256
)

// startTail follows one run container's output until the container or
// the runner goes away. Idempotent per container id.
func (r *Runner) startTail(ctx context.Context, containerId string, runId id.RunId) {
	if r.sink == nil {
		return
	}
	r.mu.Lock()
	if _, running := r.tails[containerId]; running {
		r.mu.Unlock()
		return
	}
	tctx, cancel := context.WithCancel(ctx)
	r.tails[containerId] = cancel
	r.mu.Unlock()
	go func() {
		defer cancel()
		defer func() {
			r.mu.Lock()
			delete(r.tails, containerId)
			r.mu.Unlock()
		}()
		r.followLogs(tctx, containerId, runId)
	}()
}

func (r *Runner) stopTail(containerId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cancel, ok := r.tails[containerId]; ok {
		cancel()
		delete(r.tails, containerId)
	}
}

// followLogs streams the container's demultiplexed output line by line
// into the sink. Follows from "now": a reattach after a server restart
// picks up the present, not the past.
func (r *Runner) followLogs(ctx context.Context, containerId string, runId id.RunId) {
	stream, err := r.docker.ContainerLogs(ctx, containerId, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Since:      time.Now().Format(time.RFC3339),
	})
	if err != nil {
		r.log.Debug("run container logs unavailable", xlog.Any("run", runId), xlog.Err(err))
		return
	}
	defer func() { _ = stream.Close() }()

	lines := make(chan tailLine, tailBatchCap)
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	go scanLines(outR, "stdout", lines)
	go scanLines(errR, "stderr", lines)
	go func() {
		// The docker stream is multiplexed (no TTY) — demux splits it.
		_, _ = stdcopy.StdCopy(outW, errW, stream)
		_ = outW.Close()
		_ = errW.Close()
		close(lines)
	}()

	var pending []tailLine
	flush := func() {
		if len(pending) == 0 {
			return
		}
		r.sink(ctx, r.namespace, exportRequest(runId, pending))
		pending = pending[:0]
	}
	defer flush()
	ticker := time.NewTicker(tailFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			pending = append(pending, line)
			if len(pending) >= tailBatchCap {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

type tailLine struct {
	stream string
	text   string
}

func scanLines(r io.Reader, stream string, out chan<- tailLine) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		if text := sc.Text(); text != "" {
			out <- tailLine{stream: stream, text: text}
		}
	}
}

func exportRequest(runId id.RunId, lines []tailLine) *collogspb.ExportLogsServiceRequest {
	now := uint64(time.Now().UnixNano())
	records := make([]*logspb.LogRecord, 0, len(lines))
	for _, line := range lines {
		records = append(records, &logspb.LogRecord{
			TimeUnixNano: now,
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: line.text}},
			Attributes: []*commonpb.KeyValue{
				strAttr("stream", line.stream),
				strAttr("graphene.run", string(runId)),
			},
		})
	}
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "graphene-run"),
			strAttr("graphene.run", string(runId)),
			strAttr("graphene.role", "run"),
		}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
	}}}
}

func strAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}
