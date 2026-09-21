package cmdutil

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/itchyny/gojq"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	yamlpkg "sigs.k8s.io/yaml"
)

// Out is where data goes; progress goes to stderr.
var Out = os.Stdout

// Emit renders one message per the shared output flags: a --jq
// expression over the JSON form, -o json, or -o yaml — and reports
// whether it handled the message (false — the caller renders its
// table; -o name and -o wide are the caller's table variants).
//
// Embedded-JSON bytes fields (a record's spec and state, a manifest,
// an event's payloads) decode into real objects on the way — protojson
// would render them as base64, which nobody can read or query.
func (f *Factory) Emit(m proto.Message) (bool, error) {
	switch {
	case f.JQ != "":
		return true, printJQ(f.JQ, m)
	case f.Output == "json":
		return true, printJSON(m)
	case f.Output == "yaml":
		return true, printYAML(m)
	}
	return false, nil
}

// jsonForm is the message's protojson form with the embedded-JSON
// bytes fields decoded.
func jsonForm(m proto.Message) (any, error) {
	raw, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return decodeEmbedded(v), nil
}

// embeddedJSONFields name the bytes fields that CARRY JSON by
// contract: a record's spec/state, a manifest, an event's payloads.
var embeddedJSONFields = map[string]bool{
	"spec": true, "state": true, "manifest": true,
	"params": true, "result": true, "payload": true,
	"input": true, "raw": true,
}

// decodeEmbedded walks the JSON form and turns base64 strings under
// the known field names into the objects they carry; anything that
// does not decode stays as it was.
func decodeEmbedded(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, mv := range t {
			if s, ok := mv.(string); ok && embeddedJSONFields[k] && s != "" {
				if raw, err := base64.StdEncoding.DecodeString(s); err == nil && len(raw) > 0 {
					var decoded any
					if json.Unmarshal(raw, &decoded) == nil && decoded != nil {
						t[k] = decodeEmbedded(decoded)
						continue
					}
				}
			}
			t[k] = decodeEmbedded(mv)
		}
	case []any:
		for i, item := range t {
			t[i] = decodeEmbedded(item)
		}
	}
	return v
}

func printJSON(m proto.Message) error {
	v, err := jsonForm(m)
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(Out, string(raw))
	return err
}

// printYAML renders the same decoded form through the in-house
// protoyaml mapping (JSON to the k8s YAML shape).
func printYAML(m proto.Message) error {
	v, err := jsonForm(m)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	rendered, err := yamlpkg.JSONToYAML(raw)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(Out, string(rendered))
	return err
}

// printJQ pipes the message's decoded JSON form through a jq
// expression. Strings print raw (jq's own -r behavior).
func printJQ(expr string, m proto.Message) error {
	query, err := gojq.Parse(expr)
	if err != nil {
		return fmt.Errorf("--jq: %w", err)
	}
	v, err := jsonForm(m)
	if err != nil {
		return err
	}
	return runJQ(query, v)
}

// JQBytes runs a jq expression over raw JSON bytes — the observe
// passthroughs (PromQL, Jaeger) that never were proto messages.
func JQBytes(expr string, raw []byte) error {
	query, err := gojq.Parse(expr)
	if err != nil {
		return fmt.Errorf("--jq: %w", err)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	return runJQ(query, v)
}

func runJQ(query *gojq.Query, v any) error {
	iter := query.Run(v)
	for {
		item, ok := iter.Next()
		if !ok {
			return nil
		}
		if err, isErr := item.(error); isErr {
			return fmt.Errorf("--jq: %w", err)
		}
		if s, isStr := item.(string); isStr {
			if _, err := fmt.Fprintln(Out, s); err != nil {
				return err
			}
			continue
		}
		enc, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(Out, string(enc)); err != nil {
			return err
		}
	}
}

// Table renders rows with aligned columns on stdout.
func Table(header []string, rows [][]string) error {
	w := tabwriter.NewWriter(Out, 2, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, strings.Join(header, "\t")); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintln(w, strings.Join(row, "\t")); err != nil {
			return err
		}
	}
	return w.Flush()
}

// PrintJSONBlock renders a raw-JSON field as an indented YAML block
// under its title — the readable form of a record's spec and state.
func PrintJSONBlock(title string, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	rendered, err := yamlpkg.JSONToYAML(raw)
	if err != nil {
		_, writeErr := fmt.Fprintf(Out, "%s: %s\n", title, string(raw))
		return writeErr
	}
	if _, err := fmt.Fprintf(Out, "%s:\n", title); err != nil {
		return err
	}
	for line := range strings.SplitSeq(strings.TrimRight(string(rendered), "\n"), "\n") {
		if _, err := fmt.Fprintf(Out, "  %s\n", line); err != nil {
			return err
		}
	}
	return nil
}

// systemLabelPrefix marks the labels the installation stamps itself (the
// pipeline, the run, the image, the trigger): bookkeeping a table already
// shows in its own columns, or does not need at all.
const systemLabelPrefix = "graphene.io/"

// LabelsCell renders labels compactly for a table cell, in key order — a
// map's own order differs from call to call, and a watch would read that
// as a change.
func LabelsCell(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// UserLabels drops the installation's own labels: the default table shows
// what a person put there; -o wide (and json/yaml) keep everything.
func UserLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if !strings.HasPrefix(k, systemLabelPrefix) {
			out[k] = v
		}
	}
	return out
}

// sortedKeys is the stable walk of a watch snapshot.
func sortedKeys(rows map[string]WatchRow) []string {
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Stamp renders a nanosecond timestamp for humans.
func Stamp(unixNano int64) string {
	if unixNano == 0 {
		return ""
	}
	return time.Unix(0, unixNano).Local().Format("15:04:05.000")
}

// Age renders how long ago a moment was, in kubectl's coarse units: the
// two leading ones, never a wall of digits.
func Age(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return coarse(time.Since(ts.AsTime()))
}

// Took renders a span; an open end means "so far".
func Took(start, end *timestamppb.Timestamp) string {
	if start == nil {
		return ""
	}
	if end == nil {
		return coarse(time.Since(start.AsTime())) + "+"
	}
	return coarse(end.AsTime().Sub(start.AsTime()))
}

// When renders a moment for humans, in local time.
func When(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Local().Format("2006-01-02 15:04:05")
}

func coarse(d time.Duration) string {
	d = max(d, 0)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
}

// WatchRow is one list entry under watch: the table cells and the
// message for -o json / --jq.
type WatchRow struct {
	Cols []string
	Msg  proto.Message
}

// WatchList polls fetch and prints CHANGES, kubectl-style: the first
// snapshot in full, then only rows that appeared, changed, or went
// away (marked deleted). Runs until the context ends.
func (f *Factory) WatchList(ctx context.Context, header []string, fetch func() (map[string]WatchRow, error)) error {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = len(h)
	}
	line := func(cols []string) {
		parts := make([]string, len(cols))
		for i, c := range cols {
			if i < len(widths) {
				if len(c) > widths[i] {
					widths[i] = len(c)
				}
				parts[i] = fmt.Sprintf("%-*s", widths[i], c)
			} else {
				parts[i] = c
			}
		}
		fmt.Fprintln(Out, strings.TrimRight(strings.Join(parts, "  "), " "))
	}
	emitOrLine := func(row WatchRow, extra string) error {
		if f.JQ != "" || f.Output == "json" || f.Output == "yaml" {
			_, err := f.Emit(row.Msg)
			return err
		}
		cols := row.Cols
		if extra != "" {
			cols = append(append([]string{}, cols...), extra)
		}
		line(cols)
		return nil
	}

	prev := map[string]WatchRow{}
	first := true
	for {
		cur, err := fetch()
		if err != nil {
			return err
		}
		if first {
			for _, row := range cur {
				for i, c := range row.Cols {
					if i < len(widths) && len(c) > widths[i] {
						widths[i] = len(c)
					}
				}
			}
			if f.JQ == "" && f.Output != "json" && f.Output != "yaml" && len(header) > 1 {
				line(header)
			}
			first = false
		}
		for _, key := range sortedKeys(cur) {
			row := cur[key]
			old, seen := prev[key]
			if !seen || strings.Join(old.Cols, "\x00") != strings.Join(row.Cols, "\x00") {
				if err := emitOrLine(row, ""); err != nil {
					return err
				}
			}
		}
		for _, key := range sortedKeys(prev) {
			old := prev[key]
			if _, still := cur[key]; !still {
				if err := emitOrLine(old, "deleted"); err != nil {
					return err
				}
			}
		}
		prev = cur
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}
