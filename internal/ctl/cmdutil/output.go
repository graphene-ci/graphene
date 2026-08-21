package cmdutil

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/itchyny/gojq"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	yamlpkg "sigs.k8s.io/yaml"

	"github.com/graphene-ci/graphene/internal/protoyaml"
)

// Out is where data goes; progress goes to stderr.
var Out = os.Stdout

// Emit renders one message per the shared output flags: a --jq
// expression over the JSON form, -o json, or -o yaml — and reports
// whether it handled the message (false — the caller renders its
// table; -o name and -o wide are the caller's table variants).
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

func printJSON(m proto.Message) error {
	raw, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(Out, string(raw))
	return err
}

// printYAML renders one proto message with the in-house protoyaml —
// protojson names through the k8s YAML mapping.
func printYAML(m proto.Message) error {
	raw, err := protoyaml.Marshal(m)
	if err != nil {
		return err
	}
	fmt.Fprint(Out, string(raw))
	return nil
}

// printJQ pipes the message's JSON form through a jq expression.
// Strings print raw (jq's own -r behavior).
func printJQ(expr string, m proto.Message) error {
	query, err := gojq.Parse(expr)
	if err != nil {
		return fmt.Errorf("--jq: %w", err)
	}
	raw, err := protojson.Marshal(m)
	if err != nil {
		return err
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
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
			fmt.Fprintln(Out, s)
			continue
		}
		enc, err := json.Marshal(item)
		if err != nil {
			return err
		}
		fmt.Fprintln(Out, string(enc))
	}
}

// Table renders rows with aligned columns on stdout.
func Table(header []string, rows [][]string) {
	w := tabwriter.NewWriter(Out, 2, 0, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(header, "\t"))
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush()
}

// PrintJSONBlock renders a raw-JSON field as an indented YAML block
// under its title — the readable form of a record's spec and state.
func PrintJSONBlock(title string, raw []byte) {
	if len(raw) == 0 {
		return
	}
	rendered, err := yamlpkg.JSONToYAML(raw)
	if err != nil {
		fmt.Fprintf(Out, "%s: %s\n", title, string(raw))
		return
	}
	fmt.Fprintf(Out, "%s:\n", title)
	for line := range strings.SplitSeq(strings.TrimRight(string(rendered), "\n"), "\n") {
		fmt.Fprintf(Out, "  %s\n", line)
	}
}

// LabelsCell renders labels compactly for a table cell.
func LabelsCell(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

// Stamp renders a nanosecond timestamp for humans.
func Stamp(unixNano int64) string {
	if unixNano == 0 {
		return ""
	}
	return time.Unix(0, unixNano).Local().Format("15:04:05.000")
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
		for key, row := range cur {
			old, seen := prev[key]
			if !seen || strings.Join(old.Cols, "\x00") != strings.Join(row.Cols, "\x00") {
				if err := emitOrLine(row, ""); err != nil {
					return err
				}
			}
		}
		for key, old := range prev {
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
