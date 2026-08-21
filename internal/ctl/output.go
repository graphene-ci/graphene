package ctl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/itchyny/gojq"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// printJSON renders one proto message as JSON on stdout.
func printJSON(m proto.Message) error {
	raw, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(raw))
	return err
}

// emit renders one message per the shared output flags: a --jq
// expression over the JSON form, -o json, or -o yaml — and reports
// whether it handled the message (false — the caller renders its
// table; -o name and -o wide are the caller's table variants).
func (c *common) emit(m proto.Message) (bool, error) {
	switch {
	case *c.jq != "":
		return true, printJQ(*c.jq, m)
	case *c.output == "json":
		return true, printJSON(m)
	case *c.output == "yaml":
		return true, printYAML(m)
	}
	return false, nil
}

// printYAML renders one proto message as YAML (through its JSON form,
// so the field names match -o json).
func printYAML(m proto.Message) error {
	raw, err := protojson.Marshal(m)
	if err != nil {
		return err
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	enc, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	fmt.Fprint(out, string(enc))
	return nil
}

// printJQ pipes the message's JSON form through a jq expression.
// Strings print raw (shell-friendly), everything else as JSON — jq's
// own -r behavior.
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
			fmt.Fprintln(out, s)
			continue
		}
		enc, err := json.Marshal(item)
		if err != nil {
			return err
		}
		fmt.Fprintln(out, string(enc))
	}
}

// table renders rows with aligned columns on stdout.
func table(header []string, rows [][]string) {
	w := tabwriter.NewWriter(out, 2, 0, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(header, "\t"))
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush()
}

// watchRow is one list entry under watch: the table cells and the
// message for -o json / --jq.
type watchRow struct {
	cols []string
	msg  proto.Message
}

// watchList polls fetch and prints CHANGES, kubectl-style: the first
// snapshot in full, then only rows that appeared, changed, or went
// away (marked deleted). Runs until the context ends.
func watchList(ctx context.Context, co *common, header []string, fetch func() (map[string]watchRow, error)) error {
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
		fmt.Fprintln(out, strings.TrimRight(strings.Join(parts, "  "), " "))
	}
	emitOrLine := func(row watchRow, extra string) error {
		if *co.jq != "" || *co.output == "json" {
			_, err := co.emit(row.msg)
			return err
		}
		cols := row.cols
		if extra != "" {
			cols = append(append([]string{}, cols...), extra)
		}
		line(cols)
		return nil
	}

	prev := map[string]watchRow{}
	first := true
	for {
		cur, err := fetch()
		if err != nil {
			return err
		}
		if first {
			// Size the columns on the first frame; print the header for
			// the table forms only.
			for _, row := range cur {
				for i, c := range row.cols {
					if i < len(widths) && len(c) > widths[i] {
						widths[i] = len(c)
					}
				}
			}
			if *co.jq == "" && *co.output != "json" && *co.output != "yaml" && len(header) > 1 {
				line(header)
			}
			first = false
		}
		for key, row := range cur {
			old, seen := prev[key]
			if !seen || strings.Join(old.cols, "\x00") != strings.Join(row.cols, "\x00") {
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

// labelsCell renders labels compactly for a table cell.
func labelsCell(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

// stamp renders a nanosecond timestamp for humans.
func stamp(unixNano int64) string {
	if unixNano == 0 {
		return ""
	}
	return time.Unix(0, unixNano).Local().Format("15:04:05.000")
}
