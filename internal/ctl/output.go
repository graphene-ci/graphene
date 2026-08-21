package ctl

import (
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/itchyny/gojq"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
// expression over the JSON form, or -o json — and reports whether it
// handled the message (false — the caller renders its table).
func (c *common) emit(m proto.Message) (bool, error) {
	if *c.jq != "" {
		return true, printJQ(*c.jq, m)
	}
	if *c.output == "json" {
		return true, printJSON(m)
	}
	return false, nil
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
