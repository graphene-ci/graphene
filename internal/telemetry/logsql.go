package telemetry

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LogsQL reads logs from VictoriaLogs — the one non-standard query
// surface (logs have no de-facto standard); everything vendor-shaped
// stays inside this file.
type LogsQL struct {
	// Base is the VictoriaLogs base URL (http://victorialogs:9428).
	Base   string
	Client *http.Client
}

// Query returns records matching the selector since the given time,
// oldest first.
func (l *LogsQL) Query(ctx context.Context, sel Selector, since time.Time, limit int) ([]LogRecord, error) {
	match := fmt.Sprintf("%q:=%q", sel.Attribute, sel.Value)
	if sel.AltAttribute != "" {
		match = fmt.Sprintf("(%s OR %q:=%q)", match, sel.AltAttribute, sel.AltValue)
	}
	query := fmt.Sprintf("%q:=%q AND %s", "graphene.namespace", sel.Namespace, match)
	if !since.IsZero() {
		query += " AND _time:>" + since.UTC().Format(time.RFC3339Nano)
	}
	form := url.Values{
		"query": {query},
		"limit": {strconv.Itoa(limit)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(l.Base, "/")+"/select/logsql/query",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := l.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body := make([]byte, 512)
		n, _ := resp.Body.Read(body)
		return nil, fmt.Errorf("logs backend: %s: %s", resp.Status, body[:n])
	}
	return parseLogsQLStream(resp.Body)
}

// parseLogsQLStream reads the backend's JSONL response.
func parseLogsQLStream(body io.Reader) ([]LogRecord, error) {
	var out []LogRecord
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		var fields map[string]string
		if json.Unmarshal(scanner.Bytes(), &fields) != nil {
			continue
		}
		rec := LogRecord{Attributes: map[string]string{}}
		for k, v := range fields {
			switch k {
			case "_time":
				rec.Time, _ = time.Parse(time.RFC3339Nano, v)
			case "_msg":
				rec.Body = v
			case "severity", "severity_text":
				rec.Severity = v
			case "_stream", "_stream_id":
				// stream identity is derivable from the attributes
			default:
				rec.Attributes[k] = v
			}
		}
		if rec.Severity == "" {
			rec.Severity = severityFromNumber(fields["severity_number"])
		}
		out = append(out, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, nil
}

// severityFromNumber maps OTel severity numbers to names.
func severityFromNumber(n string) string {
	v, err := strconv.Atoi(n)
	if err != nil {
		return ""
	}
	switch {
	case v >= 21:
		return "FATAL"
	case v >= 17:
		return "ERROR"
	case v >= 13:
		return "WARN"
	case v >= 9:
		return "INFO"
	case v >= 5:
		return "DEBUG"
	case v >= 1:
		return "TRACE"
	default:
		return ""
	}
}
