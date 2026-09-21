package ui

import (
	"fmt"
	"math"
	"strings"
	"time"
)

var sparkTicks = []rune("▁▂▃▄▅▆▇█")

// resample folds a series into exactly n buckets by averaging — a chart
// has a fixed number of columns, a series any number of points.
func resample(values []float64, n int) []float64 {
	if len(values) == 0 || n <= 0 {
		return nil
	}
	if len(values) <= n {
		return values
	}
	out := make([]float64, n)
	for i := range out {
		lo := i * len(values) / n
		hi := max((i+1)*len(values)/n, lo+1)
		sum := 0.0
		for _, v := range values[lo:hi] {
			sum += v
		}
		out[i] = sum / float64(hi-lo)
	}
	return out
}

// stretch widens a short series to fill the plot: three points drawn three
// columns wide are a smudge, the same three as steps are a chart.
func stretch(values []float64, width int) []float64 {
	if len(values) == 0 || len(values) >= width {
		return values
	}
	out := make([]float64, width)
	for i := range out {
		out[i] = values[i*len(values)/width]
	}
	return out
}

func bounds(values []float64) (lo, hi float64) {
	lo, hi = math.Inf(1), math.Inf(-1)
	for _, v := range values {
		lo, hi = math.Min(lo, v), math.Max(hi, v)
	}
	return lo, hi
}

// Spark is a one-line chart of a series, at most width columns. A flat
// series draws as a low line — "nothing moved" is an answer too.
func Spark(values []float64, width int) string {
	values = resample(values, width)
	if len(values) == 0 {
		return ""
	}
	lo, hi := bounds(values)
	var b strings.Builder
	for _, v := range values {
		idx := 0
		if hi > lo {
			idx = int((v - lo) / (hi - lo) * float64(len(sparkTicks)-1))
		}
		b.WriteRune(sparkTicks[idx])
	}
	return b.String()
}

// Chart draws a series as a block chart of the given height with a value
// axis on the left and a time axis below. Rows are returned unstyled-width
// safe: the axis is quiet, the plot colored.
func Chart(values []float64, from, to time.Time, width, height int, format func(float64) string) []string {
	if format == nil {
		format = Number
	}
	values = stretch(resample(values, width), width)
	if len(values) == 0 || height < 2 {
		return nil
	}
	lo, hi := bounds(values)
	if hi == lo {
		// A flat series still needs a scale to sit on.
		hi, lo = hi+1, math.Min(lo, 0)
	}
	labels := []string{format(hi), format((hi + lo) / 2), format(lo)}
	axis := 0
	for _, l := range labels {
		axis = max(axis, Len(l))
	}
	// Each cell is 8 sub-steps tall: a column's height in eighths decides
	// which rows are full, which one is partial, which are empty.
	levels := make([]int, len(values))
	for i, v := range values {
		levels[i] = int(math.Round((v - lo) / (hi - lo) * float64(height*8)))
	}
	rows := make([]string, 0, height+1)
	for r := height - 1; r >= 0; r-- {
		var b strings.Builder
		for _, level := range levels {
			switch fill := level - r*8; {
			case fill >= 8:
				b.WriteRune('█')
			case fill > 0:
				b.WriteRune(sparkTicks[fill-1])
			case r == 0:
				// The floor: a series at its minimum is still a series.
				b.WriteRune('▁')
			default:
				b.WriteRune(' ')
			}
		}
		label := ""
		switch r {
		case height - 1:
			label = labels[0]
		case height / 2:
			label = labels[1]
		case 0:
			label = labels[2]
		}
		rows = append(rows, Gray(PadLeft(label, axis)+" ┤")+Cyan(b.String()))
	}
	if !from.IsZero() && !to.IsZero() {
		left, right := from.Local().Format("15:04:05"), to.Local().Format("15:04:05")
		gapWidth := max(len(values)-len(left)-len(right), 1)
		rows = append(rows, Gray(strings.Repeat(" ", axis+2)+left+strings.Repeat(" ", gapWidth)+right))
	}
	return rows
}

// Number renders a value compactly: 2591.4 → "2.59k", 0.00412 → "4.12m".
// Whole small numbers stay as they are.
func Number(v float64) string {
	abs := math.Abs(v)
	switch {
	case v == 0:
		return "0"
	case abs >= 1e9:
		return trim(v/1e9) + "G"
	case abs >= 1e6:
		return trim(v/1e6) + "M"
	case abs >= 1e4:
		return trim(v/1e3) + "k"
	case abs >= 1:
		return trim(v)
	case abs >= 1e-3:
		return trim(v*1e3) + "m"
	case abs >= 1e-6:
		return trim(v*1e6) + "µ"
	}
	return fmt.Sprintf("%.2e", v)
}

// Bytes renders a size in binary units.
func Bytes(v float64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	i := 0
	for math.Abs(v) >= 1024 && i < len(units)-1 {
		v, i = v/1024, i+1
	}
	return trim(v) + units[i]
}

// trim prints three significant digits without a trailing ".0".
func trim(v float64) string {
	s := fmt.Sprintf("%.3g", v)
	if strings.Contains(s, "e") {
		s = fmt.Sprintf("%.0f", v)
	}
	return s
}

// Span draws one bar of a waterfall: where in [0,total] the span sits and
// how long it is, on a track of the given width. A span too short to see
// still gets one cell — it happened.
func Span(offset, length, total time.Duration, width int) string {
	if total <= 0 || width <= 0 {
		return ""
	}
	start := int(float64(offset) / float64(total) * float64(width))
	start = min(max(start, 0), width-1)
	cells := int(math.Round(float64(length) / float64(total) * float64(width)))
	cells = min(max(cells, 1), width-start)
	return strings.Repeat(" ", start) + strings.Repeat("━", cells) + strings.Repeat(" ", width-start-cells)
}

// Duration renders a span's length in the unit that reads best.
func Duration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	case d < time.Minute:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
}

// TreeGlyphs answers the prefix of a tree node's own line and the prefix
// its children inherit.
func TreeGlyphs(last bool) (own, inherit string) {
	if last {
		return "└─ ", "   "
	}
	return "├─ ", "│  "
}
