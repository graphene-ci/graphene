package runcmd

import (
	"encoding/json"
	"github.com/graphene-ci/graphene/internal/ctl/cmdutil"
	"strings"
	"testing"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
)

func formSchema(t *testing.T) *schemapb.Schema {
	t.Helper()
	s, err := schemapb.NewSchema(
		schemapb.ID("t", schemapb.SchemaName("f"), schemapb.Ver(0, 1, 0))).Coerce().Fields(
		schemapb.Str("name").Required(),
		schemapb.Duration("keep"),
		schemapb.JSON("extra"),
	).Build()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPromptParams(t *testing.T) {
	// The empty first answer must re-ask the required field; the empty
	// answer on the optional one skips it; JSON compounds parse.
	in := strings.NewReader("\nsrv-1\n1h\n{\"a\":1}\n")
	raw, err := cmdutil.PromptSchema(in, "params", formSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["name"] != "srv-1" || got["keep"] != "1h" {
		t.Fatalf("answers lost: %v", got)
	}
	if extra, ok := got["extra"].(map[string]any); !ok || extra["a"] != float64(1) {
		t.Fatalf("JSON answer not parsed: %v", got["extra"])
	}
}

func TestPromptParamsSkipsOptional(t *testing.T) {
	in := strings.NewReader("srv-1\n\n\n")
	raw, err := cmdutil.PromptSchema(in, "params", formSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if _, there := got["keep"]; there {
		t.Fatalf("optional empty answer must skip the field: %v", got)
	}
}
