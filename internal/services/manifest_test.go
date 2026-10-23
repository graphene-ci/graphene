package services

import (
	"reflect"
	"strings"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/pipeline/pkg/manifest"
	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// A pipeline built against a newer schemapb publishes field defaults; the
// door must read its manifest, not refuse it as an unknown field.
func TestManifestCarriesFieldDefaults(t *testing.T) {
	type db struct {
		Host string `json:"host" default:"localhost"`
	}
	type params struct {
		DB    db  `json:"db"`
		Users int `json:"users" default:"10"`
	}
	schema, err := manifest.SchemaOf(reflect.TypeFor[params](),
		schemapb.ID("graphene", schemapb.SchemaName("t-defaults"), schemapb.Ver(0, 1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protojson.Marshal(&manifestpb.Manifest{PipelineId: "p", ParamsSchema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"default"`) {
		t.Fatalf("schema carries no default: %s", raw)
	}
	var m manifestpb.Manifest
	if err := protojson.Unmarshal(raw, &m); err != nil {
		t.Fatalf("manifest with defaults refused: %v", err)
	}
}
