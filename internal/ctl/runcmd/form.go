package runcmd

// A run's params come from the pipeline's manifest, so the schema is
// pulled out here; asking for it is the shared form's business.

import (
	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// paramsSchemaOf pulls the params schema out of a pipeline record's
// manifest; nil when there is none to ask about.
func paramsSchemaOf(manifestJSON []byte) *schemapb.Schema {
	if len(manifestJSON) == 0 {
		return nil
	}
	var m manifestpb.Manifest
	if protojson.Unmarshal(manifestJSON, &m) != nil {
		return nil
	}
	s := m.GetParamsSchema()
	if s == nil || len(s.GetFields()) == 0 {
		return nil
	}
	return s
}
