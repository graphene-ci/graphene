package services

import (
	"strings"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/graphene-ci/pipeline/pkg/id"
	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

type mapStore map[string]string

func (m mapStore) Get(name id.SecretId) (string, error) {
	v, ok := m[string(name)]
	if !ok {
		return "", &missingErr{string(name)}
	}
	return v, nil
}

type missingErr struct{ name string }

func (e *missingErr) Error() string { return e.name + " is not configured" }

func TestSubstituteVars(t *testing.T) {
	vars := mapStore{"folder": "b1g", "zone": "ru-central1-a"}
	in := []byte(`{"folderId":"${var:folder}","nested":{"zone":"${var:zone}"},"plain":"x"}`)
	out, err := substituteVars(in, vars)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{`"b1g"`, `"ru-central1-a"`, `"x"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("want %s in %s", want, s)
		}
	}
	if strings.Contains(s, "${var:") {
		t.Fatalf("placeholder survived: %s", s)
	}
	if _, err := substituteVars([]byte(`{"a":"${var:gone}"}`), vars); err == nil {
		t.Fatal("missing variable must fail the submit")
	}
}

func TestCheckSecretRefs(t *testing.T) {
	schema, err := schemapb.NewSchema(
		schemapb.ID("graphene", schemapb.SchemaName("t-params"), schemapb.Ver(0, 1, 0))).
		Fields(schemapb.Str("key").Secret(), schemapb.Str("plain")).Build()
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, err := protojson.Marshal(&manifestpb.Manifest{ParamsSchema: schema})
	if err != nil {
		t.Fatal(err)
	}
	store := mapStore{"ssh-key": "PEM"}
	if err := checkSecretRefs(manifestJSON, []byte(`{"key":"ssh-key","plain":"v"}`), store); err != nil {
		t.Fatalf("existing secret refused: %v", err)
	}
	if err := checkSecretRefs(manifestJSON, []byte(`{"key":"nope"}`), store); err == nil {
		t.Fatal("missing secret must fail the submit")
	}
}

// A listing narrowed to one kind must be authorized against THAT kind,
// not against everything: "list pipelines" is not "list the world".
func TestKindFromQuery(t *testing.T) {
	for query, want := range map[string]string{
		"EntityKind = 'pipeline'":                            "pipeline",
		"EntityKind='revision' AND ExecutionStatus='Running'": "revision",
		"EntityKind = 'k8s.compute.Instance'":                "resource",
		"ExecutionStatus = 'Running'":                        "*",
		"":                                                   "*",
	} {
		if got := string(kindFromQuery(query)); got != want {
			t.Fatalf("kindFromQuery(%q) = %q, want %q", query, got, want)
		}
	}
}
