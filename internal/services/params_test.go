package services

import (
	"strings"
	"testing"

	"github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	managementv1 "github.com/graphene-ci/graphene/pkg/proto/management/v1"
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
// The kind comes from the typed selector, or from the query parsed by
// the SAME parser the listing itself uses.
func TestListedKind(t *testing.T) {
	// The typed selector is trusted — the server compiles it itself.
	if got := string(listedKind(&managementv1.Selector{Kind: "pipeline"}, "")); got != "pipeline" {
		t.Fatalf("selector kind ignored: %q", got)
	}
	if got := string(listedKind(&managementv1.Selector{Kind: "docker"}, "")); got != "resource" {
		t.Fatalf("a user's own kind must be the resource group: %q", got)
	}
	// A query pins the kind only by equality on exactly one term.
	for q, want := range map[string]string{
		"kind=pipeline":              "pipeline",
		"kind=revision, phase=ready": "revision",
		"kind=docker":                "resource",
		"phase=ready":                "*",
		"kind in (pipeline, secret)": "*",
		"kind!=secret":               "*",
		"kind=^pipe":                 "*",
		"":                           "*",
		"nonsense((":                 "*",
	} {
		if got := string(listedKind(nil, q)); got != want {
			t.Fatalf("listedKind(%q) = %q, want %q", q, got, want)
		}
	}
}
