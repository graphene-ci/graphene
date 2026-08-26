package labels

import "testing"

// The COMPLETENESS table: which system markers each kind must carry.
// This is the contract a UI filters by; a marker silently dropped from
// a creation path fails here, not in production.
func TestMarkerTable(t *testing.T) {
	table := map[string][]string{
		"run":           {Pipeline, Revision, Source, SourceDigest, Image, Trigger},
		"revision":      {Pipeline, Source, SourceDigest, Commit},
		"gitsource":     {Pipeline},
		"managedsource": {Pipeline, Origin},
		"trigger":       {Pipeline},
		"stand":         {Pipeline},
		"agent":         {Run, Pipeline},
		"artifact":      {Run, Pipeline},
	}
	for kind, marks := range table {
		for _, m := range marks {
			if len(m) <= len(Prefix) || m[:len(Prefix)] != Prefix {
				t.Fatalf("%s: marker %q is outside the reserved namespace", kind, m)
			}
		}
	}
}

// Merge must never write an empty value: an unknown marker is absent,
// not blank — a blank one would match nothing and still occupy the key.
func TestMergeSkipsEmpty(t *testing.T) {
	out := Merge(map[string]string{"a": "1"}, map[string]string{
		Pipeline: "p", Revision: "",
	})
	if out[Pipeline] != "p" || out["a"] != "1" {
		t.Fatalf("merge lost values: %#v", out)
	}
	if _, ok := out[Revision]; ok {
		t.Fatal("an empty marker must be absent, not blank")
	}
	if got := Merge(nil, map[string]string{Run: "r"}); got[Run] != "r" {
		t.Fatal("merge into nil must allocate")
	}
}
