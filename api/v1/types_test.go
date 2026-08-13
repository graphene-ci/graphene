package v1_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	v1 "github.com/graphene-ci/graphene/api/v1"
)

func TestAddToSchemeRegistersEveryKind(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("схема не собралась: %v", err)
	}

	for _, kind := range []string{"Pipeline", "PipelineRevision", "Run", "Probe"} {
		for _, name := range []string{kind, kind + "List"} {
			if !scheme.Recognizes(v1.GroupVersion.WithKind(name)) {
				t.Errorf("вид %s не зарегистрирован", name)
			}
		}
	}
}

func TestRunSpecRejectsOversizedParams(t *testing.T) {
	t.Parallel()

	spec := v1.RunSpec{
		RevisionRef: v1.LocalRef{Name: "perf-7f3a91c"},
		Params:      &apiextensionsv1.JSON{Raw: bytes.Repeat([]byte("x"), v1.MaxParamsBytes+1)},
	}

	if err := spec.Validate(); !errors.Is(err, v1.ErrParamsTooBig) {
		t.Fatalf("ожидали ErrParamsTooBig, получили %v", err)
	}
}

func TestRunSpecAcceptsParamsAtTheLimit(t *testing.T) {
	t.Parallel()

	spec := v1.RunSpec{
		RevisionRef: v1.LocalRef{Name: "perf-7f3a91c"},
		Params:      &apiextensionsv1.JSON{Raw: bytes.Repeat([]byte("x"), v1.MaxParamsBytes)},
	}

	if err := spec.Validate(); err != nil {
		t.Fatalf("предел должен быть достижим, а не запрещён: %v", err)
	}
}

func TestRunSpecRequiresRevision(t *testing.T) {
	t.Parallel()

	if err := (v1.RunSpec{}).Validate(); !errors.Is(err, v1.ErrNoRevision) {
		t.Fatalf("прогон без ревизии обязан быть отвергнут, получили %v", err)
	}
}

func TestRunStatusRejectsOversizedResult(t *testing.T) {
	t.Parallel()

	status := v1.RunStatus{
		Result: &apiextensionsv1.JSON{Raw: bytes.Repeat([]byte("x"), v1.MaxResultBytes+1)},
	}

	if err := status.Validate(); !errors.Is(err, v1.ErrResultTooBig) {
		t.Fatalf("ожидали ErrResultTooBig, получили %v", err)
	}
}

// Параметры доезжают до воркфлоу через JSON и обратно. Если бы они
// сериализовались как строка или теряли форму, прогон получил бы не то,
// что человек написал, — и узнал бы об этом на середине.
func TestRunSurvivesJSONRoundTrip(t *testing.T) {
	t.Parallel()

	before := v1.Run{
		ObjectMeta: metav1.ObjectMeta{Name: "perf-42", Namespace: "default"},
		Spec: v1.RunSpec{
			RevisionRef: v1.LocalRef{Name: "perf-7f3a91c"},
			Params:      &apiextensionsv1.JSON{Raw: []byte(`{"nodes":3,"version":"16"}`)},
		},
	}

	encoded, err := json.Marshal(before)
	if err != nil {
		t.Fatalf("не закодировалось: %v", err)
	}

	var after v1.Run
	if err := json.Unmarshal(encoded, &after); err != nil {
		t.Fatalf("не раскодировалось: %v", err)
	}

	if got := string(after.Spec.Params.Raw); got != `{"nodes":3,"version":"16"}` {
		t.Fatalf("параметры поехали: %s", got)
	}

	if after.Spec.RevisionRef.Name != before.Spec.RevisionRef.Name {
		t.Fatalf("ссылка на ревизию поехала: %q", after.Spec.RevisionRef.Name)
	}
}

// Ревизия держит дайджест, а не тег: тег переставляют, и «повторить
// прогон» перестало бы значить «выполнить тот же код».
func TestPipelineRevisionSpecInsistsOnDigest(t *testing.T) {
	t.Parallel()

	const sum = "sha256:0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85"

	cases := map[string]struct {
		image string
		valid bool
	}{
		"дайджест":        {"registry.example.com/perf@" + sum, true},
		"тег и дайджест":  {"registry.example.com/perf:v1@" + sum, true},
		"тег":             {"registry.example.com/perf:v1", false},
		"без ничего":      {"registry.example.com/perf", false},
		"короткая сумма":  {"registry.example.com/perf@sha256:0e3b0c44", false},
		"другой алгоритм": {"registry.example.com/perf@md5:0e3b0c44", false},
		"пусто":           {"", false},
	}

	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spec := v1.PipelineRevisionSpec{
				PipelineRef: v1.LocalRef{Name: "perf"},
				Image:       want.image,
				Queue:       "perf",
			}

			err := spec.Validate()
			if want.valid && err != nil {
				t.Fatalf("%q должен приниматься: %v", want.image, err)
			}

			if !want.valid && !errors.Is(err, v1.ErrNotDigest) {
				t.Fatalf("%q должен отвергаться как не дайджест, получили %v", want.image, err)
			}
		})
	}
}
