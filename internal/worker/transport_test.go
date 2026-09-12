package worker

import (
	"github.com/graphene-ci/pipeline/pkg/wire"
	"strconv"
	"testing"
)

func TestMachineExecutorTransport(t *testing.T) {
	for _, tls := range []bool{false, true} {
		w := &Worker{deps: Deps{External: "public:443", ExternalTLS: tls, RunToken: "test-token"}}
		spec, err := w.containerSpec("agent", "run", "image")
		if err != nil {
			t.Fatal(err)
		}
		if spec.Env[wire.EnvInsecure] != strconv.FormatBool(!tls) || spec.Env[wire.EnvAddress] != "public:443" {
			t.Fatalf("executor transport: %v", spec.Env)
		}
	}
}

func TestMachineExecutorMintsRunToken(t *testing.T) {
	w := &Worker{deps: Deps{Namespace: "tenant", RunToken: "legacy", MintRunToken: func(ns, run string) string {
		if ns != "tenant" || run != "run1" {
			t.Fatalf("wrong token scope: %s %s", ns, run)
		}
		return "minted"
	}}}
	spec, err := w.containerSpec("agent1", "run1", "image")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Env[wire.EnvToken] != "minted" {
		t.Fatal("machine executor did not get its minted identity")
	}
}

func TestMachineExecutorRejectsMissingIdentity(t *testing.T) {
	for _, mint := range []func(string, string) string{nil, func(string, string) string { return "" }} {
		w := &Worker{deps: Deps{MintRunToken: mint}}
		spec, err := w.containerSpec("agent", "run", "image")
		if err == nil || spec != nil {
			t.Fatal("executor without credentials was accepted")
		}
	}
}
