package worker

import (
	"github.com/graphene-ci/pipeline/pkg/wire"
	"strconv"
	"testing"
)

func TestMachineExecutorTransport(t *testing.T) {
	for _, tls := range []bool{false, true} {
		w := &Worker{deps: Deps{External: "public:443", ExternalTLS: tls}}
		spec := w.containerSpec("agent", "run", "image")
		if spec.Env[wire.EnvInsecure] != strconv.FormatBool(!tls) || spec.Env[wire.EnvAddress] != "public:443" {
			t.Fatalf("executor transport: %v", spec.Env)
		}
	}
}
