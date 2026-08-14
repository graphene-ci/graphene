// Command vm is the pipeline that proves the cloud.
//
// It asks for a real virtual machine, hands the machine its own install
// script through the provider's own field, and runs a command on it. The
// agent is never waited for: the step goes into the installation's queue
// and sits there until the machine boots and comes to read it.
//
// Notice what is NOT here: no branch on which cloud this is, no template,
// no `if`. The instance is a Go value of the provider's own generated type,
// and user-data is the provider's own field — we neither know nor care what
// it means.
package main

import (
	"fmt"
	"os"

	computev1alpha1 "github.com/yandex-cloud/crossplane-provider-yc/apis/namespaced/compute/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/pipeline"
)

// Params is what this pipeline takes.
type Params struct {
	// Where to build. These come from the run rather than from the code
	// because they belong to whoever runs it, not to the pipeline.
	Zone    string `json:"zone"`
	Subnet  string `json:"subnet"`
	Image   string `json:"image"`
	Cores   int    `json:"cores"`
	MemryGB int    `json:"memoryGB"`
	// Say is echoed on the machine, so the check recognizes its own
	// output rather than any output.
	Say string `json:"say"`
}

func main() {
	// Обе схемы: наша — ради своих видов, провайдера — ради Instance.
	// Другого способа узнать вид сгенерированного типа в Go нет, и отказ
	// без этой строки называет именно её.
	err := pipeline.Serve(VM,
		pipeline.Scheme(v1.AddToScheme, computev1alpha1.AddToScheme))
	if err != nil {
		fmt.Fprintln(os.Stderr, "vm:", err)
		os.Exit(1)
	}
}

// VM builds a machine in the cloud and runs one command on it.
func VM(run pipeline.Run, params Params) error {
	node := pipeline.Install(run, "node-0")

	pipeline.Apply(run, "node-0", instance(node, params))

	// Готовности ВМ не ждём и агента не ждём: очередь и есть ожидание, а
	// подключившийся агент и есть доказательство, что машина работает.
	said := pipeline.On(run, node).Command("say", agent.ExecInput{
		Argv: []string{"echo", params.Say},
	})

	if said.Code != 0 {
		return fmt.Errorf("команда вернула %d: %s", said.Code, said.Stderr)
	}

	facts := pipeline.On(run, node).Facts()
	if facts["os"] != "linux" {
		return fmt.Errorf("машина сказала о себе не то: %v", facts)
	}

	return nil
}

// instance is the virtual machine as the provider wants it described.
func instance(node pipeline.Target, params Params) *computev1alpha1.Instance {
	return &computev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: computev1alpha1.InstanceSpec{
			ForProvider: computev1alpha1.InstanceParameters{
				Zone:       &params.Zone,
				PlatformID: text("standard-v3"),
				Resources: []computev1alpha1.ResourcesParameters{{
					Cores:  number(params.Cores),
					Memory: number(params.MemryGB),
				}},
				BootDisk: []computev1alpha1.BootDiskParameters{{
					InitializeParams: []computev1alpha1.InitializeParamsParameters{{
						ImageID: &params.Image,
						Size:    number(20),
					}},
				}},
				NetworkInterface: []computev1alpha1.NetworkInterfaceParameters{{
					SubnetID: &params.Subnet,
					// Внешний адрес нужен не для входящего доступа — его
					// нет и не предполагается, — а чтобы агент дотянулся
					// до нас сам.
					NAT: yes(),
				}},
				Metadata: map[string]*string{
					// Поле ПРОВАЙДЕРА, не наше. Что в нём лежит, знает
					// cloud-init машины; мы только кладём туда то, что
					// породил наш же SDK.
					"user-data": text(node.CloudInit()),
				},
			},
		},
	}
}

func text(value string) *string { return &value }

func number(value int) *float64 {
	as := float64(value)

	return &as
}

func yes() *bool {
	value := true

	return &value
}
