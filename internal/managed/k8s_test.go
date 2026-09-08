package managed

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func k8sTestRunner(t *testing.T, template string) *k8sRunner {
	t.Helper()
	base, err := parsePodTemplate(template)
	if err != nil {
		t.Fatalf("parsePodTemplate: %v", err)
	}
	return &k8sRunner{
		namespace: "acme",
		cfg: K8sConfig{
			PodNamespace: "graphene",
			ExternalGRPC: "graphene.graphene.svc:443",
			PullSecret:   "graphene-pull",
			PodTemplate:  template,
		},
		podBase: base,
	}
}

// The template's scheduling policy survives, and the run container carries
// our wiring and pull secret.
func TestDeploymentOverlaysTemplate(t *testing.T) {
	tmpl := `
nodeSelector: {pool: runs}
tolerations:
  - {key: dedicated, value: runs, effect: NoSchedule}
containers:
  - name: run
    resources:
      requests: {cpu: 500m, memory: 512Mi}
`
	r := k8sTestRunner(t, tmpl)
	dep := r.deployment("run-1", "graphene.stroppy.io/acme/app:abc", "tok")
	spec := dep.Spec.Template.Spec

	if spec.NodeSelector["pool"] != "runs" {
		t.Errorf("nodeSelector lost: %v", spec.NodeSelector)
	}
	if len(spec.Tolerations) != 1 || spec.Tolerations[0].Key != "dedicated" {
		t.Errorf("tolerations lost: %v", spec.Tolerations)
	}
	if len(spec.Containers) != 1 || spec.Containers[0].Name != "run" {
		t.Fatalf("want one run container, got %+v", spec.Containers)
	}
	c := spec.Containers[0]
	if c.Image != "graphene.stroppy.io/acme/app:abc" {
		t.Errorf("image not set: %q", c.Image)
	}
	if c.Resources.Requests.Cpu().String() != "500m" {
		t.Errorf("resources from template lost: %v", c.Resources.Requests)
	}
	if envVal(c.Env, "GRAPHENE_RUN_ID") == "" {
		t.Errorf("wiring env missing: %v", c.Env)
	}
	if spec.RestartPolicy != corev1.RestartPolicyAlways {
		t.Errorf("restart policy must be Always, got %q", spec.RestartPolicy)
	}
	if !hasPullSecret(spec.ImagePullSecrets, "graphene-pull") {
		t.Errorf("pull secret not added: %v", spec.ImagePullSecrets)
	}
}

// No "run" container in the template: ours is prepended, sidecars kept.
func TestDeploymentPrependsRunContainer(t *testing.T) {
	tmpl := `
containers:
  - name: sidecar
    image: busybox
`
	r := k8sTestRunner(t, tmpl)
	dep := r.deployment("run-2", "img:tag", "tok")
	cs := dep.Spec.Template.Spec.Containers
	if len(cs) != 2 || cs[0].Name != "run" || cs[1].Name != "sidecar" {
		t.Fatalf("want [run, sidecar], got %+v", cs)
	}
}

// An empty template yields a bare pod with just the run container.
func TestDeploymentBarePod(t *testing.T) {
	r := k8sTestRunner(t, "")
	dep := r.deployment("run-3", "img:tag", "tok")
	cs := dep.Spec.Template.Spec.Containers
	if len(cs) != 1 || cs[0].Name != "run" {
		t.Fatalf("want single run container, got %+v", cs)
	}
}

// Our wiring env wins over a same-named entry in the template.
func TestMergeEnvOursAuthoritative(t *testing.T) {
	base := []corev1.EnvVar{{Name: "GRAPHENE_RUN_ID", Value: "stale"}, {Name: "EXTRA", Value: "keep"}}
	ours := []corev1.EnvVar{{Name: "GRAPHENE_RUN_ID", Value: "fresh"}}
	got := mergeEnv(base, ours)
	if envVal(got, "GRAPHENE_RUN_ID") != "fresh" {
		t.Errorf("ours must win: %v", got)
	}
	if envVal(got, "EXTRA") != "keep" {
		t.Errorf("base extras must survive: %v", got)
	}
}

func envVal(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
