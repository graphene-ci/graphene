package managed

import (
	"strings"
	"testing"

	"github.com/gopherex/xlog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/graphene-ci/pipeline/pkg/id"
)

func TestRunNamePreservesFullIdentity(t *testing.T) {
	pairs := [][4]string{
		{"t-stroppy-live", "matrix-mysql-family-1f79d4b3-mysql84-semi-sync", "t-stroppy-live", "matrix-mysql-family-1f79d4b3-mysql80-semi-sync"},
		{"tenant", "Run_1", "tenant", "run-1"},
		{"tenant-a", "b", "tenant", "a-b"},
		{strings.Repeat("tenant", 12), "one", strings.Repeat("tenant", 12), "two"},
	}
	for _, p := range pairs {
		a, b := runName(p[0], id.RunId(p[1])), runName(p[2], id.RunId(p[3]))
		if a == b {
			t.Errorf("distinct identities share %q", a)
		}
		for _, n := range []string{a, b} {
			if errs := validation.IsDNS1123Label(n); len(errs) > 0 {
				t.Errorf("invalid deployment name %q: %v", n, errs)
			}
		}
		if a != runName(p[0], id.RunId(p[1])) {
			t.Error("name is not deterministic")
		}
	}
}

func TestK8sWorkersDoNotShareDeployment(t *testing.T) {
	r := k8sTestRunner(t, "")
	r.namespace = "t-stroppy-live"
	r.clients = fake.NewClientset()
	r.log = xlog.New(xlog.NopCore{})
	runs := []id.RunId{"matrix-mysql-family-1f79d4b3-mysql84-semi-sync", "matrix-mysql-family-1f79d4b3-mysql80-semi-sync"}
	for _, run := range runs {
		if err := r.Start(t.Context(), run, "worker:test", "token"); err != nil {
			t.Fatal(err)
		}
	}
	list, err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("got %d deployments for two distinct runs", len(list.Items))
	}
	for _, run := range runs {
		created, err := r.Ensure(t.Context(), run, "worker:test", "token")
		if err != nil || created {
			t.Fatalf("ensure existing %s: created=%v err=%v", run, created, err)
		}
	}
}

func TestK8sRunnerAdoptsLegacyNameByIdentity(t *testing.T) {
	r := k8sTestRunner(t, "")
	r.log = xlog.New(xlog.NopCore{})
	dep := r.deployment("run-1", "worker:test", "token")
	dep.Name = "graphene-run-acme-run-1"
	r.clients = fake.NewClientset(dep)
	created, err := r.Ensure(t.Context(), "run-1", "worker:test", "token")
	if err != nil || created {
		t.Fatalf("legacy worker was duplicated: created=%v err=%v", created, err)
	}
	if err := r.Start(t.Context(), "run-1", "worker:test", "token"); err != nil {
		t.Fatal(err)
	}
	list, err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != dep.Name {
		t.Fatal("legacy worker changed")
	}
}

func TestK8sStartRejectsAnotherRunsDeployment(t *testing.T) {
	r := k8sTestRunner(t, "")
	r.log = xlog.New(xlog.NopCore{})
	dep := r.deployment("other-run", "worker:test", "token")
	dep.Name = runName(r.namespace, "run-1")
	r.clients = fake.NewClientset(dep)
	if err := r.Start(t.Context(), "run-1", "worker:test", "token"); err == nil {
		t.Fatal("another run's deployment was accepted as this worker")
	}
}
