package managed

import (
	"context"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
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
	dep.Annotations = nil
	dep.Spec.Template.Annotations = nil
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

func TestK8sLongRunIdentityLifecycle(t *testing.T) {
	r := k8sTestRunner(t, "")
	r.namespace = strings.Repeat("tenant", 12)
	r.clients = fake.NewClientset()
	r.log = xlog.New(xlog.NopCore{})
	run := id.RunId("matrix-ydb-managed-validation-ba8bf3ae-ydb-managed-dedicated-medium")
	if err := r.Start(t.Context(), run, "worker:test", "token"); err != nil {
		t.Fatal(err)
	}
	list, err := r.clients.AppsV1().Deployments(r.cfg.PodNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil || len(list.Items) != 1 {
		t.Fatalf("deployments: %v, err=%v", list, err)
	}
	dep := list.Items[0]
	for _, values := range []map[string]string{dep.Labels, dep.Spec.Selector.MatchLabels, dep.Spec.Template.Labels} {
		for key, value := range values {
			if errs := validation.IsValidLabelValue(value); len(errs) != 0 {
				t.Fatalf("invalid label %s=%q: %v", key, value, errs)
			}
		}
	}
	for _, annotations := range []map[string]string{dep.Annotations, dep.Spec.Template.Annotations} {
		if annotations[labelRun] != string(run) || annotations[labelNamespace] != r.namespace {
			t.Fatalf("identity lost: %v", annotations)
		}
	}
	if envVal(dep.Spec.Template.Spec.Containers[0].Env, "GRAPHENE_RUN_ID") != string(run) {
		t.Fatal("worker must retain original queue")
	}
	if created, err := r.Ensure(t.Context(), run, "worker:test", "token"); err != nil || created {
		t.Fatalf("ensure: created=%v err=%v", created, err)
	}
	tc := &identityTemporalClient{}
	r.temporal = tc
	r.Reap(t.Context())
	if tc.workflowID != "run/"+string(run) {
		t.Fatalf("reaper queried %q", tc.workflowID)
	}
	list, err = r.clients.AppsV1().Deployments(r.cfg.PodNamespace).List(t.Context(), metav1.ListOptions{})
	if err != nil || len(list.Items) != 0 {
		t.Fatalf("reap left deployments: %v err=%v", list, err)
	}
}

type identityTemporalClient struct {
	client.Client
	workflowID string
}

func (c *identityTemporalClient) DescribeWorkflowExecution(_ context.Context, workflowID, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	c.workflowID = workflowID
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}}, nil
}

func TestK8sEncodedLabelDoesNotAuthorizeAnotherIdentity(t *testing.T) {
	r := k8sTestRunner(t, "")
	r.log = xlog.New(xlog.NopCore{})
	run := id.RunId(strings.Repeat("run-", 20))
	dep := r.deployment(run, "worker:test", "token")
	dep.Annotations[labelRun] = "different-run"
	r.clients = fake.NewClientset(dep)
	if err := r.Start(t.Context(), run, "worker:test", "token"); err == nil {
		t.Fatal("accepted a matching label with a different full identity")
	}
}

func (c *identityTemporalClient) CountWorkflow(_ context.Context, _ *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 0}, nil
}
