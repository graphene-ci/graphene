//go:build e2e

// Package e2e checks the system as a whole, against a real cluster with a
// real Temporal in it. It is behind a build tag because it needs `make up`
// and `make install` first, and because it takes minutes rather than
// milliseconds — `make test` must stay something a person runs constantly.
package e2e_test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/graphene-ci/graphene/internal/cli"
	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/worker"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
)

const (
	namespace = "default"
	pipeline  = "probe"
	// temporalHost and control are how a worker on this machine reaches
	// the control plane. Both are loopback because the environment binds
	// them there deliberately.
	temporalHost = "127.0.0.1:7233"
	control      = "http://127.0.0.1:18080"
	// registry is where the built image goes. The cluster reads it under
	// the same name — that is what deploy/local/k3d.yaml arranges.
	registry = "localhost:5555/graphene"
	// patience bounds the whole run: building an image and starting a
	// worker for a revision that has never been pushed takes a while.
	patience = 5 * time.Minute
)

func connect(t *testing.T) client.Client {
	t.Helper()

	cfg, err := kube.Config(os.Getenv("KUBECONFIG"))
	if err != nil {
		t.Fatalf("кластера нет — сначала make up && make install: %v", err)
	}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("схема не собралась: %v", err)
		}
	}

	built, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("клиент не собрался: %v", err)
	}

	return built
}

// push builds the probe example and records its revision. Doing it once per
// test rather than once per package keeps each test standalone; the image
// digest is the same both times, so the second push is a no-op.
func push(ctx context.Context, t *testing.T, kube client.Client) {
	t.Helper()

	_, err := cli.Push(ctx, cli.PushRequest{
		Kube:      kube,
		Builder:   cli.Ko{Path: tool("ko"), Repo: registry, Out: os.Stderr},
		Namespace: namespace,
		Pipeline:  pipeline,
		Dir:       "../../examples/probe",
	})
	if err != nil {
		t.Fatalf("push не прошёл: %v", err)
	}
}

// serve runs the pipeline's worker here, the way a person would.
//
// The control plane no longer runs pipelines: a pipeline is arbitrary code
// and executing it where our credentials live would turn "push a pipeline"
// into "run anything you like inside the control plane". So the check does
// what its author would do — serves the revision from its own machine.
func serve(ctx context.Context, t *testing.T, kube client.Client, name, dir string) {
	t.Helper()

	ctx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)

	ready := make(chan struct{})

	go func() {
		close(ready)

		_ = cli.Serve(ctx, cli.ServeRequest{
			Kube:      kube,
			Namespace: namespace,
			Pipeline:  name,
			Dir:       dir,
			Temporal:  temporalHost,
			Address:   "graphene",
			Control:   control,
			// В io.Discard, а не в stderr теста: воркер запускает go run,
			// тот — собранный бинарь, и любой из них, доживший до конца
			// теста, держал бы трубу, которую go test ждёт закрытой.
			Out: io.Discard,
		})
	}()

	<-ready
	// Воркеру нужно собраться и подписаться на очередь. Прогон, начатый
	// раньше, не пропадёт — он подождёт в очереди, — но проверка ждёт
	// целиком, и лишняя минута ожидания тут дешевле мигания.
	time.Sleep(20 * time.Second)
}

func tool(name string) string {
	beside := "../../bin/" + name
	if _, err := os.Stat(beside); err == nil {
		return beside
	}

	return name
}

func start(ctx context.Context, t *testing.T, kube client.Client, after string) *v1.Run {
	t.Helper()

	run, err := cli.Start(ctx, cli.StartRequest{
		Kube:      kube,
		Namespace: namespace,
		Pipeline:  pipeline,
		Params:    []byte(`{"after":"` + after + `"}`),
	})
	if err != nil {
		t.Fatalf("прогон не создался: %v", err)
	}

	return run
}

func await(ctx context.Context, t *testing.T, kube client.Client, name string) v1.RunPhase {
	t.Helper()

	phase, err := cli.Watch(ctx, cli.WatchRequest{
		Kube: kube, Namespace: namespace, Name: name, Out: os.Stderr,
	})
	if err != nil {
		t.Fatalf("слежение сорвалось: %v", err)
	}

	return phase
}

// probesOf counts the records this run made.
func probesOf(ctx context.Context, t *testing.T, kube client.Client, run string) int {
	t.Helper()

	var list v1.ProbeList
	if err := kube.List(ctx, &list, client.MatchingLabels{worker.LabelRun: run}); err != nil {
		t.Fatalf("пробы не читаются: %v", err)
	}

	return len(list.Items)
}

// Вся сшивка целиком: push кладёт ревизию, run создаёт запись, оператор
// поднимает воркфлоу, воркфлоу через системный воркер создаёт запись,
// оператор видит её готовность и будит воркфлоу, прогон завершается.
func TestRunGoesThrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	push(ctx, t, kube)

	serve(ctx, t, kube, pipeline, "../../examples/probe")

	run := start(ctx, t, kube, "2s")

	if phase := await(ctx, t, kube, run.Name); phase != v1.RunSucceeded {
		t.Fatalf("прогон завершился как %s", phase)
	}

	if count := probesOf(ctx, t, kube, run.Name); count != 1 {
		t.Fatalf("прогон создал %d записей вместо одной", count)
	}
}

// Убийство управляющего посреди прогона не должно ни ронять прогон, ни
// плодить записи: ход дела лежит в Temporal, а не в памяти оператора, и
// имя записи выводится из прогона и памятки, а не придумывается.
func TestRunSurvivesOperatorDeath(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), patience)
	defer cancel()

	kube := connect(t)
	push(ctx, t, kube)

	serve(ctx, t, kube, pipeline, "../../examples/probe")

	run := start(ctx, t, kube, "25s")

	// Дать прогону начаться, чтобы убийство пришлось на середину, а не
	// на момент до старта воркфлоу.
	time.Sleep(6 * time.Second)

	kill(ctx, t, kube)

	if phase := await(ctx, t, kube, run.Name); phase != v1.RunSucceeded {
		t.Fatalf("после убийства оператора прогон завершился как %s", phase)
	}

	if count := probesOf(ctx, t, kube, run.Name); count != 1 {
		t.Fatalf("после перезапуска оператора записей стало %d", count)
	}
}

// kill removes the operator's pod. Kubernetes brings it back; the point is
// that everything in its memory is gone when it returns.
func kill(ctx context.Context, t *testing.T, kube client.Client) {
	t.Helper()

	var pods corev1.PodList

	err := kube.List(ctx, &pods,
		client.InNamespace("graphene-system"),
		client.MatchingLabels{"app.kubernetes.io/component": "operator"})
	if err != nil {
		t.Fatalf("поды оператора не читаются: %v", err)
	}

	if len(pods.Items) == 0 {
		t.Fatal("оператор не установлен — сначала make install")
	}

	for i := range pods.Items {
		if err := kube.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0)); err != nil {
			t.Fatalf("оператор не убился: %v", err)
		}
	}

	t.Logf("оператор убит: %d под(ов)", len(pods.Items))
}
