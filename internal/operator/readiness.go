package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/graphene-ci/graphene/internal/kube"
	"github.com/graphene-ci/graphene/internal/worker"
	"github.com/graphene-ci/graphene/sdk/agent"
	v1 "github.com/graphene-ci/graphene/sdk/api/v1"
	"github.com/graphene-ci/graphene/sdk/tracing"
)

// ErrNotWatchable means the cluster does not serve the kind we were asked
// to watch.
var ErrNotWatchable = errors.New("вид не обслуживается кластером")

// Signaller wakes a workflow that is waiting for a record.
type Signaller interface {
	Signal(ctx context.Context, workflowID, name string, payload agent.ReadySignal) error
}

// Watcher is asked to follow a kind. What it is asked about is a run's
// requirements, which is the second use of a list that already existed.
type Watcher interface {
	Watch(ctx context.Context, kind agent.Kind) error
}

// Readiness tells a waiting workflow when a record it created has arrived.
//
// This is the other half of Await. The workflow does not poll and holds no
// worker slot while a machine boots for three minutes: it sleeps in its
// history until this sends the signal.
//
// It follows whatever kinds it is told to, and it is told by the runs that
// start: a revision declares what it applies, the operator refuses early if
// the cluster does not serve that, and the same list says what to watch.
// One list, two uses — a kind nobody applies is never watched, and a kind
// somebody applies is watched before the first record of it exists.
//
// The informers are dynamic because the kinds are not knowable when this is
// compiled — that is the whole point of the system. Each is filtered to
// records we made: somebody's own Instance in the same cluster is none of
// our business, and following every record of a kind would put the entire
// cloud inventory into our cache.
type Readiness struct {
	kube    client.Client
	signal  Signaller
	resolve kube.Resolver

	mu      sync.Mutex
	factory dynamicinformer.DynamicSharedInformerFactory
	started map[schema.GroupVersionResource]bool
	stop    chan struct{}
	running bool
}

// NewReadiness builds one.
func NewReadiness(
	records client.Client, source dynamic.Interface, resolve kube.Resolver, signal Signaller,
) *Readiness {
	ours := func(options *metav1.ListOptions) {
		options.LabelSelector = worker.LabelManaged + "=true"
	}

	return &Readiness{
		kube:    records,
		signal:  signal,
		resolve: resolve,
		factory: dynamicinformer.NewFilteredDynamicSharedInformerFactory(source, 0, "", ours),
		started: map[schema.GroupVersionResource]bool{},
		stop:    make(chan struct{}),
	}
}

// Watch starts following a kind, once. Being asked twice is the normal
// case: every run of every revision asks for its own requirements.
func (r *Readiness) Watch(ctx context.Context, kind agent.Kind) error {
	gvk := schema.GroupVersionKind{Group: kind.Group, Version: kind.Version, Kind: kind.Kind}

	resource, _, err := r.resolve(ctx, gvk)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrNotWatchable, gvk, err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started[resource] {
		return nil
	}

	informer := r.factory.ForResource(resource).Informer()

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.report(ctx, obj) },
		UpdateFunc: func(_, obj any) { r.report(ctx, obj) },
	}

	if _, err := informer.AddEventHandler(handler); err != nil {
		return fmt.Errorf("наблюдение за %s не встало: %w", gvk, err)
	}

	r.started[resource] = true

	// Start запускает только то, что ещё не запущено, поэтому звать её на
	// каждый новый вид — правильно, а не расточительно.
	if r.running {
		r.factory.Start(r.stop)
	}

	return nil
}

// Start runs until the context is done. It satisfies manager.Runnable, so
// the manager starts and stops this with everything else.
func (r *Readiness) Start(ctx context.Context) error {
	r.mu.Lock()
	r.running = true
	r.factory.Start(r.stop)
	r.mu.Unlock()

	<-ctx.Done()
	close(r.stop)

	return nil
}

// report tells whoever waits for this record that it has arrived.
//
// Being called more than once for the same record is normal and harmless:
// the workflow keeps readiness by memo, and hearing the same thing twice
// does not change what it knows.
func (r *Readiness) report(ctx context.Context, obj any) {
	record, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}

	memo := record.GetAnnotations()[worker.AnnotationMemo]
	runName := record.GetLabels()[worker.LabelRun]

	if memo == "" || runName == "" {
		return
	}

	ready, reason := readiness(record.Object)
	if !ready {
		return
	}

	var run v1.Run

	key := client.ObjectKey{Namespace: record.GetNamespace(), Name: runName}
	if err := r.kube.Get(ctx, key, &run); err != nil {
		return
	}

	if run.Status.WorkflowID == "" || run.Status.Phase.Terminal() {
		return
	}

	// Ожидание, которое никто не держал: запись попросили тогда, готовой
	// она стала сейчас, и между этими двумя моментами не было процесса,
	// который мог бы держать отрезок трассы открытым. Закрыть его может
	// только тот, кто заметил конец, — и трассу для этого он берёт с
	// самой записи. Ровно это отвечает на «почему стояло восемь минут».
	waited(ctx, record)

	payload := agent.ReadySignal{
		Name:   memo,
		Ready:  true,
		Reason: reason,
		Status: statusOf(record.Object),
	}

	if err := r.signal.Signal(ctx, run.Status.WorkflowID, agent.SignalReady, payload); err != nil {
		log.FromContext(ctx).Error(err, "сигнал готовности не дошёл",
			"прогон", runName, "памятка", memo)
	}
}

// waited writes down how long this record took to arrive.
func waited(ctx context.Context, record *unstructured.Unstructured) {
	packed := record.GetAnnotations()[worker.AnnotationTrace]
	if packed == "" {
		return
	}

	var carried map[string]string
	if err := json.Unmarshal([]byte(packed), &carried); err != nil {
		return
	}

	began := record.GetCreationTimestamp().Time
	if began.IsZero() {
		return
	}

	tracing.Took(tracing.Resume(ctx, carried), "готовность "+record.GetKind(), began,
		attribute.String("record.kind", record.GetKind()),
		attribute.String("record.name", record.GetName()),
		attribute.String("memo", record.GetAnnotations()[worker.AnnotationMemo]))
}

// readiness reads the Ready condition, which is what everything in this
// ecosystem uses to say it has arrived — our kinds and Crossplane's alike.
func readiness(fields map[string]any) (bool, string) {
	conditions, found, err := unstructured.NestedSlice(fields, "status", "conditions")
	if err != nil || !found {
		return false, ""
	}

	for _, one := range conditions {
		condition, ok := one.(map[string]any)
		if !ok {
			continue
		}

		if condition["type"] != v1.ConditionReady {
			continue
		}

		reason, _ := condition["reason"].(string)

		return condition["status"] == "True", reason
	}

	return false, ""
}

// statusOf is the record's status as the cluster has it — the address, the
// id, whatever the provider filled in.
func statusOf(fields map[string]any) []byte {
	status, found, err := unstructured.NestedMap(fields, "status")
	if err != nil || !found {
		return nil
	}

	raw, err := json.Marshal(status)
	if err != nil {
		return nil
	}

	return raw
}
