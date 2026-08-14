package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/graphene-ci/graphene/pkg/agent"
)

// WorkflowName is what every pipeline's workflow is called in Temporal.
// One name for all of them: which pipeline ran is the queue and the
// revision, not the workflow type.
const WorkflowName = "graphene.Pipeline"

// Environment variables the worker of a revision is started with. The
// operator sets them; nothing here reads a config file.
const (
	EnvAddress   = "GRAPHENE_TEMPORAL_ADDRESS"
	EnvNamespace = "GRAPHENE_TEMPORAL_NAMESPACE"
	EnvQueue     = "GRAPHENE_QUEUE"
	EnvRevision  = "GRAPHENE_REVISION"
	EnvRecords   = "GRAPHENE_NAMESPACE"
)

// activityTimeout bounds one activity attempt. Ours are writes to the
// cluster — short, and retried until they land — so a minute is generous.
// Waiting for a machine is not an activity: that is what the readiness
// signal is for.
const activityTimeout = time.Minute

// Refusals Serve can produce before anything starts.
var (
	// ErrNoQueue means nothing said which queue to listen on.
	ErrNoQueue = errors.New("не задана очередь: " + EnvQueue)
	// ErrNotPipeline means the function handed to Serve is not one.
	ErrNotPipeline = errors.New("это не пайплайн: нужна func(pipeline.Run, P) error")
)

// Options is how a pipeline's worker is configured. Everything has a
// sensible source in the environment; the options are for tests and for
// the odd case the environment cannot express.
type Options struct {
	Address   string
	Namespace string
	Queue     string
	Scheme    *runtime.Scheme
	// Revision is the record this worker belongs to. The operator sets it
	// when it starts the worker; empty means nobody is listening and the
	// worker simply does not announce itself.
	Revision string
	// Records is the cluster namespace the revision's record lives in.
	Records string
}

// Option changes how Serve behaves.
type Option func(*Options)

// Queue overrides the task queue this worker listens on.
func Queue(name string) Option { return func(o *Options) { o.Queue = name } }

// Address overrides where Temporal is.
func Address(addr string) Option { return func(o *Options) { o.Address = addr } }

// Scheme teaches the SDK the kinds a pipeline applies.
//
// It is here because a generated type carries no apiVersion and no kind of
// its own, and Go has no global registry to look them up in. Without it,
// Apply refuses and says so by name; with it, the pipeline reads as if the
// question never came up.
func Scheme(add ...func(*runtime.Scheme) error) Option {
	return func(opts *Options) {
		if opts.Scheme == nil {
			opts.Scheme = runtime.NewScheme()
		}

		for _, one := range add {
			if err := one(opts.Scheme); err != nil {
				panic(fmt.Errorf("схема не собралась: %w", err))
			}
		}
	}
}

// Serve runs the pipeline as a worker: it connects to Temporal, listens on
// its queue, and executes the pipeline whenever a run asks for it. It
// returns when the process is asked to stop.
//
// fn is an ordinary function — func(pipeline.Run, P) error, where P is
// whatever the pipeline takes as parameters. It is not a workflow
// signature: the adapter below is.
func Serve(fn any, opts ...Option) error {
	options := fromEnvironment()
	for _, apply := range opts {
		apply(&options)
	}

	if options.Queue == "" {
		return ErrNoQueue
	}

	run, err := Workflow(fn, opts...)
	if err != nil {
		return err
	}

	temporal, err := client.Dial(client.Options{
		HostPort:  options.Address,
		Namespace: options.Namespace,
	})
	if err != nil {
		return fmt.Errorf("не подключиться к Temporal: %w", err)
	}
	defer temporal.Close()

	if err := announce(context.Background(), temporal, options); err != nil {
		return err
	}

	w := worker.New(temporal, options.Queue, worker.Options{})
	w.RegisterWorkflowWithOptions(run, workflow.RegisterOptions{Name: WorkflowName})

	// worker.Run takes a channel of anything; os/signal needs one of
	// os.Signal. One forwards into the other, and closing is what stops
	// the worker.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	stop := make(chan any, 1)

	go func() {
		<-signals
		close(stop)
	}()

	if err := w.Run(stop); err != nil {
		return fmt.Errorf("воркер остановился: %w", err)
	}

	return nil
}

func fromEnvironment() Options {
	return Options{
		Address:   os.Getenv(EnvAddress),
		Namespace: os.Getenv(EnvNamespace),
		Queue:     os.Getenv(EnvQueue),
		Revision:  os.Getenv(EnvRevision),
		Records:   os.Getenv(EnvRecords),
	}
}

// Requirements lists every kind a scheme knows, which is every kind this
// pipeline could apply.
//
// It reports what the pipeline COULD do rather than what it will: which
// kinds a run actually applies depends on its parameters, and refusing
// early is only possible on the wider answer. A requirement the run never
// reaches is a provider somebody installed for nothing — cheap. A missing
// one found halfway is a half-built stand — not cheap.
func Requirements(scheme *runtime.Scheme) []agent.Kind {
	if scheme == nil {
		return nil
	}

	seen := make(map[agent.Kind]bool)
	kinds := make([]agent.Kind, 0)

	for gvk, typ := range scheme.AllKnownTypes() {
		if !applicable(gvk, typ) {
			continue
		}

		one := agent.Kind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind}
		if seen[one] {
			continue
		}

		seen[one] = true

		kinds = append(kinds, one)
	}

	// Полный порядок, а не по имени: схема отдаёт виды в случайном
	// порядке, а требования сравнивают и пишут в запись — список,
	// который переставляется сам по себе, давал бы ложную разницу при
	// каждой сверке.
	sort.Slice(kinds, func(i, j int) bool {
		if kinds[i].Group != kinds[j].Group {
			return kinds[i].Group < kinds[j].Group
		}

		if kinds[i].Version != kinds[j].Version {
			return kinds[i].Version < kinds[j].Version
		}

		return kinds[i].Kind < kinds[j].Kind
	})

	return kinds
}

// machineryTypes is where kubernetes keeps the types every scheme carries
// whether it wants them or not.
const machineryTypes = "k8s.io/apimachinery/pkg/apis/meta/v1"

// applicable answers whether a kind is one a pipeline could actually apply.
//
// The filter is by the Go type's package, not by name, and that was learned
// the hard way: metav1.AddToGroupVersion — which every scheme calls, ours
// included — registers CreateOptions, ListOptions, PatchOptions and
// WatchEvent INTO THE CALLER'S OWN GROUP AND VERSION. By name and group
// they are indistinguishable from our kinds; by package they are obviously
// not ours. Requiring them made every run refuse itself.
func applicable(gvk schema.GroupVersionKind, typ reflect.Type) bool {
	switch {
	case gvk.Group == "":
		return false
	case gvk.Version == runtime.APIVersionInternal:
		return false
	case strings.HasSuffix(gvk.Kind, "List"):
		return false
	case typ != nil && typ.PkgPath() == machineryTypes:
		return false
	default:
		return true
	}
}

// announce tells the cluster what this revision needs. A worker that cannot
// announce still serves: the requirement check is a courtesy to whoever
// starts a run, not a condition of running.
func announce(ctx context.Context, temporal client.Client, options Options) error {
	if options.Revision == "" {
		return nil
	}

	start := client.StartWorkflowOptions{
		ID:                    "revision-" + options.Revision,
		TaskQueue:             agent.SystemQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	input := agent.RegisterRevisionInput{
		Revision:  options.Revision,
		Namespace: options.Records,
		Requires:  Requirements(options.Scheme),
	}

	handle, err := temporal.ExecuteWorkflow(ctx, start, agent.WorkflowRegisterRevision, input)
	if err != nil {
		return fmt.Errorf("ревизия не представилась: %w", err)
	}

	if err := handle.Get(ctx, nil); err != nil {
		return fmt.Errorf("требования не записались: %w", err)
	}

	return nil
}

// Workflow turns an ordinary function into the workflow Temporal will
// call. Serve does this for you; it is exported because a test — and, in
// time, anything that hosts pipelines itself — needs the same adapter
// without a connection to Temporal.
//
// The user's function takes a Run and its own parameter type; a workflow
// takes a workflow.Context and whatever was passed to it. The adapter is
// where those two meet — and where a panic raised by a step becomes an
// ordinary workflow failure, so that a pipeline can be written as a
// program rather than as a chain of error checks.
func Workflow(fn any, opts ...Option) (any, error) {
	var options Options
	for _, apply := range opts {
		apply(&options)
	}

	scheme := options.Scheme

	params, err := paramsTypeOf(fn)
	if err != nil {
		return nil, err
	}

	call := reflect.ValueOf(fn)

	return func(ctx workflow.Context, input agent.RunInput) (err error) {
		defer func() {
			if raised := recover(); raised != nil {
				err = asError(raised)
			}
		}()

		value := reflect.New(params)
		if len(input.Params) > 0 {
			if err := json.Unmarshal(input.Params, value.Interface()); err != nil {
				return fmt.Errorf("параметры не разобрались: %w", err)
			}
		}

		// Every activity of ours is a write to the cluster: short, and
		// retried until it lands. A timeout is not optional in Temporal,
		// so it is set here rather than at every call site.
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:           agent.SystemQueue,
			StartToCloseTimeout: activityTimeout,
		})

		run := Run{s: &state{
			ctx:     ctx,
			owner:   input.Owner,
			ready:   workflow.GetSignalChannel(ctx, agent.SignalReady),
			scheme:  scheme,
			arrived: map[string]agent.ReadySignal{},
		}}

		out := call.Call([]reflect.Value{reflect.ValueOf(run), value.Elem()})
		if raised, ok := out[0].Interface().(error); ok && raised != nil {
			return raised
		}

		return nil
	}, nil
}

// paramsTypeOf checks the shape of a pipeline and reports its parameter
// type. The refusal happens at startup rather than at the first run: a
// worker that cannot execute anything should say so while somebody is
// still watching it start.
func paramsTypeOf(fn any) (reflect.Type, error) {
	typ := reflect.TypeOf(fn)
	if typ == nil || typ.Kind() != reflect.Func {
		return nil, ErrNotPipeline
	}

	if typ.NumIn() != 2 || typ.NumOut() != 1 {
		return nil, ErrNotPipeline
	}

	if typ.In(0) != reflect.TypeFor[Run]() {
		return nil, ErrNotPipeline
	}

	if !typ.Out(0).Implements(reflect.TypeFor[error]()) {
		return nil, ErrNotPipeline
	}

	return typ.In(1), nil
}

// asError turns whatever was panicked with into the workflow's failure.
func asError(raised any) error {
	if err, ok := raised.(error); ok {
		return err
	}

	return fmt.Errorf("%w: %v", ErrNotPipeline, raised)
}
