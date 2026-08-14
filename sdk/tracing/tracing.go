// Package tracing is how a run explains itself. One trace covers the whole
// of it — the record, the workflow, the step on the machine, the teardown —
// so that "why did this stand for eight minutes" is one look instead of
// four logs lined up by hand.
package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

// envEndpoint is where traces go. Empty means nowhere, and that is a
// working configuration: an installation that does not want tracing pays
// nothing for it and every call below still works.
const envEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"

// flushGrace is how long a shutting-down process waits for what it already
// recorded to leave.
const flushGrace = 5 * time.Second

// Setup starts sending traces and returns how to stop.
//
// It never fails the caller for want of a collector: a receiver that is not
// there is a receiver we retry against, not a reason for the control plane
// to refuse to start.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	endpoint := os.Getenv(envEndpoint)
	if endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())

		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("приёмник трасс не собрался: %w", err)
	}

	from, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL, semconv.ServiceName(service)))
	if err != nil {
		return nil, fmt.Errorf("описание службы не собралось: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter), sdktrace.WithResource(from))

	otel.SetTracerProvider(provider)

	return func(ctx context.Context) error {
		ctx, stop := context.WithTimeout(ctx, flushGrace)
		defer stop()

		if err := provider.Shutdown(ctx); err != nil {
			return fmt.Errorf("трассы не дослались: %w", err)
		}

		return nil
	}, nil
}

// Tracer is what to write spans with.
func Tracer() trace.Tracer { return otel.Tracer("graphene") }

// Carry renders the current trace context as something that can travel in
// a record, a manifest, or an environment — anywhere a map of strings goes.
//
// This is the seam that makes one trace out of a system where the halves
// do not call each other: a record is created now and becomes ready in
// eight minutes, and nothing connects the two except what the record
// itself carries.
func Carry(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	return carrier
}

// Resume picks the trace back up from what was carried.
func Resume(ctx context.Context, carried map[string]string) context.Context {
	if len(carried) == 0 {
		return ctx
	}

	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carried))
}

// Took records something that already happened: it began then, it ended
// now, and nobody was holding a span while it went on.
//
// Waiting is exactly this shape. A record is created, and something outside
// the process makes it ready — a cloud, a boot, a person. The only place
// that can say how long that took is the one that noticed it ended.
func Took(ctx context.Context, name string, began time.Time, attrs ...attribute.KeyValue) {
	_, span := Tracer().Start(ctx, name,
		trace.WithTimestamp(began), trace.WithAttributes(attrs...))
	span.End()
}

// Interceptor is what puts a workflow and its activities in one trace.
//
// Temporal is the one place where a call crosses a process boundary
// without a network call anybody can see: a workflow schedules an activity
// and something somewhere else picks it up minutes later. The interceptor
// carries the trace context through the same history that carries the work.
func Interceptor() (interceptor.ClientInterceptor, error) {
	made, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
		Tracer: Tracer(),
	})
	if err != nil {
		return nil, fmt.Errorf("перехватчик трасс не собрался: %w", err)
	}

	return made, nil
}

// Interceptors is what goes into client options, or nothing if tracing is
// off. A client that refuses to start because a collector is missing would
// be a worse system than one that is silent about where time went.
func Interceptors() []interceptor.ClientInterceptor {
	made, err := Interceptor()
	if err != nil {
		return nil
	}

	return []interceptor.ClientInterceptor{made}
}
