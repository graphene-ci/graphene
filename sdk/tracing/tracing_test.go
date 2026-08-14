package tracing_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/graphene-ci/graphene/sdk/tracing"
)

// Трасса переживает дорогу в записи и обратно. Это и есть шов, которым
// прогон сшивается в одно: запись создана сейчас, готовой станет через
// восемь минут, и связать эти два момента может только то, что она несёт.
func TestTraceSurvivesTheRecord(t *testing.T) {
	t.Parallel()

	if _, err := tracing.Setup(t.Context(), "test"); err != nil {
		t.Fatalf("трассировка не завелась: %v", err)
	}

	// Приёмника нет, а провайдер нужен настоящий: пустой адрес даёт
	// пустышку, которая не рождает опознаваемых отрезков.
	otel.SetTracerProvider(sdktrace.NewTracerProvider())

	ctx, span := tracing.Tracer().Start(t.Context(), "прогон")
	defer span.End()

	carried := tracing.Carry(ctx)
	if len(carried) == 0 {
		t.Fatal("трасса не поехала — записи нечего нести")
	}

	resumed := tracing.Resume(context.Background(), carried)

	was := trace.SpanContextFromContext(ctx)
	now := trace.SpanContextFromContext(resumed)

	if now.TraceID() != was.TraceID() {
		t.Fatalf("трасса другая: %s вместо %s", now.TraceID(), was.TraceID())
	}

	if now.SpanID() != was.SpanID() {
		t.Fatal("отрезок потерялся — то, что случилось потом, повиснет отдельно")
	}
}

// Без приёмника всё работает и ничего не стоит. Установка, которая не
// хочет трассировки, не должна за неё платить и тем более не должна из-за
// неё не подниматься.
func TestNoReceiverIsAWorkingConfiguration(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	stop, err := tracing.Setup(t.Context(), "test")
	if err != nil {
		t.Fatalf("без приёмника не завелось: %v", err)
	}

	_, span := tracing.Tracer().Start(t.Context(), "шаг")
	span.End()

	if err := stop(t.Context()); err != nil {
		t.Fatalf("остановка не прошла: %v", err)
	}

	if made, err := tracing.Interceptor(); err != nil || made == nil {
		t.Fatalf("перехватчик не собрался без приёмника: %v", err)
	}
}
