package pipeline_test

import (
	"os/exec"
	"strings"
	"testing"
)

// sdk is the package the user's pipeline imports. Everything it drags in,
// the user's binary carries.
const sdk = "github.com/graphene-ci/graphene/pkg/pipeline"

// Модули, которые пользовательскому пайплайну положено нести.
//
// Список из МОДУЛЕЙ, а не из пакетов, и намеренно: пакетов у Temporal и
// apimachinery сотни, они меняются от патча к патчу, и такой тест падал бы
// на чужих обновлениях вместо наших ошибок. Модуль появляется редко и
// всегда по чьему-то решению — ровно то, что тест обязан ловить.
//
// Чего здесь нет и быть не должно: sigs.k8s.io/controller-runtime,
// k8s.io/client-go и любые облачные SDK. Пользователь их не заказывал.
func allowedModules() map[string]bool {
	return map[string]bool{
		// Наш собственный модуль: какие ИМЕННО пакеты из него сюда
		// доезжают, проверяет отдельный тест ниже.
		"github.com/graphene-ci/graphene": true,

		// Ход дела.
		"go.temporal.io/sdk": true,
		"go.temporal.io/api": true,
		// Тянет go.temporal.io/api: её protobuf размечен аннотациями
		// grpc-gateway. Кода шлюза в пайплайн не попадает, только типы
		// аннотаций.
		"github.com/grpc-ecosystem/grpc-gateway/v2": true,

		// Виды и их сериализация.
		"k8s.io/apimachinery":                  true,
		"k8s.io/apiextensions-apiserver":       true,
		"k8s.io/klog/v2":                       true,
		"k8s.io/utils":                         true,
		"k8s.io/kube-openapi":                  true,
		"sigs.k8s.io/json":                     true,
		"sigs.k8s.io/structured-merge-diff/v6": true,
		"sigs.k8s.io/randfill":                 true,
		"gopkg.in/inf.v0":                      true,
		"github.com/fxamacker/cbor/v2":         true,
		"github.com/x448/float16":              true,
		"github.com/google/gofuzz":             true,
		"github.com/json-iterator/go":          true,
		"github.com/modern-go/reflect2":        true,
		"github.com/modern-go/concurrent":      true,
		"github.com/go-openapi/jsonpointer":    true,
		"github.com/go-openapi/swag":           true,
		"github.com/go-openapi/swag/jsonname":  true,
		"github.com/mailru/easyjson":           true,
		"github.com/josharian/intern":          true,

		// Чем Temporal разговаривает по проводу.
		"google.golang.org/grpc":                          true,
		"google.golang.org/genproto/googleapis/api":       true,
		"google.golang.org/protobuf":                      true,
		"google.golang.org/genproto/googleapis/rpc":       true,
		"github.com/gogo/protobuf":                        true,
		"golang.org/x/net":                                true,
		"golang.org/x/text":                               true,
		"golang.org/x/sys":                                true,
		"golang.org/x/time":                               true,
		"golang.org/x/exp":                                true,
		"github.com/pborman/uuid":                         true,
		"github.com/google/uuid":                          true,
		"github.com/facebookgo/clock":                     true,
		"github.com/robfig/cron/v3":                       true,
		"go.uber.org/atomic":                              true,
		"go.opentelemetry.io/otel":                        true,
		"go.opentelemetry.io/otel/trace":                  true,
		"go.opentelemetry.io/otel/metric":                 true,
		"go.opentelemetry.io/auto/sdk":                    true,
		"github.com/go-logr/logr":                         true,
		"github.com/go-logr/stdr":                         true,
		"github.com/nexus-rpc/sdk-go":                     true,
		"github.com/nexus-rpc/nexus-proto-annotations":    true,
		"github.com/grpc-ecosystem/go-grpc-middleware/v2": true,
		"golang.org/x/sync":                               true,
		"github.com/robfig/cron":                          true,

		// Это не наш выбор и не ошибка: go.temporal.io/sdk/internal —
		// пакет, через который проходит ВЕСЬ SDK, — импортирует
		// testify/mock и gomock безусловно, вместе со своим тестовым
		// окружением. Значит, их несёт каждый воркер на Go, чей угодно,
		// не только наш.
		//
		// Убрать это можно было бы только отказавшись от Go SDK Temporal.
		// Записано здесь, чтобы это было известным фактом, а не сюрпризом
		// при разборе размера образа.
		"github.com/stretchr/testify":   true,
		"github.com/stretchr/objx":      true,
		"github.com/golang/mock":        true,
		"github.com/davecgh/go-spew":    true,
		"github.com/pmezard/go-difflib": true,
		"gopkg.in/yaml.v3":              true,
		"go.yaml.in/yaml/v2":            true,
	}
}

func TestSdkStaysThin(t *testing.T) {
	t.Parallel()

	unexpected := make([]string, 0)

	for _, module := range modulesOf(t, sdk) {
		if !allowedModules()[module] {
			unexpected = append(unexpected, module)
		}
	}

	if len(unexpected) > 0 {
		t.Fatalf("SDK потянул модули, которых пользователь не заказывал:\n  %s\n\n"+
			"Либо перенеси код в internal/, либо — если модуль здесь правда нужен —\n"+
			"впиши его в allowedModules() и объясни в том же коммите, зачем.",
			strings.Join(unexpected, "\n  "))
	}
}

// Из нашего модуля SDK имеет право видеть только виды и контракт. Всё
// остальное — internal/, cmd/, обёртки — на той стороне: SDK планирует
// работу, а не выполняет её.
func TestSdkSeesOnlyKindsAndContract(t *testing.T) {
	t.Parallel()

	allowed := map[string]bool{
		sdk:                                      true,
		"github.com/graphene-ci/graphene/api/v1": true,
		"github.com/graphene-ci/graphene/pkg/agent": true,
	}

	for _, pkg := range packagesOf(t, sdk) {
		if !strings.HasPrefix(pkg, "github.com/graphene-ci/graphene") {
			continue
		}

		if !allowed[pkg] {
			t.Errorf("SDK дотянулся до своего же %s — это не его сторона", pkg)
		}
	}
}

// modulesOf lists every module the package depends on, transitively.
func modulesOf(t *testing.T, pkg string) []string {
	t.Helper()

	return golist(t, "{{if .Module}}{{.Module.Path}}{{end}}", pkg)
}

// packagesOf lists every package the package depends on, transitively.
func packagesOf(t *testing.T, pkg string) []string {
	t.Helper()

	return golist(t, "{{.ImportPath}}", pkg)
}

func golist(t *testing.T, format, pkg string) []string {
	t.Helper()

	out, err := exec.Command("go", "list", "-deps", "-f", format, pkg).CombinedOutput()
	if err != nil {
		t.Fatalf("go list не отработал: %v\n%s", err, out)
	}

	seen := make(map[string]bool)
	unique := make([]string, 0)

	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || seen[line] {
			continue
		}

		seen[line] = true

		unique = append(unique, line)
	}

	return unique
}
