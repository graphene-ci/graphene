.DEFAULT_GOAL := help
BIN := $(CURDIR)/bin
export PATH := $(BIN):$(PATH)

# Ничего не пишем в ~/.kube/config: окружение репозитория живёт в самом
# репозитории и сносится вместе с ним.
export KUBECONFIG := $(CURDIR)/.kubeconfig

# One toolchain for everything: the build, the tests and the tools in bin/.
# kubernetes 0.36 requires 1.26, and a linter built with an older toolchain
# cannot type-check what the newer one compiles — it panics rather than
# reports. Go downloads this version itself; nothing is installed system-wide.
GO_VERSION := 1.26.5
export GOTOOLCHAIN := go$(GO_VERSION)

CONTROLLER_GEN_VERSION := v0.21.0
GOLANGCI_LINT_VERSION  := v2.12.2
KO_VERSION             := v0.19.1
K3D_VERSION            := v5.9.0
HELM_VERSION           := v3.21.3
KUBECTL_VERSION        := v1.36.3
CROSSPLANE_VERSION     := 2.3.4

CLUSTER  := graphene
REGISTRY := localhost:5555/graphene
OS       := $(shell go env GOOS)
ARCH     := $(shell go env GOARCH)
KUBECTL  := $(BIN)/kubectl
HELM     := $(BIN)/helm
K3D      := $(BIN)/k3d

.PHONY: configure
configure: ## Поставить инструменты в bin/ прибитых версий
	GOBIN=$(BIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	GOBIN=$(BIN) go install github.com/google/ko@$(KO_VERSION)
	GOBIN=$(BIN) go install github.com/k3d-io/k3d/v5@$(K3D_VERSION)
	GOBIN=$(BIN) go install helm.sh/helm/v3/cmd/helm@$(HELM_VERSION)
	# Единственный инструмент не из go install: kubectl живёт в
	# k8s.io/kubernetes, а тот не ставится без replace-директив. Поэтому
	# бинарь качается — и сверяется с опубликованной суммой, иначе
	# «прибитая версия» означает только имя файла.
	curl -fsSL -o $(KUBECTL) \
		https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(OS)/$(ARCH)/kubectl
	curl -fsSL -o $(KUBECTL).sha256 \
		https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(OS)/$(ARCH)/kubectl.sha256
	echo "$$(cat $(KUBECTL).sha256)  $(KUBECTL)" | sha256sum --check --status
	rm -f $(KUBECTL).sha256
	chmod +x $(KUBECTL)
	go mod tidy

.PHONY: generate
generate: ## Породить deepcopy и манифесты CRD из типов в api/
	$(BIN)/controller-gen object paths=./api/...
	$(BIN)/controller-gen crd paths=./api/... output:crd:dir=deploy/crd

.PHONY: up
up: ## Поднять локальное окружение: k3s, Temporal, Crossplane
	$(K3D) cluster get $(CLUSTER) >/dev/null 2>&1 || \
		$(K3D) cluster create --config deploy/local/k3d.yaml
	$(K3D) kubeconfig get $(CLUSTER) > $(KUBECONFIG)
	$(KUBECTL) apply -f deploy/local/temporal.yaml
	$(HELM) upgrade --install crossplane \
		https://charts.crossplane.io/stable/crossplane-$(CROSSPLANE_VERSION).tgz \
		--namespace crossplane-system --create-namespace --wait
	go run ./cmd/graphene up --wait

.PHONY: build
build: ## Собрать наши бинари в bin/
	go build -o $(BIN)/graphene ./cmd/graphene

.PHONY: install
install: generate ## Поставить наш управляющий слой в локальный кластер
	# Бинарь агента едет внутри образа оператора как ko-данные: тогда
	# «оператор установлен» и «агента нужной версии можно поставить» —
	# один и тот же факт, а не два.
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cmd/graphene-operator/kodata/agent/linux/amd64/graphene-agent \
		./cmd/graphene-agent
	$(KUBECTL) apply -f deploy/crd/
	$(KUBECTL) apply -f deploy/graphene/rbac.yaml
	KO_DOCKER_REPO=$(REGISTRY) $(BIN)/ko build --bare \
		./cmd/graphene-operator > .operator-image
	KO_DOCKER_REPO=$(REGISTRY) $(BIN)/ko build --bare \
		./cmd/graphene-worker > .worker-image
	sed -e "s|OPERATOR_IMAGE|$$(cat .operator-image)|" \
		-e "s|WORKER_IMAGE|$$(cat .worker-image)|" \
		deploy/graphene/control.yaml | $(KUBECTL) apply -f -
	$(KUBECTL) -n graphene-system rollout status deployment/graphene-operator --timeout=120s
	$(KUBECTL) -n graphene-system rollout status deployment/graphene-worker --timeout=120s

.PHONY: down
down: ## Снести локальное окружение целиком
	-$(K3D) cluster delete $(CLUSTER)
	rm -f $(KUBECONFIG)

.PHONY: test
test: ## Прогнать тесты
	go test ./...

.PHONY: e2e
e2e: build ## Сквозная проверка на живом кластере (нужны make up и make install)
	go test -tags e2e -count=1 -timeout 20m ./test/e2e/...

.PHONY: lint
lint: ## Линтер
	$(BIN)/golangci-lint run ./...

.PHONY: help
help: ## Список целей
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'
