.DEFAULT_GOAL := help
BIN := $(CURDIR)/bin
export PATH := $(BIN):$(PATH)

CONTROLLER_GEN_VERSION := v0.21.0
GOLANGCI_LINT_VERSION  := v2.12.2
KO_VERSION             := v0.19.1

.PHONY: configure
configure: ## Поставить инструменты в bin/ прибитых версий
	GOBIN=$(BIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	GOBIN=$(BIN) go install github.com/google/ko@$(KO_VERSION)
	go mod tidy

.PHONY: test
test: ## Прогнать тесты
	go test ./...

.PHONY: lint
lint: ## Линтер
	$(BIN)/golangci-lint run ./...

.PHONY: help
help: ## Список целей
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'
