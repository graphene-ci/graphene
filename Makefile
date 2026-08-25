.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin

export PATH := $(BIN):$(PATH)

GOLANGCI_LINT_VERSION := v2.12.2

.PHONY: configure
configure: $(BIN)/golangci-lint ## Install pinned repository tools into bin

$(BIN)/golangci-lint:
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: test
test: ## Run tests
	go test ./...

.PHONY: lint
lint: ## Run Go linters
	$(BIN)/golangci-lint run ./...

.PHONY: build
build: ## Build the server binary
	go build ./cmd/graphene-server

.PHONY: generate
generate: ## Generate Go contracts from proto
	easyp generate

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'

.PHONY: compose-up
compose-up: ## Bring the installation up (server + dependencies)
	docker compose up -d --build

.PHONY: compose-down
compose-down: ## Tear the installation down, keep volumes
	docker compose down
