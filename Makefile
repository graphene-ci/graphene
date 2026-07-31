.DEFAULT_GOAL := help
BIN := $(CURDIR)/bin

.PHONY: configure
configure: ## Set up a working environment from scratch (tools go to bin/)
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	go mod tidy

.PHONY: lint
lint: ## Lint the module
	$(BIN)/golangci-lint run ./...

.PHONY: fmt
fmt: ## Format the module (gci, gofumpt, goimports)
	$(BIN)/golangci-lint fmt ./...

.PHONY: test
test: ## Run all tests
	go test ./...

.PHONY: help
help: ## List all targets with explanations
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
