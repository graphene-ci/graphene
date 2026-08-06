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

.PHONY: build
build: ## Build both binaries into bin/
	go build -o $(BIN)/graphened ./cmd/graphened
	go build -o $(BIN)/gctl ./cmd/gctl

.PHONY: install
install: build ## Install the kernel as a service for this user
	$(BIN)/graphened install

.PHONY: reinstall
reinstall: build ## Reinstall over the running service (new binary, same data)
	$(BIN)/graphened stop || true
	$(BIN)/graphened install
	$(BIN)/graphened start

.PHONY: status
status: ## Show the service status
	$(BIN)/graphened status

.PHONY: uninstall
uninstall: ## Remove the service (data and configuration are kept)
	$(BIN)/graphened uninstall

.PHONY: help
help: ## List all targets with explanations
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
