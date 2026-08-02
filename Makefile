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
build: ## Build the graphen binary into bin/
	go build -o $(BIN)/graphen ./cmd/graphen

.PHONY: kernel-install
kernel-install: build ## Install the kernel as a user service (no privileges)
	$(BIN)/graphen kernel install --scope user

.PHONY: kernel-reinstall
kernel-reinstall: build ## Reinstall over the running service (new binary, same data)
	$(BIN)/graphen kernel install --scope user --force
	systemctl --user restart graphen-kernel.service

.PHONY: kernel-status
kernel-status: ## Show the user service status
	$(BIN)/graphen kernel status --scope user

.PHONY: kernel-logs
kernel-logs: ## Follow the user service logs
	journalctl --user -u graphen-kernel.service -f

.PHONY: kernel-uninstall
kernel-uninstall: ## Remove the user service (data and configuration are kept)
	$(BIN)/graphen kernel uninstall --scope user

.PHONY: help
help: ## List all targets with explanations
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
