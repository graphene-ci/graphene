.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin

export PATH := $(BIN):$(PATH)

SDK_TS := $(CURDIR)/sdk/ts

GOLANGCI_LINT_VERSION := v2.12.2
EASYP_VERSION := v0.16.6
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
PROTOC_GEN_CONNECT_GO_VERSION := v1.20.0
NODE_VERSION := $(shell cat $(CURDIR)/.node-version)

GO_TOOLS := $(BIN)/golangci-lint $(BIN)/easyp $(BIN)/protoc-gen-go \
	$(BIN)/protoc-gen-go-grpc $(BIN)/protoc-gen-connect-go

.PHONY: configure
configure: $(GO_TOOLS) $(SDK_TS)/node_modules ## Install pinned repository tools into bin and sdk/ts

$(BIN)/golangci-lint:
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(BIN)/easyp:
	GOBIN=$(BIN) go install github.com/easyp-tech/easyp/cmd/easyp@$(EASYP_VERSION)

$(BIN)/protoc-gen-go:
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(BIN)/protoc-gen-go-grpc:
	GOBIN=$(BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

$(BIN)/protoc-gen-connect-go:
	GOBIN=$(BIN) go install connectrpc.com/connect/cmd/protoc-gen-connect-go@$(PROTOC_GEN_CONNECT_GO_VERSION)

.PHONY: verify-node
verify-node:
	@test "$$(node --version)" = "$(NODE_VERSION)" || { \
		echo "Node.js $(NODE_VERSION) required, found $$(node --version 2>/dev/null || echo none)" >&2; \
		exit 1; \
	}

$(SDK_TS)/node_modules: $(SDK_TS)/package-lock.json | verify-node
	npm --prefix $(SDK_TS) ci --no-audit --no-fund
	touch $@

.PHONY: test
test: $(SDK_TS)/node_modules ## Run Go and TypeScript SDK tests
	go test ./...
	npm --prefix $(SDK_TS) test

.PHONY: lint
lint: $(SDK_TS)/node_modules ## Run Go linters and Biome on the TypeScript SDK
	$(BIN)/golangci-lint run ./...
	npm --prefix $(SDK_TS) run lint

.PHONY: build
build: ## Build the server binary
	go build ./cmd/graphene-server

.PHONY: generate
generate: $(GO_TOOLS) $(SDK_TS)/node_modules ## Generate Go and TypeScript contracts from proto
	$(RM) -r $(SDK_TS)/src/proto
	$(BIN)/easyp generate

.PHONY: sdk-ts
sdk-ts: $(SDK_TS)/node_modules ## Build the TypeScript SDK into sdk/ts/dist
	$(RM) -r $(SDK_TS)/dist
	npm --prefix $(SDK_TS) run build

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'

.PHONY: ver
ver: ## Cut a release: make ver v=X.Y.Z (creates + pushes the tag → release CI)
	@set -eu; \
	v="$(v)"; \
	if ! echo "$$v" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$'; then echo "usage: make ver v=X.Y.Z" >&2; exit 1; fi; \
	if [ -n "$$(git status --porcelain)" ]; then echo "working tree is dirty — commit first" >&2; exit 1; fi; \
	git tag "v$$v" && git push origin "v$$v"; \
	echo "tagged v$$v — release workflow running"

.PHONY: bump
bump: ## Bump release tag: make bump TYPE=patch|minor|major (default patch)
	@set -eu; \
	type="$(or $(TYPE),patch)"; \
	case "$$type" in patch|minor|major) ;; *) echo "TYPE must be patch|minor|major" >&2; exit 1 ;; esac; \
	cur="$$(git describe --tags --abbrev=0 2>/dev/null | sed -E 's/^v//' || true)"; \
	echo "$$cur" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' || cur="0.0.0"; \
	major="$${cur%%.*}"; rest="$${cur#*.}"; minor="$${rest%%.*}"; patch="$${rest##*.}"; \
	case "$$type" in \
	  patch) new="$$major.$$minor.$$((patch+1))" ;; \
	  minor) new="$$major.$$((minor+1)).0" ;; \
	  major) new="$$((major+1)).0.0" ;; \
	esac; \
	echo "bump $$cur -> $$new"; \
	$(MAKE) ver v="$$new"

.PHONY: compose-up
compose-up: ## Bring the installation up (server + dependencies)
	docker compose up -d --build

.PHONY: compose-down
compose-down: ## Tear the installation down, keep volumes
	docker compose down
