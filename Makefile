.DEFAULT_GOAL := help

.PHONY: configure
configure: ## Set up a working environment from scratch (tools go to bin/)
	@echo "Nothing to configure yet — bootstrap skeleton."

.PHONY: help
help: ## List all targets with explanations
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
