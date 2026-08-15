# Make this makefile self-documented with target `help`
.PHONY: help
.DEFAULT_GOAL := help
help: ## Show help
	@grep -Eh '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: fmt
fmt: ## Format go files
	golangci-lint fmt --verbose ./... ./crdbotel/... ./crdbprom/...

.PHONY: lint
lint: ## Lint go files
	golangci-lint run --verbose ./... ./crdbotel/... ./crdbprom/...

.PHONY: lint-fix
lint-fix: ## Lint go files and apply auto-fixes
	golangci-lint run --verbose --fix ./... ./crdbotel/... ./crdbprom/...

.PHONY: test
test: ## Run tests
	go test -v -failfast -race -timeout 10m ./... ./crdbotel/... ./crdbprom/...

.PHONY: download
download:
	@echo Download go.mod dependencies
	@go mod download
	@go -C crdbotel mod download
	@go -C crdbprom mod download

.PHONY: vulncheck
vulncheck: ## Check for Vulnerabilities (make sure you have the tools install: `make install-tools`)
	govulncheck ./... ./crdbotel/... ./crdbprom/...

.PHONY: vulncheck-sarif
vulncheck-sarif: ## Check for Vulnerabilities
	govulncheck -format=sarif ./... ./crdbotel/... ./crdbprom/... > govulncheck.sarif

.PHONY: generate
generate: # Generate latest SDK
	go-sdk-gen generate --mod github.com/sumup/sumup-go --pkg sumup --name SumUp ./openapi.json
	gomarkdoc --output DOCUMENTATION.md ./...

.PHONY: install-tools
install-tools: # Install development dependencies
	go install github.com/sumup/go-sdk-gen/cmd/go-sdk-gen@latest
	go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
