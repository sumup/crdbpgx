# Make this makefile self-documented with target `help`
.PHONY: help
.DEFAULT_GOAL := help
help: ## Show help
	@grep -Eh '^[0-9a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: workspace
workspace: ## Configure the local workspace for all modules
	@test -f go.work || go work init . ./crdbotel ./crdbprom
	@go work use . ./crdbotel ./crdbprom
	@go work edit -replace=github.com/sumup/crdbpgx@v1.1.0=.

.PHONY: fmt
fmt: workspace ## Format go files in all modules
	golangci-lint fmt --verbose ./... ./crdbotel/... ./crdbprom/...

.PHONY: lint
lint: workspace ## Lint go files in all modules
	golangci-lint run --verbose ./... ./crdbotel/... ./crdbprom/...

.PHONY: lint-fix
lint-fix: workspace ## Lint go files and apply auto-fixes
	golangci-lint run --verbose --fix ./... ./crdbotel/... ./crdbprom/...

.PHONY: test
test: workspace ## Run tests in all modules
	go test -v -race -timeout 1m ./... ./crdbotel/... ./crdbprom/...

.PHONY: test-integration
test-integration: ## Run integration tests against CRDB_TEST_DATABASE_URL
	go test -tags=integration -run '^TestIntegration' -v -race -count=1 -timeout 2m ./...

.PHONY: download
download: workspace
	@echo Download go.mod dependencies
	@go mod download

.PHONY: vulncheck
vulncheck: workspace ## Check for Vulnerabilities (make sure you have the tools install: `make install-tools`)
	govulncheck ./... ./crdbotel/... ./crdbprom/...

.PHONY: vulncheck-sarif
vulncheck-sarif: workspace ## Check for Vulnerabilities
	govulncheck -format=sarif ./... ./crdbotel/... ./crdbprom/... > govulncheck.sarif
	# Remove duplicate CVE tags and stacks emitted by govulncheck (golang/go#75890).
	jq '(.runs[]?.tool.driver.rules[]?.properties.tags | select(type == "array")) |= unique | (.runs[]?.results[]?.stacks | select(type == "array")) |= unique' govulncheck.sarif > govulncheck.sarif.tmp
	mv govulncheck.sarif.tmp govulncheck.sarif

.PHONY: generate
generate: # Generate latest SDK
	go-sdk-gen generate --mod github.com/sumup/sumup-go --pkg sumup --name SumUp ./openapi.json
	gomarkdoc --output DOCUMENTATION.md ./...

.PHONY: install-tools
install-tools: # Install development dependencies
	go install github.com/sumup/go-sdk-gen/cmd/go-sdk-gen@latest
	go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@latest
	go install golang.org/x/vuln/cmd/govulncheck@latest
