.PHONY: help build test clean run lint proto stress bench docker docker-backend \
        cluster cluster-down deploy demo undeploy fmt vet check race deps

# Variables
BINARY_NAME=collapser
BUILD_DIR=bin
MAIN_PATH=./cmd/proxy/main.go
BACKEND_PATH=./cmd/backend/main.go
CLIENT_PATH=./cmd/client/main.go

# Docker variables
DOCKER_IMAGE=collapser-proxy
BACKEND_IMAGE=collapser-backend
DOCKER_TAG=latest

# Cluster variables
KIND_CLUSTER=collapser

# Default target
help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Build Targets
# ============================================================================

build: ## Build the proxy binary
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)

run: build ## Build and run the proxy
	@echo "Running $(BINARY_NAME)..."
	@$(BUILD_DIR)/$(BINARY_NAME)

# ============================================================================
# Codegen
# ============================================================================

proto: ## Regenerate protobuf stubs (requires protoc, protoc-gen-go, protoc-gen-go-grpc)
	@echo "Generating protobuf stubs..."
	@protoc \
		--go_out=. --go_opt=module=github.com/VarunGitGood/collapser-grpc \
		--go-grpc_out=. --go-grpc_opt=module=github.com/VarunGitGood/collapser-grpc \
		proto/hello.proto
	@echo "Generated stubs are committed, so a fresh clone builds without protoc."

# ============================================================================
# Test Targets
# ============================================================================

test: ## Run unit tests
	@echo "Running unit tests..."
	@go test -v -race -coverprofile=coverage.out ./...

test-short: ## Run short tests only
	@echo "Running short tests..."
	@go test -v -short -race ./...

stress: ## Run stress tests
	@echo "Running stress tests..."
	@go test -v -run=TestStress -timeout=30m ./internal/collapser/

bench: ## Run benchmarks
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -run=^$$ ./...

bench-cpu: ## Run benchmarks with CPU profiling
	@echo "Running benchmarks with CPU profiling..."
	@go test -bench=. -benchmem -cpuprofile=cpu.prof -run=^$$ ./...
	@echo "Analyze with: go tool pprof cpu.prof"

bench-mem: ## Run benchmarks with memory profiling
	@echo "Running benchmarks with memory profiling..."
	@go test -bench=. -benchmem -memprofile=mem.prof -run=^$$ ./...
	@echo "Analyze with: go tool pprof mem.prof"

bench-compare: ## Run benchmarks and save results for comparison
	@echo "Running benchmarks and saving to bench.txt..."
	@go test -bench=. -benchmem -run=^$$ ./... | tee bench.txt

race: ## Run tests with race detector
	@echo "Running race detector..."
	@go test -race -short ./...

soak-test: ## Run 1-hour soak test
	@echo "Running 1-hour soak test..."
	@go test -v -run=TestStress_SustainedLoad -timeout=70m ./internal/collapser/

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@rm -f cpu.prof mem.prof
	@rm -f bench.txt

lint: ## Run linter
	@echo "Running linter..."
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed. Install it from https://golangci-lint.run/usage/install/"; \
		echo "Or run: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin"; \
	fi

fmt: ## Format code
	@echo "Formatting code..."
	@go fmt ./...

vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

check: fmt vet lint test ## Run all checks (fmt, vet, lint, test)

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

deps-upgrade: ## Upgrade dependencies
	@echo "Upgrading dependencies..."
	@go get -u ./...
	@go mod tidy

deps-vendor: ## Vendor dependencies
	@echo "Vendoring dependencies..."
	@go mod vendor

# ============================================================================
# Container & Cluster Targets
# ============================================================================

docker: ## Build the proxy and demo backend images
	@docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) --target proxy .
	@docker build -t $(BACKEND_IMAGE):$(DOCKER_TAG) --target backend .

cluster: ## Create a local kind cluster with Istio installed
	@./deploy/scripts/cluster-up.sh

deploy: docker ## Load images into kind and apply the k8s + Istio manifests
	@kind load docker-image $(DOCKER_IMAGE):$(DOCKER_TAG) --name $(KIND_CLUSTER)
	@kind load docker-image $(BACKEND_IMAGE):$(DOCKER_TAG) --name $(KIND_CLUSTER)
	@kubectl apply -f deploy/k8s/
	@kubectl apply -f deploy/istio/
	@kubectl rollout status deploy/collapser-proxy --timeout=180s
	@kubectl rollout status deploy/hello-backend --timeout=180s

demo: ## Drive load through the deployed proxy and print the collapse ratio
	@./deploy/scripts/demo.sh

undeploy: ## Remove the workloads, keep the cluster
	@kubectl delete -f deploy/istio/ --ignore-not-found
	@kubectl delete -f deploy/k8s/ --ignore-not-found

cluster-down: ## Delete the local kind cluster
	@kind delete cluster --name $(KIND_CLUSTER)

.DEFAULT_GOAL := help