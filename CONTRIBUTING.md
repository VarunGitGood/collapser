# Contributing to Collapser

Collapser is a Go gRPC request-deduplication sidecar with a local Docker/kind
demonstration environment.

## Development setup

Install Go 1.25.5 or later, then clone the project and download its modules:

```bash
git clone https://github.com/VarunGitGood/collapser.git
cd collapser
make deps
```

For the local Kubernetes demonstration, also install Docker, kind and kubectl.
`make cluster` creates the cluster in Docker; `make deploy` builds the local
images, loads them into kind and applies the sidecar manifests.

## Building

```bash
make build
```

## Running Tests

```bash
make test
```

## Code Style

- Run `make fmt` to format your code
- Run `make vet` to check for common issues
- Run `make lint` to run the linter (requires golangci-lint)

## Project structure

```
.
├── cmd/           # Main applications
├── internal/      # Deduplication engine, gRPC proxy and observability
├── proto/         # Demo service definition and committed generated stubs
├── deploy/        # kind, Kubernetes and measurement artifacts
├── site/          # Static GitHub Pages project page
└── Makefile       # Build and test commands
```

## Pull Request Process

1. Fork the repository
2. Create your feature branch
3. Make your changes
4. Write or update tests
5. Ensure all tests pass
6. Submit a pull request
