# Repository Guidelines

## Project Structure & Module Organization
The repo hosts a single Go service that filters Kafka traffic. Entrypoint code lives in `cmd/filter/`, reusable logic in `internal/` (`config` for YAML parsing + TLS helpers, `kafka` for writer pooling, `store` for cached match fingerprints). Runtime configuration sits under `config/` with `config.example.yaml` as the template. Add helper docs (like runbooks) under project root; keep binaries out of source control by writing them to `bin/` or `/tmp`.

## Build, Test, and Development Commands
- `go run ./cmd/filter -config config/config.yaml` – start the bridge locally; respects Ctrl+C/SIGTERM and exposes `http.listenAddr` for manual reference injection (POST an array of strings).
- `go build -o bin/kafka-filter ./cmd/filter` – produce a static binary for deployment.
- `go test ./...` – run future unit tests (currently none) and catch compile errors.
- `golangci-lint run` – optional but recommended; configure via `.golangci.yml` when added.
- `docker build -t your-registry/kafka-bridge:latest .` – build container image; use `k8s/` manifests for deployment. Mount a writable volume to `storage.path` to persist cached reference values.

## Coding Style & Naming Conventions
Use Go 1.21. Follow `gofmt` formatting and keep files ASCII. Package names stay lowercase and short (`store`, `kafka`). Public structs/functions need doc comments when exported outside a package. Topic, consumer-group, and config identifiers in examples should stay kebab-case (`bridge-reference`). Avoid global state; prefer context-aware functions for goroutines handling Kafka IO.

## Testing Guidelines
Place tests next to implementation files (e.g., `internal/store/store_test.go`). Use Go’s `testing` package with table-driven cases. Mock Kafka interactions using in-memory constructs or `kafka-go`’s `Conn` test helpers; never rely on production brokers. If a change cannot be automatically tested (e.g., requires a live cluster), describe the manual steps in PR notes.

## Commit & Pull Request Guidelines
Prefer Conventional Commits (`feat: add reference feed`). Keep subject lines ≤72 chars, wrap bodies at 100 chars, and mention Jira/GitHub IDs when relevant. PRs should summarize the scenario, list configs touched, and paste log excerpts demonstrating filtered traffic. Request review from another Go maintainer; merge only after green CI and at least one approval.

## Security & Configuration Tips
Store TLS artifacts (CA, client cert, key) outside the repo and point `sourceClusters[].tls` to their locations. Use environment variables or secret managers for broker credentials and avoid committing raw PEM files. When sharing sample configs, redact tenant info and reference Vault paths. If the reference feeds contain sensitive identifiers, keep logs on the lowest verbosity in shared environments.
