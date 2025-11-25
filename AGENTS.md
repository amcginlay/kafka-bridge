# Repository Guidelines

## Project Structure & Module Organization
The repo hosts a single Go service that filters Kafka traffic. Entrypoint code lives in `cmd/filter/`, reusable logic in `internal/` (`config` for YAML parsing, `kafka` for writer pooling, `store` for cached ISNs). Runtime configuration sits under `config/` with `config.example.yaml` as the template. Add helper docs (like runbooks) under project root; keep binaries out of source control by writing them to `bin/` or `/tmp`.

## Build, Test, and Development Commands
- `go run ./cmd/filter -config config/config.yaml` – start the bridge locally; respects Ctrl+C/SIGTERM.
- `go build -o bin/kafka-filter ./cmd/filter` – produce a static binary for deployment.
- `go test ./...` – run future unit tests (currently none) and catch compile errors.
- `golangci-lint run` – optional but recommended; configure via `.golangci.yml` when added.

## Coding Style & Naming Conventions
Use Go 1.21. Follow `gofmt` formatting and keep files ASCII. Package names stay lowercase and short (`store`, `kafka`). Public structs/functions need doc comments when exported outside a package. Topic, consumer-group, and config identifiers in examples should stay kebab-case (`bridge-reference`). Avoid global state; prefer context-aware functions for goroutines handling Kafka IO.

## Testing Guidelines
Place tests next to implementation files (e.g., `internal/store/store_test.go`). Use Go’s `testing` package with table-driven cases. Mock Kafka interactions using in-memory constructs or `kafka-go`’s `Conn` test helpers; never rely on production brokers. If a change cannot be automatically tested (e.g., requires a live cluster), describe the manual steps in PR notes.

## Commit & Pull Request Guidelines
Prefer Conventional Commits (`feat: add reference feed`). Keep subject lines ≤72 chars, wrap bodies at 100 chars, and mention Jira/GitHub IDs when relevant. PRs should summarize the scenario, list configs touched, and paste log excerpts demonstrating filtered traffic. Request review from another Go maintainer; merge only after green CI and at least one approval.

## Security & Configuration Tips
Store secrets (broker creds, SASL configs) in environment variables or `.env.local` (gitignored). `config/config.yaml` should reference Vault/secret managers rather than inline passwords. When sharing sample configs, redact any tenant info. If the reference feed contains sensitive identifiers, ensure downstream logs obfuscate them before exporting to shared systems.
