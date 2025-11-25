## Kafka Topic Filter Tool

This Go utility consumes JSON payloads from one or more Kafka topics, inspects the `data.isn` field, and forwards matching records to configured destination topics. It also subscribes to a secondary broker to ingest authoritative `isn` values and uses that live cache to decide which messages should be forwarded.

### Requirements

- Go 1.21+
- Access to the Kafka brokers you want to bridge

### Configure

1. Copy `config/config.example.yaml` to `config/config.yaml`.
2. Adjust the file:
   - `brokers`: list of Kafka bootstrap servers (e.g., `localhost:9092`).
   - `groupId` / `clientId`: identifiers for the consumer group and producer client.
   - `referenceFeed`: connection details for the broker that provides valid `isn` values. Supply `brokers`, `groupId`, and `topics`. Set `resetState: true` to clear any previously cached identifiers on startup.
   - `routes`: each block declares `sourceTopics`, a `destinationTopic`, and optional `matchValues` (static allowlisted `data.isn` entries). Every message is additionally filtered against the cached `isn` list from the reference feed.

Example snippet:

```yaml
referenceFeed:
  brokers: ["other-broker:9093"]
  groupId: bridge-reference
  topics: ["reference-topic"]
routes:
  - name: test route
    sourceTopics: ["test-topic"]
    destinationTopic: filtered-test-topic
    matchValues: ["alpha", "beta"]
```

### Run

```bash
go run ./cmd/filter -config config/config.yaml
```

The process listens for `SIGINT`/`SIGTERM` and shuts down gracefully. Logs show which route forwarded which offsets, making it easy to verify matches.

### Build

```bash
go build -o bin/kafka-filter ./cmd/filter
```

Ship the resulting binary alongside your `config.yaml` for deployments (e.g., Docker or systemd). Use `go test ./...` to execute unit tests whenever they are added.
