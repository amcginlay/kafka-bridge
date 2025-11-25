## Kafka Topic Filter Tool

This Go utility consumes JSON payloads from one Kafka cluster (secured with mTLS), listens to reference topics and writes to destination topics on a second cluster, and forwards a source message only when its field values match any profile captured from the reference feeds.

### Requirements

- Go 1.21+
- Two reachable Kafka clusters (source + bridge/reference)
- Client certificates for the source cluster (PEM files)

### Configure

1. Copy `config/config.example.yaml` to `config/config.yaml`.
2. Adjust the file:
   - `sourceCluster`: brokers plus TLS certs/keys for the mTLS-protected cluster hosting the source topics.
   - `bridgeCluster`: brokers (and optional TLS) for the cluster hosting reference feeds and destination topics.
   - `clientId`, `sourceGroupId`, `referenceGroupId`: identifiers reused across consumers and producers.
   - `http`: optional admin server, `listenAddr` defaults to `:8080`. POST reference payloads here instead of (or in addition to) consuming them from reference topics.
   - `routes`: each route declares source topics, destination topic, the reference feed topics tied to that source, and `matchFields` (field paths such as `fieldA` or `subObj.fieldB`) that are extracted from both reference and source payloads for matching.

Example snippet:

```yaml
sourceCluster:
  brokers: ["source-cluster:9094"]
  tls:
    caFile: path/to/ca.pem
    certFile: path/to/client-cert.pem
    keyFile: path/to/client-key.pem
bridgeCluster:
  brokers: ["bridge-cluster:9092"]
clientId: kafka-filter
sourceGroupId: filter-source
referenceGroupId: filter-reference
routes:
  - name: route-a
    sourceTopics: ["source-topic-a"]
    destinationTopic: filtered-topic-a
    referenceTopics: ["reference-feed-topic-a", "reference-feed-topic-b"]
    matchFields: ["fieldA", "subObj.fieldB"]
```

### Add reference payloads via HTTP

Run the service and POST a JSON body (same shape as reference feed messages) to add a fingerprint:

```bash
curl -X POST http://localhost:8080/reference/route-a \
  -H 'Content-Type: application/json' \
  -d '{"fieldA":"value1","subObj":{"fieldB":"value2"}}'
```

The route key matches the `name` (or falls back to `destinationTopic`, lowercased and slugged).

### Run

```bash
go run ./cmd/filter -config config/config.yaml
```

Logs indicate which reference collector stored fingerprints and which routes forwarded messages. All consumers start from the latest offsets and honor Ctrl+C/SIGTERM for graceful shutdowns.

### Build

```bash
go build -o bin/kafka-filter ./cmd/filter
```

Ship the resulting binary plus `config.yaml` with your deployment tooling. Run `go test ./...` to execute future unit tests once the local Go toolchain is fixed.
