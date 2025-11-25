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
   - `storage`: optional persistence; set `path` (e.g., `/var/lib/kafka-bridge/cache.json`) and `flushInterval` to keep cached reference values across restarts.
   - `routes`: each route declares a single `sourceTopic`, destination topic, and per-reference-topic `matchFields` (field paths such as `fieldA` or `subObj.fieldB`) that are extracted from reference payloads; source payloads are matched if any cached value appears anywhere in the message.

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
storage:
  path: /var/lib/kafka-bridge/cache.json
  flushInterval: 10s
routes:
  - name: route-a
    sourceTopic: "source-topic-a"
    destinationTopic: filtered-topic-a
    referenceFeeds:
      - topic: reference-feed-topic-a
        matchFields: ["fieldA"]
      - topic: reference-feed-topic-b
        matchFields: ["subObj.fieldB"]
```

### Add reference payloads via HTTP

Run the service and POST a JSON array of strings to add reference values manually:

```bash
curl -X POST http://localhost:8080/reference/route-a \
  -H 'Content-Type: application/json' \
  -d '["value1","value2"]'
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

### Containerize

```bash
docker build -t your-registry/kafka-bridge:latest .
docker push your-registry/kafka-bridge:latest
```

The Dockerfile is multi-stage (static binary on distroless) and copies `config/config.example.yaml` to `/etc/kafka-bridge/config.yaml` by default. Override with your own config via a volume or `-config` argument.

### Deploy to Kubernetes

1. Edit `k8s/configmap.yaml` to reflect your clusters, topics, and TLS file paths.
2. Create a TLS secret with your PEM files (or use `k8s/secret-tls-example.yaml` as a template).
3. Apply manifests:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret-tls-example.yaml # replace with your real secret
kubectl apply -f k8s/deployment.yaml
```

The HTTP admin endpoint is exposed on port 8080 via the `Service` and accepts POSTs at `/reference/{routeId}` to inject reference payloads.

Persist the cache by mounting a writable volume to `storage.path` (see the example config map and deployment). An `emptyDir` works for single-pod lifetimes; use a PVC for reuse across restarts.
