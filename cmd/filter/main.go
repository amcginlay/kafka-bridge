package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-bridge/internal/config"
	kafkapkg "kafka-bridge/internal/kafka"
	"kafka-bridge/internal/store"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config/config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sourceDialer, err := buildDialer(cfg.SourceCluster, cfg.ClientID)
	if err != nil {
		log.Fatalf("source dialer: %v", err)
	}
	bridgeDialer, err := buildDialer(cfg.BridgeCluster, cfg.ClientID)
	if err != nil {
		log.Fatalf("bridge dialer: %v", err)
	}

	writerPool := kafkapkg.NewWriterPool(cfg.BridgeCluster.Brokers, bridgeDialer)
	defer func() {
		if err := writerPool.Close(); err != nil {
			log.Printf("close writers: %v", err)
		}
	}()

	matchStore := store.NewMatchStore()

	var wg sync.WaitGroup
	for _, route := range cfg.Routes {
		route := route
		routeID := routeKey(route)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runReferenceCollector(ctx, cfg, route, bridgeDialer, matchStore, routeID); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reference collector %s stopped: %v", route.DisplayName(), err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := streamRoute(ctx, cfg, route, sourceDialer, writerPool, matchStore, routeID); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("route %s stopped: %v", route.DisplayName(), err)
			}
		}()
	}

	wg.Wait()
}

func buildDialer(cluster config.ClusterConfig, clientID string) (*kafka.Dialer, error) {
	tlsCfg, err := cluster.TLSConfigObject()
	if err != nil {
		return nil, err
	}
	return &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsCfg,
		ClientID:  clientID,
	}, nil
}

func streamRoute(ctx context.Context, cfg *config.Config, route config.Route, dialer *kafka.Dialer, writers *kafkapkg.WriterPool, store *store.MatchStore, routeID string) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.SourceCluster.Brokers,
		GroupID:        fmt.Sprintf("%s-%s", cfg.SourceGroupID, slug(route.DisplayName())),
		GroupTopics:    route.SourceTopics,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
		Dialer:         dialer,
	})
	defer reader.Close()

	log.Printf("route %s listening to source topics %s", route.DisplayName(), strings.Join(route.SourceTopics, ","))
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		fingerprint, err := fingerprintMessage(msg.Value, route.MatchFields)
		if err != nil {
			log.Printf("route %s: invalid payload skipped: %v", route.DisplayName(), err)
			continue
		}

		if !store.Contains(routeID, fingerprint) {
			continue
		}

		writer := writers.Get(route.DestinationTopic)
		if err := writer.WriteMessages(ctx, cloneMessage(msg)); err != nil {
			log.Printf("route %s: write failed: %v", route.DisplayName(), err)
			continue
		}
		log.Printf("route %s forwarded offset %d to %s", route.DisplayName(), msg.Offset, route.DestinationTopic)
	}
}

func runReferenceCollector(ctx context.Context, cfg *config.Config, route config.Route, dialer *kafka.Dialer, store *store.MatchStore, routeID string) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.BridgeCluster.Brokers,
		GroupID:        fmt.Sprintf("%s-%s", cfg.ReferenceGroupID, slug(route.DisplayName())),
		GroupTopics:    route.ReferenceTopics,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
		Dialer:         dialer,
	})
	defer reader.Close()

	log.Printf("reference collector %s listening to %s", route.DisplayName(), strings.Join(route.ReferenceTopics, ","))
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		fingerprint, err := fingerprintMessage(msg.Value, route.MatchFields)
		if err != nil {
			log.Printf("reference collector %s: invalid payload skipped: %v", route.DisplayName(), err)
			continue
		}

		if store.Add(routeID, fingerprint) {
			log.Printf("reference collector %s stored fingerprint (count=%d)", route.DisplayName(), store.Size(routeID))
		}
	}
}

func fingerprintMessage(value []byte, fields []string) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal(value, &payload); err != nil {
		return "", err
	}

	entries := make([]fieldValue, len(fields))
	for i, field := range fields {
		val, err := lookupField(payload, field)
		if err != nil {
			return "", err
		}
		entries[i] = fieldValue{Path: field, Value: val}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})

	data, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func lookupField(payload map[string]any, field string) (any, error) {
	parts := strings.Split(field, ".")
	switch len(parts) {
	case 1:
		if val, ok := payload[parts[0]]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("field %s not found", field)
	case 2:
		child, ok := payload[parts[0]].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %s missing nested object", field)
		}
		if val, ok := child[parts[1]]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("field %s not found", field)
	default:
		return nil, fmt.Errorf("field %s depth unsupported", field)
	}
}

type fieldValue struct {
	Path  string `json:"path"`
	Value any    `json:"value"`
}

func cloneMessage(m kafka.Message) kafka.Message {
	cloned := kafka.Message{
		Key:     append([]byte(nil), m.Key...),
		Value:   append([]byte(nil), m.Value...),
		Headers: make([]kafka.Header, len(m.Headers)),
		Time:    m.Time,
	}
	copy(cloned.Headers, m.Headers)
	return cloned
}

func slug(in string) string {
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", ".", "-")
	return replacer.Replace(strings.ToLower(in))
}

func routeKey(route config.Route) string {
	return slug(route.DisplayName())
}
