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

	writerPool := kafkapkg.NewWriterPool(cfg.Brokers, cfg.ClientID)
	defer func() {
		if err := writerPool.Close(); err != nil {
			log.Printf("close writers: %v", err)
		}
	}()

	isnStore := store.NewISNStore()
	if cfg.ReferenceFeed.ResetState {
		isnStore.Reset()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runReferenceCollector(ctx, cfg.ReferenceFeed, isnStore); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("reference collector stopped: %v", err)
		}
	}()

	for _, route := range cfg.Routes {
		route := route
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := streamRoute(ctx, cfg, route, writerPool, isnStore); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("route %s stopped: %v", route.DisplayName(), err)
			}
		}()
	}

	wg.Wait()
}

func streamRoute(ctx context.Context, cfg *config.Config, route config.Route, writers *kafkapkg.WriterPool, isnStore *store.ISNStore) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        fmt.Sprintf("%s-%s", cfg.GroupID, slug(route.DisplayName())),
		GroupTopics:    route.SourceTopics,
		CommitInterval: cfg.CommitInterval,
	})
	defer reader.Close()

	log.Printf("route %s listening to %s -> %s", route.DisplayName(), strings.Join(route.SourceTopics, ","), route.DestinationTopic)
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		match, isnValue, err := matchesISN(m.Value, route.MatchValues, isnStore)
		if err != nil {
			log.Printf("route %s: skip invalid JSON: %v", route.DisplayName(), err)
			continue
		}
		if !match {
			continue
		}

		writer := writers.Get(route.DestinationTopic)
		if err := writer.WriteMessages(ctx, cloneMessage(m)); err != nil {
			log.Printf("route %s: write failed: %v", route.DisplayName(), err)
			continue
		}
		log.Printf("route %s forwarded message offset %d (isn=%s) to %s", route.DisplayName(), m.Offset, isnValue, route.DestinationTopic)
	}
}

func matchesISN(value []byte, allowed []string, isnStore *store.ISNStore) (bool, string, error) {
	if len(allowed) == 0 && isnStore == nil {
		return true, "", nil
	}

	isn, err := extractDataISN(value)
	if err != nil {
		return false, "", err
	}

	if len(allowed) > 0 {
		matched := false
		for _, candidate := range allowed {
			if candidate == isn {
				matched = true
				break
			}
		}
		if !matched {
			return false, isn, nil
		}
	}

	if isnStore != nil && !isnStore.Contains(isn) {
		return false, isn, nil
	}

	return true, isn, nil
}

func extractDataISN(value []byte) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal(value, &payload); err != nil {
		return "", err
	}

	data, ok := payload["data"].(map[string]any)
	if !ok {
		return "", errors.New("missing data object")
	}

	raw, ok := data["isn"]
	if !ok {
		return "", errors.New("data.isn not found")
	}

	return fmt.Sprintf("%v", raw), nil
}

func slug(in string) string {
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", ".", "-")
	return replacer.Replace(strings.ToLower(in))
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

func runReferenceCollector(ctx context.Context, feed config.ReferenceFeed, store *store.ISNStore) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        feed.Brokers,
		GroupID:        feed.GroupID,
		GroupTopics:    feed.Topics,
		CommitInterval: 5 * time.Second,
	})
	defer reader.Close()

	log.Printf("reference collector listening to %s", strings.Join(feed.Topics, ","))
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		isn, err := extractReferenceISN(msg.Value)
		if err != nil {
			log.Printf("reference collector: invalid JSON skipped: %v", err)
			continue
		}

		if added := store.Add(isn); added {
			log.Printf("reference collector: added isn=%s (total=%d)", isn, store.Size())
		}
	}
}

func extractReferenceISN(value []byte) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal(value, &payload); err != nil {
		return "", err
	}

	if raw, ok := payload["isn"]; ok {
		return fmt.Sprintf("%v", raw), nil
	}

	// fall back to nested structure for consistency
	if data, ok := payload["data"].(map[string]any); ok {
		if raw, ok := data["isn"]; ok {
			return fmt.Sprintf("%v", raw), nil
		}
	}

	return "", errors.New("isn not found")
}
