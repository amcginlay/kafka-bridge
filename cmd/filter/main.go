package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-bridge/internal/config"
	"kafka-bridge/internal/engine"
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
	matchers := make(map[string]*engine.Matcher)
	for _, route := range cfg.Routes {
		routeID := routeKey(route)
		matchers[routeID] = engine.NewMatcher(routeID, route.ReferenceFeeds, matchStore)
	}

	if cfg.Storage.Path != "" {
		if err := loadSnapshot(cfg.Storage.Path, matchStore); err != nil {
			log.Printf("warn: failed to load snapshot: %v", err)
		}
		startSnapshotWriter(ctx, cfg.Storage.Path, cfg.Storage.FlushInterval, matchStore)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := startHTTPServer(ctx, cfg.HTTP.ListenAddr, matchers); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("http server stopped: %v", err)
		}
	}()

	for _, route := range cfg.Routes {
		route := route
		routeID := routeKey(route)
		matcher := matchers[routeID]

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runReferenceCollector(ctx, cfg, route, bridgeDialer, matcher); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reference collector %s stopped: %v", route.DisplayName(), err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := streamRoute(ctx, cfg, route, sourceDialer, writerPool, matcher); err != nil && !errors.Is(err, context.Canceled) {
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

func streamRoute(ctx context.Context, cfg *config.Config, route config.Route, dialer *kafka.Dialer, writers *kafkapkg.WriterPool, matcher *engine.Matcher) error {
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

		match, err := matcher.ShouldForward(msg.Value)
		if err != nil {
			log.Printf("route %s: invalid payload skipped: %v", route.DisplayName(), err)
			continue
		}
		if !match {
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

func runReferenceCollector(ctx context.Context, cfg *config.Config, route config.Route, dialer *kafka.Dialer, matcher *engine.Matcher) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.BridgeCluster.Brokers,
		GroupID:        fmt.Sprintf("%s-%s", cfg.ReferenceGroupID, slug(route.DisplayName())),
		GroupTopics:    referenceTopics(route.ReferenceFeeds),
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
		Dialer:         dialer,
	})
	defer reader.Close()

	log.Printf("reference collector %s listening to %s", route.DisplayName(), strings.Join(referenceTopics(route.ReferenceFeeds), ","))
	log.Printf("reference collector %s listening to %s", route.DisplayName(), strings.Join(referenceTopics(route.ReferenceFeeds), ","))
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		added, err := matcher.ProcessReference(msg.Topic, msg.Value)
		if err != nil {
			log.Printf("reference collector %s: invalid payload skipped: %v", route.DisplayName(), err)
			continue
		}

		if added {
			log.Printf("reference collector %s stored fingerprint (count=%d)", route.DisplayName(), matcher.Size())
		}
	}
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

func referenceTopics(feeds []config.ReferenceFeed) []string {
	out := make([]string, 0, len(feeds))
	for _, f := range feeds {
		out = append(out, f.Topic)
	}
	return out
}

func loadSnapshot(path string, store *store.MatchStore) error {
	snap, err := store.LoadSnapshot(path)
	if err != nil {
		return err
	}
	store.Load(snap)
	return nil
}

func startSnapshotWriter(ctx context.Context, path string, interval time.Duration, store *store.MatchStore) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = store.SaveSnapshot(path)
				return
			case <-ticker.C:
				if err := store.SaveSnapshot(path); err != nil {
					log.Printf("warn: snapshot save failed: %v", err)
				}
			}
		}
	}()
}

func startHTTPServer(ctx context.Context, addr string, matchers map[string]*engine.Matcher) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/reference/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		routeID := strings.TrimPrefix(r.URL.Path, "/reference/")
		if routeID == "" {
			http.Error(w, "route id required", http.StatusBadRequest)
			return
		}
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "topic query param required", http.StatusBadRequest)
			return
		}
		matcher, ok := matchers[routeID]
		if !ok {
			http.Error(w, "route not found", http.StatusNotFound)
			return
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
		added, err := matcher.ProcessReference(topic, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid payload: %v", err), http.StatusBadRequest)
			return
		}
		status := http.StatusOK
		if added {
			status = http.StatusCreated
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("ok\n"))
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("http server listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return ctx.Err()
}
