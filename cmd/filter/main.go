package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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

	sourceDialers := make(map[string]*kafka.Dialer, len(cfg.SourceClusters))
	for _, sc := range cfg.SourceClusters {
		dialer, err := buildDialer(sc.ClusterConfig(), cfg.ClientID)
		if err != nil {
			log.Fatalf("source dialer %s: %v", sc.Name, err)
		}
		sourceDialers[sc.Name] = dialer
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

		sourceCluster, ok := cfg.SourceClusterByName(route.SourceCluster)
		if !ok {
			log.Fatalf("route %s references unknown sourceCluster %s", route.DisplayName(), route.SourceCluster)
		}
		sourceDialer, ok := sourceDialers[sourceCluster.Name]
		if !ok {
			log.Fatalf("source dialer missing for %s", sourceCluster.Name)
		}

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
			if err := streamRoute(ctx, cfg, route, sourceCluster, sourceDialer, writerPool, matcher); err != nil && !errors.Is(err, context.Canceled) {
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

func streamRoute(ctx context.Context, cfg *config.Config, route config.Route, sourceCluster config.SourceCluster, dialer *kafka.Dialer, writers *kafkapkg.WriterPool, matcher *engine.Matcher) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        sourceCluster.Brokers,
		GroupID:        fmt.Sprintf("%s-%s", sourceCluster.SourceGroupID, slug(route.DisplayName())),
		GroupTopics:    []string{route.SourceTopic},
		CommitInterval: cfg.CommitInterval,
		StartOffset:    kafka.LastOffset,
		Dialer:         dialer,
	})
	defer reader.Close()

	log.Printf("route %s listening to source topic %s", route.DisplayName(), route.SourceTopic)
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

		writer, err := writers.Get(route.DestinationTopic)
		if err != nil {
			log.Printf("route %s: ensure topic %s failed: %v", route.DisplayName(), route.DestinationTopic, err)
			continue
		}

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
	mux.HandleFunc("/referenceAllRoutes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var values []string
		if err := json.NewDecoder(r.Body).Decode(&values); err != nil {
			http.Error(w, "invalid JSON array of strings", http.StatusBadRequest)
			return
		}
		if len(values) == 0 {
			http.Error(w, "empty payload", http.StatusBadRequest)
			return
		}

		added := false
		for _, matcher := range matchers {
			if matcher.AddValues(values) {
				added = true
			}
		}
		status := http.StatusOK
		if added {
			status = http.StatusCreated
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("ok\n"))
	})
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
		matcher, ok := matchers[routeID]
		if !ok {
			http.Error(w, "route not found", http.StatusNotFound)
			return
		}
		defer r.Body.Close()
		var values []string
		if err := json.NewDecoder(r.Body).Decode(&values); err != nil {
			http.Error(w, "invalid JSON array of strings", http.StatusBadRequest)
			return
		}
		if len(values) == 0 {
			http.Error(w, "empty payload", http.StatusBadRequest)
			return
		}
		added := matcher.AddValues(values)
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
