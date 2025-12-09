package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultCommitInterval = 5 * time.Second

// Config captures all runtime settings.
type Config struct {
	SourceClusters   []SourceCluster `yaml:"sourceClusters"`
	BridgeCluster    ClusterConfig   `yaml:"bridgeCluster"`
	ClientID         string          `yaml:"clientId"`
	ReferenceGroupID string          `yaml:"referenceGroupId"`
	CommitInterval   time.Duration   `yaml:"commitInterval"`
	Routes           []Route         `yaml:"routes"`
	HTTP             HTTPServer      `yaml:"http"`
	Storage          Storage         `yaml:"storage"`
}

// ClusterConfig holds broker and TLS settings.
type ClusterConfig struct {
	Brokers []string   `yaml:"brokers"`
	TLS     *TLSConfig `yaml:"tls"`
}

// SourceCluster ties a cluster configuration to a unique name for routing.
type SourceCluster struct {
	Name          string     `yaml:"name"`
	Brokers       []string   `yaml:"brokers"`
	SourceGroupID string     `yaml:"sourceGroupId"`
	TLS           *TLSConfig `yaml:"tls"`
}

// TLSConfig describes certificates required for TLS/mTLS.
type TLSConfig struct {
	CAFile             string `yaml:"caFile"`
	CertFile           string `yaml:"certFile"`
	KeyFile            string `yaml:"keyFile"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
}

// Route maps one or more source topics to a destination topic with reference feeds.
type Route struct {
	Name             string          `yaml:"name"`
	SourceCluster    string          `yaml:"sourceCluster"`
	SourceTopic      string          `yaml:"sourceTopic"`
	DestinationTopic string          `yaml:"destinationTopic"`
	ReferenceFeeds   []ReferenceFeed `yaml:"referenceFeeds"`
}

// HTTPServer configures the optional admin HTTP listener.
type HTTPServer struct {
	ListenAddr string `yaml:"listenAddr"`
}

// ReferenceFeed describes per-topic extraction rules.
type ReferenceFeed struct {
	Topic        string   `yaml:"topic"`
	TopicHeaders []string `yaml:"topicHeaders"`
	MatchFields  []string `yaml:"matchFields"`
}

// Storage configures optional on-disk persistence for cached values.
type Storage struct {
	Path          string        `yaml:"path"`
	FlushInterval time.Duration `yaml:"flushInterval"`
}

// Load parses the YAML configuration.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.CommitInterval == 0 {
		cfg.CommitInterval = defaultCommitInterval
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate ensures all required fields are populated.
func (c *Config) Validate() error {
	if len(c.SourceClusters) == 0 {
		return errors.New("at least one sourceCluster must be defined")
	}
	sourceClusterNames := make(map[string]struct{}, len(c.SourceClusters))
	for i, sc := range c.SourceClusters {
		if err := sc.validate(); err != nil {
			return fmt.Errorf("sourceCluster %d: %w", i, err)
		}
		if _, exists := sourceClusterNames[sc.Name]; exists {
			return fmt.Errorf("sourceCluster %d: duplicate name %q", i, sc.Name)
		}
		sourceClusterNames[sc.Name] = struct{}{}
	}
	if err := c.BridgeCluster.validate(); err != nil {
		return fmt.Errorf("bridgeCluster: %w", err)
	}
	if c.ClientID == "" {
		return errors.New("clientId is required")
	}
	if c.ReferenceGroupID == "" {
		return errors.New("referenceGroupId is required")
	}
	if len(c.Routes) == 0 {
		return errors.New("at least one route must be defined")
	}
	for i := range c.Routes {
		if err := c.Routes[i].validate(i); err != nil {
			return err
		}
		if _, ok := sourceClusterNames[c.Routes[i].SourceCluster]; !ok {
			return fmt.Errorf("route %d: sourceCluster %q not found", i, c.Routes[i].SourceCluster)
		}
	}
	if c.HTTP.ListenAddr == "" {
		c.HTTP.ListenAddr = ":8080"
	}
	if c.Storage.FlushInterval == 0 {
		c.Storage.FlushInterval = 10 * time.Second
	}
	return nil
}

// SourceClusterByName returns a configured source cluster by name.
func (c *Config) SourceClusterByName(name string) (SourceCluster, bool) {
	for _, sc := range c.SourceClusters {
		if sc.Name == name {
			return sc, true
		}
	}
	return SourceCluster{}, false
}

func (c ClusterConfig) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers cannot be empty")
	}
	if c.TLS != nil {
		if err := c.TLS.validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s SourceCluster) validate() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	if s.SourceGroupID == "" {
		return errors.New("sourceGroupId is required")
	}
	cfg := ClusterConfig{
		Brokers: s.Brokers,
		TLS:     s.TLS,
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	return nil
}

func (s SourceCluster) ClusterConfig() ClusterConfig {
	return ClusterConfig{
		Brokers: s.Brokers,
		TLS:     s.TLS,
	}
}

func (t *TLSConfig) validate() error {
	if t == nil {
		return nil
	}
	if t.CertFile != "" && t.KeyFile == "" {
		return errors.New("keyFile required when certFile is set")
	}
	if t.KeyFile != "" && t.CertFile == "" {
		return errors.New("certFile required when keyFile is set")
	}
	return nil
}

func (r *Route) validate(idx int) error {
	if r.SourceCluster == "" {
		return fmt.Errorf("route %d: sourceCluster is required", idx)
	}
	if r.SourceTopic == "" {
		return fmt.Errorf("route %d: sourceTopic cannot be empty", idx)
	}
	if r.DestinationTopic == "" {
		return fmt.Errorf("route %d: destinationTopic is required", idx)
	}
	if len(r.ReferenceFeeds) == 0 {
		return fmt.Errorf("route %d: referenceFeeds cannot be empty", idx)
	}
	for fi, feed := range r.ReferenceFeeds {
		if feed.Topic == "" {
			return fmt.Errorf("route %d: reference feed %d topic is required", idx, fi)
		}
		for _, th := range feed.TopicHeaders {
			if !strings.Contains(th, "=") {
				return fmt.Errorf("route %d: reference feed %s topicHeaders entry %q must be key=value", idx, feed.Topic, th)
			}
		}
		if len(feed.MatchFields) == 0 {
			return fmt.Errorf("route %d: reference feed %s matchFields cannot be empty", idx, feed.Topic)
		}
		for _, field := range feed.MatchFields {
			parts := strings.Split(field, ".")
			if len(parts) == 0 || len(parts) > 2 {
				return fmt.Errorf("route %d: match field %q must be 'field' or 'parent.child'", idx, field)
			}
			for _, part := range parts {
				if part == "" {
					return fmt.Errorf("route %d: match field %q is invalid", idx, field)
				}
			}
		}
	}
	return nil
}

// DisplayName returns an identifier for logs.
func (r Route) DisplayName() string {
	if r.Name != "" {
		return r.Name
	}
	return r.DestinationTopic
}

// TLSConfigObject builds a tls.Config for the cluster.
func (c ClusterConfig) TLSConfigObject() (*tls.Config, error) {
	if c.TLS == nil {
		return nil, nil
	}
	return c.TLS.tlsConfig()
}

func (t *TLSConfig) tlsConfig() (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: t.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if t.CAFile != "" {
		caData, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(caData); !ok {
			return nil, errors.New("append ca certs failed")
		}
		tlsConfig.RootCAs = pool
	}

	return tlsConfig, nil
}
