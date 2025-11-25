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
	SourceCluster    ClusterConfig `yaml:"sourceCluster"`
	BridgeCluster    ClusterConfig `yaml:"bridgeCluster"`
	ClientID         string        `yaml:"clientId"`
	SourceGroupID    string        `yaml:"sourceGroupId"`
	ReferenceGroupID string        `yaml:"referenceGroupId"`
	CommitInterval   time.Duration `yaml:"commitInterval"`
	Routes           []Route       `yaml:"routes"`
	HTTP             HTTPServer    `yaml:"http"`
}

// ClusterConfig holds broker and TLS settings.
type ClusterConfig struct {
	Brokers []string   `yaml:"brokers"`
	TLS     *TLSConfig `yaml:"tls"`
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
	Name             string   `yaml:"name"`
	SourceTopics     []string `yaml:"sourceTopics"`
	DestinationTopic string   `yaml:"destinationTopic"`
	ReferenceTopics  []string `yaml:"referenceTopics"`
	MatchFields      []string `yaml:"matchFields"`
}

// HTTPServer configures the optional admin HTTP listener.
type HTTPServer struct {
	ListenAddr string `yaml:"listenAddr"`
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
	if err := c.SourceCluster.validate(); err != nil {
		return fmt.Errorf("sourceCluster: %w", err)
	}
	if err := c.BridgeCluster.validate(); err != nil {
		return fmt.Errorf("bridgeCluster: %w", err)
	}
	if c.ClientID == "" {
		return errors.New("clientId is required")
	}
	if c.SourceGroupID == "" {
		return errors.New("sourceGroupId is required")
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
	}
	if c.HTTP.ListenAddr == "" {
		c.HTTP.ListenAddr = ":8080"
	}
	return nil
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
	if len(r.SourceTopics) == 0 {
		return fmt.Errorf("route %d: sourceTopics cannot be empty", idx)
	}
	if r.DestinationTopic == "" {
		return fmt.Errorf("route %d: destinationTopic is required", idx)
	}
	if len(r.ReferenceTopics) == 0 {
		return fmt.Errorf("route %d: referenceTopics cannot be empty", idx)
	}
	if len(r.MatchFields) == 0 {
		return fmt.Errorf("route %d: matchFields cannot be empty", idx)
	}
	for _, field := range r.MatchFields {
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
