package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultCommitInterval = 5 * time.Second

// Config describes the runtime configuration of the filtering tool.
type Config struct {
	Brokers        []string      `yaml:"brokers"`
	GroupID        string        `yaml:"groupId"`
	ClientID       string        `yaml:"clientId"`
	CommitInterval time.Duration `yaml:"commitInterval"`
	Routes         []Route       `yaml:"routes"`
	ReferenceFeed  ReferenceFeed `yaml:"referenceFeed"`
}

// Route wires messages from one or more source topics to a destination topic.
type Route struct {
	Name             string   `yaml:"name"`
	SourceTopics     []string `yaml:"sourceTopics"`
	DestinationTopic string   `yaml:"destinationTopic"`
	MatchValues      []string `yaml:"matchValues"`
}

// ReferenceFeed describes the broker/topics that populate the allowed ISN set.
type ReferenceFeed struct {
	Brokers    []string `yaml:"brokers"`
	GroupID    string   `yaml:"groupId"`
	ClientID   string   `yaml:"clientId"`
	Topics     []string `yaml:"topics"`
	ResetState bool     `yaml:"resetState"`
}

// Load reads and validates the configuration file.
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

// Validate ensures the config has the required fields populated.
func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers list cannot be empty")
	}
	if c.GroupID == "" {
		return errors.New("groupId is required")
	}
	if len(c.Routes) == 0 {
		return errors.New("at least one route must be defined")
	}

	for i := range c.Routes {
		if err := c.Routes[i].validate(i); err != nil {
			return err
		}
	}

	if err := c.ReferenceFeed.validate(); err != nil {
		return err
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
	return nil
}

func (r ReferenceFeed) validate() error {
	if len(r.Brokers) == 0 {
		return errors.New("referenceFeed.brokers cannot be empty")
	}
	if r.GroupID == "" {
		return errors.New("referenceFeed.groupId is required")
	}
	if len(r.Topics) == 0 {
		return errors.New("referenceFeed.topics cannot be empty")
	}
	return nil
}

// DisplayName returns a human-readable identifier for log lines.
func (r Route) DisplayName() string {
	if r.Name != "" {
		return r.Name
	}
	return r.DestinationTopic
}
