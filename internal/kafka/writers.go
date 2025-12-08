package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// WriterPool lazily creates writers per topic and reuses them.
type WriterPool struct {
	mu      sync.Mutex
	writers map[string]*kafka.Writer
	brokers []string
	dialer  *kafka.Dialer
}

// NewWriterPool builds a writer pool for the provided brokers and dialer.
func NewWriterPool(brokers []string, dialer *kafka.Dialer) *WriterPool {
	return &WriterPool{
		brokers: brokers,
		dialer:  dialer,
		writers: make(map[string]*kafka.Writer),
	}
}

// Get returns a writer bound to the destination topic, ensuring the topic exists.
func (p *WriterPool) Get(topic string) (*kafka.Writer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if writer, ok := p.writers[topic]; ok {
		return writer, nil
	}

	if err := ensureTopicExists(p.brokers, p.dialer, topic); err != nil {
		return nil, err
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      p.brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: int(kafka.RequireAll),
		Async:        false,
		Dialer:       p.dialer,
	})

	p.writers[topic] = writer
	return writer, nil
}

// Close flushes and closes all managed writers.
func (p *WriterPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for topic, writer := range p.writers {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close writer %s: %w", topic, err)
		}
	}
	return firstErr
}

func ensureTopicExists(brokers []string, dialer *kafka.Dialer, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	ctrl, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	ctrlAddr := net.JoinHostPort(ctrl.Host, strconv.Itoa(ctrl.Port))
	ctrlConn, err := dialer.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer ctrlConn.Close()

	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     -1,
		ReplicationFactor: -1,
	})
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "exists") {
			return nil
		}
		return fmt.Errorf("create topic %s: %w", topic, err)
	}

	return nil
}
