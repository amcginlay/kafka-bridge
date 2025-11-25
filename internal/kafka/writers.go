package kafka

import (
	"fmt"
	"sync"

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

// Get returns a writer bound to the destination topic.
func (p *WriterPool) Get(topic string) *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()

	if writer, ok := p.writers[topic]; ok {
		return writer
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
	return writer
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
