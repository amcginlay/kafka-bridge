package store

import "sync"

// ISNStore keeps the latest ISN values seen on the reference feed.
type ISNStore struct {
	mu     sync.RWMutex
	values map[string]struct{}
}

// NewISNStore builds an empty store.
func NewISNStore() *ISNStore {
	return &ISNStore{
		values: make(map[string]struct{}),
	}
}

// Add records the provided identifier and returns true when it was newly inserted.
func (s *ISNStore) Add(isn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.values[isn]; exists {
		return false
	}
	s.values[isn] = struct{}{}
	return true
}

// Contains reports whether the identifier has been observed.
func (s *ISNStore) Contains(isn string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.values[isn]
	return ok
}

// Reset clears the store.
func (s *ISNStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = make(map[string]struct{})
}

// Size returns the current number of stored identifiers.
func (s *ISNStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.values)
}
