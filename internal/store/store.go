package store

import "sync"

// MatchStore keeps allowed payload fingerprints per route.
type MatchStore struct {
	mu     sync.RWMutex
	values map[string]map[string]struct{}
}

// NewMatchStore creates an empty store.
func NewMatchStore() *MatchStore {
	return &MatchStore{values: make(map[string]map[string]struct{})}
}

// Add inserts the fingerprint for the given route.
func (s *MatchStore) Add(route string, fingerprint string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	routeMap, ok := s.values[route]
	if !ok {
		routeMap = make(map[string]struct{})
		s.values[route] = routeMap
	}
	if _, exists := routeMap[fingerprint]; exists {
		return false
	}
	routeMap[fingerprint] = struct{}{}
	return true
}

// Contains reports whether a fingerprint exists for the route.
func (s *MatchStore) Contains(route string, fingerprint string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	routeMap, ok := s.values[route]
	if !ok {
		return false
	}
	_, exists := routeMap[fingerprint]
	return exists
}

// Size returns the number of fingerprints stored for the route.
func (s *MatchStore) Size(route string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.values[route])
}

// Clear removes all cached fingerprints across routes and returns the count removed.
func (s *MatchStore) Clear() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, routeMap := range s.values {
		removed += len(routeMap)
	}
	s.values = make(map[string]map[string]struct{})
	return removed
}

// SaveSnapshot writes the current snapshot to disk.
func (s *MatchStore) SaveSnapshot(path string) error {
	return Save(path, s.Snapshot())
}

// LoadSnapshot reads a snapshot from disk.
func (s *MatchStore) LoadSnapshot(path string) (map[string][]string, error) {
	return Load(path)
}

// Snapshot returns a copy of all values keyed by route.
func (s *MatchStore) Snapshot() map[string][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]string, len(s.values))
	for route, vals := range s.values {
		list := make([]string, 0, len(vals))
		for v := range vals {
			list = append(list, v)
		}
		out[route] = list
	}
	return out
}

// Load replaces the store contents with the provided snapshot.
func (s *MatchStore) Load(snapshot map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values = make(map[string]map[string]struct{}, len(snapshot))
	for route, vals := range snapshot {
		routeMap := make(map[string]struct{}, len(vals))
		for _, v := range vals {
			routeMap[v] = struct{}{}
		}
		s.values[route] = routeMap
	}
}
