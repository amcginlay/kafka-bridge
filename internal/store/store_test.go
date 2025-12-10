package store

import "testing"

func TestMatchStoreClear(t *testing.T) {
	s := NewMatchStore()
	s.Add("route-a", "one")
	s.Add("route-a", "two")
	s.Add("route-b", "one")

	removed := s.Clear()
	if removed != 3 {
		t.Fatalf("expected 3 removed entries, got %d", removed)
	}
	if got := s.Size("route-a"); got != 0 {
		t.Fatalf("expected route-a to be empty, got %d", got)
	}
	if got := s.Size("route-b"); got != 0 {
		t.Fatalf("expected route-b to be empty, got %d", got)
	}

	if removed = s.Clear(); removed != 0 {
		t.Fatalf("expected no removals from empty store, got %d", removed)
	}
}
