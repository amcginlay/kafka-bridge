package store

import "testing"

func TestMatchStorePerRoute(t *testing.T) {
	s := NewMatchStore()

	if added := s.Add("routeA", "fp1"); !added {
		t.Fatalf("expected first insert to add")
	}
	if added := s.Add("routeA", "fp1"); added {
		t.Fatalf("expected duplicate insert to be ignored")
	}
	if !s.Contains("routeA", "fp1") {
		t.Fatalf("expected routeA to contain fp1")
	}
	if s.Contains("routeB", "fp1") {
		t.Fatalf("unexpected fp1 presence in routeB")
	}
	if size := s.Size("routeA"); size != 1 {
		t.Fatalf("expected size 1 for routeA, got %d", size)
	}
}
