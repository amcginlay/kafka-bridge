package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"kafka-bridge/internal/engine"
	"kafka-bridge/internal/store"
)

func TestCacheClearEndpoint(t *testing.T) {
	matchStore := store.NewMatchStore()
	matchStore.Add("route-a", "value1")
	matchStore.Add("route-b", "value2")

	server := httptest.NewServer(buildHTTPMux(map[string]*engine.Matcher{}, matchStore))
	t.Cleanup(server.Close)

	resp, err := http.Post(server.URL+"/cache/clear", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /cache/clear request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if got := matchStore.Size("route-a"); got != 0 {
		t.Fatalf("expected cache to be cleared for route-a, got %d", got)
	}
	if got := matchStore.Size("route-b"); got != 0 {
		t.Fatalf("expected cache to be cleared for route-b, got %d", got)
	}

	resp, err = http.Get(server.URL + "/cache/clear")
	if err != nil {
		t.Fatalf("GET /cache/clear request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405 for GET, got %d", resp.StatusCode)
	}
}

func TestCacheGetEndpoint(t *testing.T) {
	matchStore := store.NewMatchStore()
	matchStore.Add("route-a", "value1")
	matchStore.Add("route-b", "value2")

	server := httptest.NewServer(buildHTTPMux(map[string]*engine.Matcher{}, matchStore))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/cache")
	if err != nil {
		t.Fatalf("GET /cache request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	var snapshot map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("failed to decode snapshot: %v", err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 routes in snapshot, got %d", len(snapshot))
	}
	if vals, ok := snapshot["route-a"]; !ok || !contains(vals, "value1") {
		t.Fatalf("route-a missing expected value: %v", snapshot["route-a"])
	}
	if vals, ok := snapshot["route-b"]; !ok || !contains(vals, "value2") {
		t.Fatalf("route-b missing expected value: %v", snapshot["route-b"])
	}

	resp, err = http.Post(server.URL+"/cache", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /cache request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405 for POST, got %d", resp.StatusCode)
	}
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
