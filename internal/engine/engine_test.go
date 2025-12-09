package engine

import (
	"encoding/json"
	"testing"

	"kafka-bridge/internal/config"
	"kafka-bridge/internal/store"
)

func TestFingerprintDeterministic(t *testing.T) {
	payload := map[string]any{
		"fieldA": "value1",
		"sub": map[string]any{
			"fieldB": "value2",
		},
	}
	values, err := extractMatchValues(payload, []string{"sub.fieldB", "fieldA"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := values, []string{"value2", "value1"}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("unexpected match values: %v", values)
	}
}

func TestFingerprintMissingField(t *testing.T) {
	payload := map[string]any{"fieldA": "value1"}
	_, err := extractMatchValues(payload, []string{"missing"})
	if err == nil {
		t.Fatalf("expected error for missing field")
	}
}

func TestMatcherReferenceAndForward(t *testing.T) {
	s := store.NewMatchStore()
	m, err := NewMatcher("route", []config.ReferenceFeed{
		{Topic: "feed-a", MatchFields: []string{"fieldA"}},
		{Topic: "feed-b", MatchFields: []string{"sub.fieldB"}},
	}, s)
	if err != nil {
		t.Fatalf("NewMatcher error: %v", err)
	}

	refPayload := map[string]any{
		"fieldA": "value1",
		"sub":    map[string]any{"fieldB": "value2"},
	}
	refBytes, _ := json.Marshal(refPayload)

	added, err := m.ProcessReference("feed-a", nil, refBytes)
	if err != nil || !added {
		t.Fatalf("expected reference to be added, err=%v", err)
	}
	added, err = m.ProcessReference("feed-b", nil, refBytes)
	if err != nil || !added {
		t.Fatalf("expected second reference to be added, err=%v", err)
	}

	srcPayload := map[string]any{
		"fieldA": "value1",
		"sub":    map[string]any{"fieldB": "value2"},
	}
	srcBytes, _ := json.Marshal(srcPayload)

	forward, err := m.ShouldForward(srcBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !forward {
		t.Fatalf("expected source to forward after reference cached")
	}

	nonMatching := map[string]any{
		"fieldA": "other",
		"sub":    map[string]any{"fieldB": "different"},
	}
	nonBytes, _ := json.Marshal(nonMatching)
	forward, _ = m.ShouldForward(nonBytes)
	if forward {
		t.Fatalf("expected non-matching source to be blocked")
	}

	// manual add should allow matching when value appears anywhere
	if added := m.AddValues([]string{"other"}); !added {
		t.Fatalf("expected manual add to succeed")
	}
	forward, _ = m.ShouldForward(nonBytes)
	if !forward {
		t.Fatalf("expected manual value to allow forwarding when present in payload")
	}
}
