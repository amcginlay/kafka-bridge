package main

import (
	"encoding/json"
	"testing"
)

func TestFingerprintMessageOrdersFields(t *testing.T) {
	payload := map[string]any{
		"fieldA": "value1",
		"sub": map[string]any{
			"fieldB": "value2",
		},
	}
	data, _ := json.Marshal(payload)

	fp, err := fingerprintMessage(data, []string{"sub.fieldB", "fieldA"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `[{"path":"fieldA","value":"value1"},{"path":"sub.fieldB","value":"value2"}]`
	if fp != expected {
		t.Fatalf("fingerprint mismatch\nwant: %s\ngot:  %s", expected, fp)
	}
}

func TestFingerprintMessageMissingField(t *testing.T) {
	payload := map[string]any{"fieldA": "value1"}
	data, _ := json.Marshal(payload)

	_, err := fingerprintMessage(data, []string{"missing"})
	if err == nil {
		t.Fatalf("expected error for missing field")
	}
}
