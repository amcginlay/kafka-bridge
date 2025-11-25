package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"kafka-bridge/internal/config"
	"kafka-bridge/internal/store"
)

// Matcher coordinates reference caching and source matching per route.
type Matcher struct {
	routeID string
	fields  map[string][]string
	store   *store.MatchStore
}

// NewMatcher constructs a matcher for a specific route.
func NewMatcher(routeID string, feeds []config.ReferenceFeed, store *store.MatchStore) *Matcher {
	fieldMap := make(map[string][]string, len(feeds))
	for _, f := range feeds {
		fieldMap[f.Topic] = append([]string(nil), f.MatchFields...)
	}
	return &Matcher{
		routeID: routeID,
		fields:  fieldMap,
		store:   store,
	}
}

// ProcessReference ingests a reference payload from a specific topic and stores each extracted value.
func (m *Matcher) ProcessReference(topic string, payload []byte) (bool, error) {
	fields, ok := m.fields[topic]
	if !ok {
		return false, fmt.Errorf("no match fields configured for topic %s", topic)
	}

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		return false, err
	}

	values, err := extractMatchValues(body, fields)
	if err != nil {
		return false, err
	}

	added := false
	for _, v := range values {
		if m.store.Add(m.routeID, v) {
			added = true
		}
	}
	return added, nil
}

// ShouldForward checks if ANY cached reference value appears anywhere in the payload.
func (m *Matcher) ShouldForward(payload []byte) (bool, error) {
	var body any
	if err := json.Unmarshal(payload, &body); err != nil {
		return false, err
	}

	values := flattenValues(body)
	for _, v := range values {
		if m.store.Contains(m.routeID, v) {
			return true, nil
		}
	}
	return false, nil
}

// Size returns the number of cached values for the route.
func (m *Matcher) Size() int {
	return m.store.Size(m.routeID)
}

func extractMatchValues(payload map[string]any, fields []string) ([]string, error) {
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		val, err := lookupField(payload, field)
		if err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%v", val))
	}
	return out, nil
}

func lookupField(payload map[string]any, field string) (any, error) {
	parts := strings.Split(field, ".")
	switch len(parts) {
	case 1:
		if val, ok := payload[parts[0]]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("field %s not found", field)
	case 2:
		child, ok := payload[parts[0]].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %s missing nested object", field)
		}
		if val, ok := child[parts[1]]; ok {
			return val, nil
		}
		return nil, fmt.Errorf("field %s not found", field)
	default:
		return nil, fmt.Errorf("field %s depth unsupported", field)
	}
}

func flattenValues(v any) []string {
	switch val := v.(type) {
	case map[string]any:
		var res []string
		// deterministic iteration
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			res = append(res, flattenValues(val[k])...)
		}
		return res
	case []any:
		var res []string
		for _, item := range val {
			res = append(res, flattenValues(item)...)
		}
		return res
	default:
		return []string{fmt.Sprintf("%v", val)}
	}
}
