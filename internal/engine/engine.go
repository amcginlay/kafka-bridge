package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"kafka-bridge/internal/config"
	"kafka-bridge/internal/store"
)

// Matcher coordinates reference caching and source matching per route.
type Matcher struct {
	routeID string
	feeds   []feedMatcher
	store   *store.MatchStore
}

type feedMatcher struct {
	name         string
	topic        string
	topicHeaders map[string]string
	fields       []string
}

// NewMatcher constructs a matcher for a specific route.
func NewMatcher(routeID string, feeds []config.ReferenceFeed, store *store.MatchStore) (*Matcher, error) {
	var feedMatchers []feedMatcher
	for _, f := range feeds {
		hdrs, err := parseTopicHeaders(f.TopicHeaders)
		if err != nil {
			return nil, err
		}
		feedMatchers = append(feedMatchers, feedMatcher{
			name:         f.DisplayName(),
			topic:        f.Topic,
			topicHeaders: hdrs,
			fields:       append([]string(nil), f.MatchFields...),
		})
	}
	return &Matcher{
		routeID: routeID,
		feeds:   feedMatchers,
		store:   store,
	}, nil
}

// ProcessReference ingests a reference payload from a specific topic/headers and stores each extracted value.
func (m *Matcher) ProcessReference(topic string, headers map[string]string, payload []byte) (bool, string, error) {
	feed, ok := m.feedFor(topic, headers)
	if !ok {
		return false, "", fmt.Errorf("no match fields configured for topic %s with provided headers", topic)
	}

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		return false, feed.name, err
	}

	values, err := extractMatchValues(body, feed.fields)
	if err != nil {
		return false, feed.name, err
	}

	added := false
	for _, v := range values {
		for _, variant := range yearVariants(v) {
			if m.store.Add(m.routeID, variant) {
				added = true
			}
		}
	}
	return added, feed.name, nil
}

// ShouldForward checks if ANY cached reference value appears anywhere in the payload.
func (m *Matcher) ShouldForward(payload []byte) (bool, error) {
	var body any
	if err := json.Unmarshal(payload, &body); err != nil {
		return false, err
	}

	values := flattenValues(body)
	for _, v := range values {
		for _, variant := range yearVariants(v) {
			if m.store.Contains(m.routeID, variant) {
				return true, nil
			}
		}
	}
	return false, nil
}

// Size returns the number of cached values for the route.
func (m *Matcher) Size() int {
	return m.store.Size(m.routeID)
}

// AddValues inserts raw reference values (used by HTTP injection).
func (m *Matcher) AddValues(values []string) bool {
	added := false
	for _, v := range values {
		for _, variant := range yearVariants(v) {
			if m.store.Add(m.routeID, variant) {
				added = true
			}
		}
	}
	return added
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

func parseTopicHeaders(raw []string) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(raw))
	for _, kv := range raw {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid topicHeader %q (expected key=value)", kv)
		}
		out[strings.ToLower(parts[0])] = parts[1]
	}
	return out, nil
}

func (m *Matcher) feedFor(topic string, headers map[string]string) (feedMatcher, bool) {
	for _, f := range m.feeds {
		if f.topic != topic {
			continue
		}
		if !headersMatch(f.topicHeaders, headers) {
			continue
		}
		return f, true
	}
	return feedMatcher{}, false
}

func headersMatch(expected map[string]string, actual map[string]string) bool {
	if len(expected) == 0 {
		return true
	}
	for k, v := range expected {
		if actual == nil {
			return false
		}
		if av, ok := actual[strings.ToLower(k)]; !ok || av != v {
			return false
		}
	}
	return true
}

func yearVariants(v string) []string {
	variants := make(map[string]struct{}, 2)
	variants[v] = struct{}{}

	// If value starts with NN/, add 20NN/ variant.
	if len(v) >= 3 && isDigit(rune(v[0])) && isDigit(rune(v[1])) && v[2] == '/' {
		variants["20"+v] = struct{}{}
	}

	// If value starts with 20NN/, add NN/ variant.
	if len(v) >= 5 && strings.HasPrefix(v, "20") && isDigit(rune(v[2])) && isDigit(rune(v[3])) && v[4] == '/' {
		variants[v[2:]] = struct{}{}
	}

	out := make([]string, 0, len(variants))
	for val := range variants {
		out = append(out, val)
	}
	return out
}

func isDigit(r rune) bool {
	return unicode.IsDigit(r)
}
