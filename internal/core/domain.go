package core

import "encoding/json"

// Condition represents a single check, e.g., "word 'help' count >= 3"
type Condition struct {
	Word     string `json:"word"`
	Operator string `json:"operator"` // ">", ">=", "==", etc.
	Count    int    `json:"count"`
}

// Rule represents an escalation rule
type Rule struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Conditions json.RawMessage `json:"conditions"` // Stored as JSON in DB, unmarshaled to []Condition
	Action     string          `json:"action"`     // e.g., "log", "webhook"
}

// ParsedRule is a helper struct with unmarshaled conditions
type ParsedRule struct {
	Rule
	ParsedConditions []Condition
}

// Analysis represents the result of analyzing a conversation
type Analysis struct {
	WordCounts map[string]int
}

// Tokenize splits content into words (simple implementation)
func Tokenize(content string) []string {
	// In a real implementation, use regex or a proper tokenizer
	// This is a placeholder for the example
	var words []string
	currentWord := ""
	for _, r := range content {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			currentWord += string(r)
		} else if currentWord != "" {
			words = append(words, currentWord)
			currentWord = ""
		}
	}
	if currentWord != "" {
		words = append(words, currentWord)
	}
	return words
}
