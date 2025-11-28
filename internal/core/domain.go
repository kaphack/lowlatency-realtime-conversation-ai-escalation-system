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
