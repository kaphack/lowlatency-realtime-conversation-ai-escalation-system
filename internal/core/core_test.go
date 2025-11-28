package core

import (
	"encoding/json"
	"testing"
)

func TestAnalyzer(t *testing.T) {
	analyzer := NewAnalyzer()
	text := "Hello world! This is a test. Hello again."
	counts := analyzer.Analyze(text)

	if counts["hello"] != 2 {
		t.Errorf("Expected 'hello' count 2, got %d", counts["hello"])
	}
	if counts["world"] != 1 {
		t.Errorf("Expected 'world' count 1, got %d", counts["world"])
	}
}

func TestEngine(t *testing.T) {
	engine := NewEngine()
	
	// Rule: "help" >= 2
	cond := Condition{Word: "help", Operator: ">=", Count: 2}
	condBytes, _ := json.Marshal([]Condition{cond})
	rule := ParsedRule{
		Rule: Rule{Name: "Help Rule", Action: "escalate", Conditions: json.RawMessage(condBytes)},
		ParsedConditions: []Condition{cond},
	}

	// Case 1: Match
	analysis := map[string]int{"help": 2, "other": 5}
	actions := engine.Evaluate(analysis, []ParsedRule{rule})
	if len(actions) != 1 || actions[0] != "escalate" {
		t.Errorf("Expected escalation, got %v", actions)
	}

	// Case 2: No Match
	analysis = map[string]int{"help": 1}
	actions = engine.Evaluate(analysis, []ParsedRule{rule})
	if len(actions) != 0 {
		t.Errorf("Expected no action, got %v", actions)
	}
}
