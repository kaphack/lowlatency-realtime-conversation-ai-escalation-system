package core

import (
	"log"
	"strings"
)

// Engine evaluates rules against analysis results
type Engine struct{}

func NewEngine() *Engine {
	return &Engine{}
}

// Evaluate checks if the analysis meets any rule conditions and returns triggered actions
func (e *Engine) Evaluate(analysis map[string]int, rules []ParsedRule) []string {
	var actions []string

	for _, rule := range rules {
		if e.matches(analysis, rule.ParsedConditions) {
			log.Printf("Rule matched: %s", rule.Name)
			actions = append(actions, rule.Action)
		}
	}

	return actions
}

func (e *Engine) matches(analysis map[string]int, conditions []Condition) bool {
	// All conditions must match (AND logic)
	// For OR logic, we'd need a more complex structure
	for _, cond := range conditions {
		actualCount := analysis[strings.ToLower(cond.Word)]
		if !compare(actualCount, cond.Count, cond.Operator) {
			return false
		}
	}
	return true
}

func compare(actual, target int, op string) bool {
	switch op {
	case ">":
		return actual > target
	case ">=":
		return actual >= target
	case "<":
		return actual < target
	case "<=":
		return actual <= target
	case "==":
		return actual == target
	default:
		return false
	}
}
