package core

import (
	"log"
	"strings"
	"unicode"

	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
)

// RuleRepository defines the interface for fetching rules
type RuleRepository interface {
	GetAllRules() ([]ParsedRule, error)
}

// Analyzer is responsible for processing text and extracting metrics
type Analyzer struct {
	repo RuleRepository
}

func NewAnalyzer(repo RuleRepository) *Analyzer {
	return &Analyzer{
		repo: repo,
	}
}

// Analyze returns a map of word counts from the input text
func (a *Analyzer) Analyze(convoChunk *conversationv1.ConversationChunk) map[string]int {
	text := convoChunk.Text
	counts := make(map[string]int)

	// Normalize and split
	// This is a simple tokenizer. For production, consider regex or more robust NLP.
	f := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	words := strings.FieldsFunc(text, f)

	for _, word := range words {
		normalized := strings.ToLower(word)
		counts[normalized]++
	}

	// Get all rules from db check one by one if rules matches trigger action
	rules, err := a.repo.GetAllRules()
	if err != nil {
		log.Printf("Failed to fetch rules in Analyzer: %v", err)
		return counts
	}

	engine := NewEngine()
	actions := engine.Evaluate(counts, rules)
	for _, action := range actions {
		log.Printf("!!! ANALYZER TRIGGERED ACTION !!! Action: %s | Text: %s", strings.ToUpper(action), text)
		// In a real system, we might want to return these actions or call a callback
	}

	return counts
}
