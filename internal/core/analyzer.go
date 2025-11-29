package core

import (
	"strings"
	"unicode"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
)

// Analyzer is responsible for processing text and extracting metrics
type Analyzer struct {
	repo *db.Repository
}

func NewAnalyzer(repo *db.Repository) *Analyzer {
	return &Analyzer{
		repo: repo,
	}
}

// Analyze returns a map of word counts from the input text
func (a *Analyzer) Analyze(convoChunk *conversationv1.ConversationChunk) map[string]int {
	text := convoChunk.Text
	counts := make(map[string]int)
	//todo: get all rules from db check one by one if rules matches trigger action
	//....
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

	return counts
}
