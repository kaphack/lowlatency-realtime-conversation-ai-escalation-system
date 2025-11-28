package core

import (
	"strings"
	"unicode"
)

// Analyzer is responsible for processing text and extracting metrics
type Analyzer struct{}

func NewAnalyzer() *Analyzer {
	return &Analyzer{}
}

// Analyze returns a map of word counts from the input text
func (a *Analyzer) Analyze(text string) map[string]int {
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

	return counts
}
