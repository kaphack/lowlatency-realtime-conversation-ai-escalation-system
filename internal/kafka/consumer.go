package kafka

import (
	"context"
	"log"
	"strings"

	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader   *kafka.Reader
	analyzer *core.Analyzer
	engine   *core.Engine
	repo     *db.Repository
}

func NewConsumer(brokers []string, topic string, groupID string, repo *db.Repository) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &Consumer{
		reader:   reader,
		analyzer: core.NewAnalyzer(),
		engine:   core.NewEngine(),
		repo:     repo,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	defer c.reader.Close()

	log.Println("Kafka consumer started...")

	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		text := string(m.Value)
		log.Printf("Received message: %s", text)

		// 1. Analyze
		analysis := c.analyzer.Analyze(text)

		// 2. Fetch Rules (In a real system, cache this!)
		rules, err := c.repo.GetAllRules()
		if err != nil {
			log.Printf("Failed to fetch rules: %v", err)
			continue
		}

		// 3. Evaluate
		actions := c.engine.Evaluate(analysis, rules)

		// 4. Trigger Actions
		for _, action := range actions {
			c.trigger(action, text)
		}
	}
}

func (c *Consumer) trigger(action string, context string) {
	// In a real system, this would call an external service or workflow engine
	log.Printf("!!! ESCALATION TRIGGERED !!! Action: %s | Context: %s", strings.ToUpper(action), context)
}
