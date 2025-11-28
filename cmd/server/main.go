package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/kafka"
)

func main() {
	// Configuration
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "escalation.db"
	}
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "conversations"
	}

	// Initialize DB
	repo, err := db.NewRepository(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Seed a default rule if none exist
	seedRules(repo)

	// Initialize Consumer
	consumer := kafka.NewConsumer(
		[]string{kafkaBrokers},
		kafkaTopic,
		"escalation-group",
		repo,
	)

	// Run Consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}

func seedRules(repo *db.Repository) {
	rules, _ := repo.GetAllRules()
	if len(rules) > 0 {
		return
	}

	log.Println("Seeding default rules...")
	// Example: Trigger "human_handoff" if "help" appears >= 2 times
	conds := []core.Condition{
		{Word: "help", Operator: ">=", Count: 2},
	}
	_, err := repo.CreateRule("Help Request", conds, "human_handoff")
	if err != nil {
		log.Printf("Failed to seed rule: %v", err)
	}
}
