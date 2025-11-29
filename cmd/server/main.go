package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/api"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/kafka"
)

func main() {
	// Configuration
	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		dbUser = "hackathon_user"
	}
	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		dbPassword = "HAck@th0n_2025"
	}
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "34.133.251.179"
	}
	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "3306"
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "escalation_db"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "conversations"
	}

	// Initialize DB
	repo, err := db.NewRepository(dsn)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Seed a default rule if none exist
	seedRules(repo)

	// Initialize API Handler
	apiHandler := api.NewHandler(repo)
	mux := http.NewServeMux()
	apiHandler.RegisterRoutes(mux)

	// Configure HTTP Server
	apiPort := os.Getenv("API_PORT")
	if apiPort == "" {
		apiPort = "8080"
	}
	httpServer := &http.Server{
		Addr:    ":" + apiPort,
		Handler: mux,
	}

	// Initialize Consumer
	consumer := kafka.NewConsumer(
		[]string{kafkaBrokers},
		kafkaTopic,
		"escalation-group",
		repo,
	)

	// Run Consumer and HTTP Server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start HTTP Server in goroutine
	go func() {
		log.Printf("Starting HTTP server on port %s", apiPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start Kafka Consumer in goroutine
	go func() {
		if err := consumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutting down...")

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Cancel Kafka consumer context
	cancel()
	log.Println("Shutdown complete")
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
