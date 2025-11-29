package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/kafka"
	pb "github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/proto/convo"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/service"

	"google.golang.org/grpc"
)

func main() {
	// Configuration (env overrides)
	dbPath := getenv("DB_PATH", "escalation.db")
	kafkaBrokers := getenv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getenv("KAFKA_TOPIC", "conversations")
	consumerGroup := getenv("KAFKA_CONSUMER_GROUP", "escalation-group")

	// Initialize DB
	repo, err := db.NewRepository(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// Seed a default rule if none exist
	seedRules(repo)

	// Create service (this creates a Kafka writer used by the gRPC handler)
	svc := service.NewServer([]string{kafkaBrokers}, kafkaTopic)
	// ensure Kafka writer closed on exit
	defer func() {
		if err := svc.Close(); err != nil {
			log.Printf("error closing service writer: %v", err)
		}
	}()

	// Create gRPC server and register service handler
	grpcServer := grpc.NewServer()
	pb.RegisterConversationServiceServer(grpcServer, svc)

	// Start listening for gRPC
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on :50051: %v", err)
	}

	// Context to manage lifecycle of goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Kafka consumer (your existing consumer logic)
	consumer := kafka.NewConsumer(
		[]string{kafkaBrokers},
		kafkaTopic,
		consumerGroup,
		repo,
	)

	// run consumer in background
	go func() {
		log.Println("Starting Kafka consumer...")
		if err := consumer.Start(ctx); err != nil {
			// If consumer returns an error, log and cancel main context to shutdown
			log.Printf("Consumer error: %v", err)
			cancel()
		}
	}()

	// run gRPC server in background
	go func() {
		log.Println("gRPC server listening on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC serve error: %v", err)
			cancel()
		}
	}()

	// graceful shutdown on signals or context cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-sigCh:
		log.Printf("signal received: %v — shutting down", s)
	case <-ctx.Done():
		log.Println("context canceled — shutting down")
	}

	// Begin shutdown procedure
	// 1) stop accepting new gRPC connections and gracefully stop
	grpcServer.GracefulStop()

	// 2) cancel consumer context and wait a short time to finish
	cancel()
	time.Sleep(200 * time.Millisecond)

	// 3) close service writer (deferred above)
	log.Println("server stopped")
}

// getenv returns env value or fallback if empty
func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
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
