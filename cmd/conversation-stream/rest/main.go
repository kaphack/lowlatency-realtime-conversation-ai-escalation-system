package main

import (
	"log"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/httpapi"
)

func main() {
	r := httpapi.NewRouter()

	log.Println("REST API running on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("failed to run http server: %v", err)
	}
}
