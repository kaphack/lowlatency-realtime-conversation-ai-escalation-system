package main

//nice
import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	sessionID := flag.String("session_id", "", "conversation session id (optional)")
	sender := flag.String("sender", "CUSTOMER", "sender id (e.g. CUSTOMER, AGENT)")
	flag.Parse()

	if *sessionID == "" {
		*sessionID = fmt.Sprintf("session-%d", time.Now().UnixNano())
	}

	log.Printf("Connecting to gRPC server at %s", *addr)

	grpcConnection, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer grpcConnection.Close()

	client := conversationv1.NewConversationStreamClient(grpcConnection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamConversation(ctx)
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	log.Printf("Streaming conversation for session_id=%s", *sessionID)
	log.Println("Type lines and press ENTER to send. Ctrl+D (EOF) to finish.")

	scanner := bufio.NewScanner(os.Stdin)
	messageCounter := 0

	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		messageCounter++
		msgID := fmt.Sprintf("msg-%d", messageCounter)

		chunk := &conversationv1.ConversationChunk{
			SessionId:   *sessionID,
			MessageId:   msgID,
			Sender:      *sender,
			Text:        text,
			TimestampMs: time.Now().UnixMilli(),
			Metadata: map[string]string{
				"source": "cli-producer",
			},
		}

		log.Printf("grpcProducer:main:Sending chunk: session=%s msg_id=%s text=%q", chunk.SessionId, chunk.MessageId, chunk.Text)

		if err := stream.Send(chunk); err != nil {
			log.Fatalf("failed to send chunk: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("stdin error: %v", err)
	}

	log.Println("Closing stream and waiting for ACK...")

	ack, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("failed to receive ACK: %v", err)
	}

	log.Printf("grpcProducer:Received ACK: session_id=%s last_message_id=%s success=%v message=%q",
		ack.SessionId, ack.LastMessageId, ack.Success, ack.Message)
}
