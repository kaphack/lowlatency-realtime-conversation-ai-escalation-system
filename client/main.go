package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	pb "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto/convo"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxConcurrentStreams = 100 // Maximum number of parallel streams
	serverAddress        = "localhost:50051"
	transcriptFile       = "client/sample-transcripts.txt"
	streamTimeout        = 30 * time.Second // Timeout per stream
	totalTimeout         = 60 * time.Second // Total client timeout
)

func main() {
	// Connect to server
	conn, err := grpc.Dial(
		serverAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Could not connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewConversationServiceClient(conn)

	// Read all transcripts from file
	transcripts, err := readTranscriptsFromFile(transcriptFile)
	if err != nil {
		log.Printf("Could not read transcript file: %v, using dummy data", err)
		transcripts = getDummyTranscripts()
	}

	if len(transcripts) == 0 {
		log.Println("No transcripts to send")
		return
	}

	// Dynamically choose number of streams: up to maxConcurrentStreams
	numStreams := len(transcripts)
	if numStreams > maxConcurrentStreams {
		numStreams = maxConcurrentStreams
	}

	// Divide transcripts among concurrent streams
	chunkedTranscripts := divideTranscripts(transcripts, numStreams)

	// Use a cancellable context with total timeout
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	var wg sync.WaitGroup
	var hasError sync.Once // Ensure we cancel on first error

	// Start multiple concurrent streams (goroutines)
	for i, chunk := range chunkedTranscripts {
		wg.Add(1)
		go func(streamID int, transcriptChunk []string) {
			defer wg.Done()

			conversationID := fmt.Sprintf("conv-%d", streamID)
			participantID := fmt.Sprintf("user-%d", streamID)

			// Each stream gets its own child context (inherits cancellation)
			streamCtx, streamCancel := context.WithTimeout(ctx, streamTimeout)
			defer streamCancel()

			if err := sendTranscriptStream(streamCtx, client, conversationID, participantID, transcriptChunk); err != nil {
				log.Printf("[Stream %d] Error: %v", streamID, err)
				hasError.Do(cancel) // Cancel all on first error (optional)
			}
		}(i+1, chunk)
	}

	wg.Wait()
	log.Println("All transcript streams completed")
}

// sendTranscriptStream sends a chunk of transcripts in a single stream
func sendTranscriptStream(
	ctx context.Context,
	client pb.ConversationServiceClient,
	conversationID string,
	participantID string,
	transcripts []string,
) error {
	stream, err := client.StreamAudio(ctx)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}
	defer stream.CloseAndRecv() // Still try to receive ACK even on send errors

	log.Printf("[%s] Starting stream with %d transcripts", conversationID, len(transcripts))

	for seq, text := range transcripts {
		if len(text) == 0 {
			continue
		}

		// Respect context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := stream.Send(&pb.AudioChunk{
			ConversationId: conversationID,
			ParticipantId:  participantID,
			Sequence:       uint64(seq + 1),
			Payload:        []byte(text),
			Timestamp:      time.Now().UnixMilli(),
		})
		if err != nil {
			log.Printf("[%s] Error sending seq %d: %v", conversationID, seq+1, err)
			return fmt.Errorf("send failed at seq %d: %w", seq+1, err)
		}

		log.Printf("[%s] Sent seq=%d text=\"%s\"", conversationID, seq+1, text)
		time.Sleep(100 * time.Millisecond) // Simulate real-time streaming
	}

	// Close stream and receive ACK
	ack, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("CloseAndRecv failed: %w", err)
	}

	log.Printf("[%s] Server ACK: ok=%v msg=%s", conversationID, ack.GetOk(), ack.GetMessage())
	return nil
}

// ... rest of helper functions (unchanged) ...

func readTranscriptsFromFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var transcripts []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			transcripts = append(transcripts, line)
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, err
	}

	return transcripts, nil
}

func divideTranscripts(transcripts []string, n int) [][]string {
	if n <= 0 || len(transcripts) == 0 {
		return [][]string{transcripts}
	}

	chunkSize := (len(transcripts) + n - 1) / n // ceiling division
	chunks := make([][]string, 0, n)

	for i := 0; i < len(transcripts); i += chunkSize {
		end := i + chunkSize
		if end > len(transcripts) {
			end = len(transcripts)
		}
		chunks = append(chunks, transcripts[i:end])
	}

	return chunks
}

func getDummyTranscripts() []string {
	return []string{
		"Hello, this is a test transcript.",
	}
}
