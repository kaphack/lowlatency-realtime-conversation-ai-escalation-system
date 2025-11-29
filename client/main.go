package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxConcurrentStreams = 100 // Maximum number of parallel streams
	httpEndpoint         = "http://localhost:8080/api/v1/produce"
	transcriptFile       = "client/sample-transcripts.txt"
	streamTimeout        = 30 * time.Second // Timeout per stream
	totalTimeout         = 60 * time.Second // Total client timeout
	readBufferSize       = 1024             // Buffer size for reading transcripts
)

type ProduceRequest struct {
	SessionID string `json:"session_id"`
	Sender    string `json:"sender"`
	Text      string `json:"text"`
}

type ProduceResponse struct {
	AckSession string `json:"ack_session"`
	LastMsg    string `json:"last_msg"`
	Success    bool   `json:"success"`
}

func main() {

	// Use a cancellable context with total timeout
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	// Channel for reading transcripts asynchronously
	transcriptChan := make(chan string, readBufferSize)
	errorChan := make(chan error, 1)

	// Start goroutine to read transcripts from file
	var readWg sync.WaitGroup
	readWg.Add(1)
	go func() {
		defer readWg.Done()
		defer close(transcriptChan)

		if err := readTranscriptsAsync(ctx, transcriptFile, transcriptChan); err != nil {
			log.Printf("Error reading transcripts: %v", err)
			errorChan <- err
			return
		}
	}()

	// Collect transcripts from channel
	var transcripts []string
	for text := range transcriptChan {
		transcripts = append(transcripts, text)
	}

	readWg.Wait()

	// Check for read errors
	select {
	case err := <-errorChan:
		if err != nil {
			log.Printf("Using dummy data due to read error: %v", err)
			transcripts = getDummyTranscripts()
		}
	default:
	}

	if len(transcripts) == 0 {
		log.Println("No transcripts to send")
		return
	}

	log.Printf("Read %d transcripts from file", len(transcripts))

	// Dynamically choose number of streams
	numStreams := len(transcripts)
	if numStreams > maxConcurrentStreams {
		numStreams = maxConcurrentStreams
	}

	// Divide transcripts among concurrent streams
	chunkedTranscripts := divideTranscripts(transcripts, numStreams)

	var wg sync.WaitGroup
	//var hasError sync.Once

	grpcConnection, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}

	grpcClient := conversationv1.NewConversationStreamClient(grpcConnection)
	stream, err := grpcClient.StreamConversation(ctx)

	if godotenv.Load() != nil {
		log.Println("No .env file found, using system env")
	}

	// Start multiple concurrent streams (goroutines)
	for i, chunk := range chunkedTranscripts {
		wg.Add(1)
		go func(streamID int, transcriptChunk []string) {
			defer wg.Done()

			sessionID := fmt.Sprintf("session-%d", streamID)
			senderID := fmt.Sprintf("user-%d", streamID)
			chunkText := fmt.Sprintf("%s", transcriptChunk[streamID])

			chunkObj := &conversationv1.ConversationChunk{
				SessionId:   sessionID,
				MessageId:   "",
				Sender:      senderID,
				Text:        chunkText,
				TimestampMs: time.Now().UnixMilli(),
				Metadata: map[string]string{
					"source": "rest-api",
				},
			}
			stream.Send(chunkObj)

			//// Each stream gets its own child context
			//streamCtx, streamCancel := context.WithTimeout(ctx, streamTimeout)
			//defer streamCancel()
			//
			//if err := sendTranscriptStreamHTTP(streamCtx, httpClient, sessionID, senderID, transcriptChunk); err != nil {
			//	log.Printf("[Stream %d] Error: %v", streamID, err)
			//	hasError.Do(cancel) // Cancel all on first error (optional)
			//}
		}(i+1, chunk)
	}

	wg.Wait()
	log.Println("All transcript streams completed")
}

// readTranscriptsAsync reads transcripts from file and sends them to channel
func readTranscriptsAsync(ctx context.Context, filePath string, output chan<- string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		if len(line) > 0 {
			lineNum++
			log.Printf("Read line %d: %s", lineNum, line)
			output <- line
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		return err
	}

	log.Printf("Finished reading %d transcripts", lineNum)
	return nil
}

// sendTranscriptStreamHTTP sends a chunk of transcripts via HTTP API
func sendTranscriptStreamHTTP(
	ctx context.Context,
	client *http.Client,
	sessionID string,
	senderID string,
	transcripts []string,
) error {
	log.Printf("[%s] Starting stream with %d transcripts", sessionID, len(transcripts))

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

		// Create request payload
		reqPayload := ProduceRequest{
			SessionID: sessionID,
			Sender:    senderID,
			Text:      text,
		}

		jsonData, err := json.Marshal(reqPayload)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}

		// Create HTTP request with context
		req, err := http.NewRequestWithContext(ctx, "POST", httpEndpoint, bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		// Send request
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[%s] Error sending seq %d: %v", sessionID, seq+1, err)
			return fmt.Errorf("send failed at seq %d: %w", seq+1, err)
		}

		// Read response
		var result ProduceResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return fmt.Errorf("failed to decode response: %w", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP error: status=%d", resp.StatusCode)
		}

		if !result.Success {
			log.Printf("[%s] Server reported failure for seq %d", sessionID, seq+1)
		}

		log.Printf("[%s] Sent seq=%d text=\"%s\" success=%v", sessionID, seq+1, text, result.Success)

		// Simulate real-time streaming
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("[%s] Stream completed successfully", sessionID)
	return nil
}

// divideTranscripts divides transcripts into n chunks
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

// getDummyTranscripts returns fallback test data
func getDummyTranscripts() []string {
	return []string{
		"Hello, this is a test transcript.",
		"This is the second line of dummy data.",
		"And this is the third line for testing.",
	}
}
