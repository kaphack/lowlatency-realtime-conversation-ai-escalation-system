package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	pb "hackathon-ai/proto/convo"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Dial with insecure credentials (dev). Replace with TLS in prod.
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewConversationServiceClient(conn)

	ctx := context.Background()
	stream, err := client.StreamAudio(ctx)
	if err != nil {
		log.Fatalf("Error creating stream: %v", err)
	}

	// Try to open a sample file (client/sample.wav). If not present, send dummy payloads.
	f, ferr := os.Open("client/sample.wav")
	if ferr != nil {
		log.Println("sample.wav not found, sending dummy payloads")
		sendDummy(stream)
		return
	}
	defer f.Close()

	sendFileStream(f, stream)
}

func sendDummy(stream pb.ConversationService_StreamAudioClient) {
	for i := 1; i <= 5; i++ {
		err := stream.Send(&pb.AudioChunk{
			ConversationId: "test123",
			ParticipantId:  "user1",
			Sequence:       uint64(i),
			Payload:        []byte("hello-" + time.Now().Format(time.RFC3339Nano)),
			Timestamp:      time.Now().UnixMilli(),
		})
		if err != nil {
			// If the server closed the stream, try to receive the ACK then exit
			log.Printf("Error sending chunk %d: %v", i, err)
			tryCloseAndRecv(stream)
			return
		}
		log.Println("Sent chunk:", i)
		time.Sleep(500 * time.Millisecond)
	}

	// normal finish
	tryCloseAndRecv(stream)
}

func sendFileStream(f *os.File, stream pb.ConversationService_StreamAudioClient) {
	buf := make([]byte, 16000) // 16KB per chunk; adjust to your packetization (e.g. 20ms)
	seq := uint64(1)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			log.Println("Reached EOF of file. Closing stream.")
			tryCloseAndRecv(stream)
			return
		}
		if err != nil {
			log.Printf("Error reading file: %v", err)
			tryCloseAndRecv(stream)
			return
		}

		err = stream.Send(&pb.AudioChunk{
			ConversationId: "test123",
			ParticipantId:  "user1",
			Sequence:       seq,
			Payload:        buf[:n],
			Timestamp:      time.Now().UnixMilli(),
		})
		if err != nil {
			log.Printf("Error sending chunk %d: %v", seq, err)
			tryCloseAndRecv(stream)
			return
		}
		log.Printf("Sent chunk %d (bytes=%d)", seq, n)
		seq++
		// small sleep to simulate live streaming; tune as needed
		time.Sleep(50 * time.Millisecond)
	}
}

func tryCloseAndRecv(stream pb.ConversationService_StreamAudioClient) {
	ack, err := stream.CloseAndRecv()
	if err != nil {
		log.Printf("CloseAndRecv error: %v", err)
		return
	}
	log.Printf("Server ACK: conversation=%s ok=%v msg=%s", ack.GetConversationId(), ack.GetOk(), ack.GetMessage())
}
