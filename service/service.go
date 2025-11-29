package service

import (
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/proto/convo"
)

// ConversationServer implements the proto service and writes audio chunks to Kafka.
type ConversationServer struct {
	pb.UnimplementedConversationServiceServer

	KafkaWriter *kafka.Writer
}

// NewServer creates a ConversationServer with a configured Kafka writer.
func NewServer(brokers []string, topic string) *ConversationServer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Balancer:     &kafka.Hash{},          // ensures same conversation key goes to same partition
		BatchTimeout: 100 * time.Millisecond, // tune for latency vs throughput
		// You can tune BatchSize, RequiredAcks, etc. here as needed.
	})

	return &ConversationServer{
		KafkaWriter: writer,
	}
}

// Close shuts down resources held by server (call on application exit).
func (s *ConversationServer) Close() error {
	if s == nil || s.KafkaWriter == nil {
		return nil
	}
	return s.KafkaWriter.Close()
}

// StreamAudio receives client-streamed AudioChunk messages and writes each chunk into Kafka.
func (s *ConversationServer) StreamAudio(stream pb.ConversationService_StreamAudioServer) error {
	ctx := stream.Context()
	log.Println("StreamAudio: new client stream")

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			ack := &pb.ProduceAck{
				ConversationId: "stream-closed",
				Ok:             true,
				Message:        "stream received",
			}
			if err := stream.SendAndClose(ack); err != nil {
				log.Printf("SendAndClose error: %v", err)
				return err
			}
			log.Println("StreamAudio: client stream closed gracefully")
			return nil
		}
		if err != nil {
			log.Printf("StreamAudio: recv error: %v", err)
			return err
		}

		value, err := proto.Marshal(chunk)
		if err != nil {
			// Log with identifying info and continue (or choose to return error if chunk is critical)
			log.Printf("proto.Marshal error conv=%s seq=%d: %v", chunk.ConversationId, chunk.Sequence, err)
			continue
		}

		msg := kafka.Message{
			Key:   []byte(chunk.ConversationId),
			Value: value,
			Time:  time.Now(),
		}

		// retry a few times with backoff on transient errors
		maxRetries := 3
		var writeErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			writeErr = s.KafkaWriter.WriteMessages(ctx, msg)
			if writeErr == nil {
				break
			}
			// If ctx cancelled, break early
			if ctx.Err() != nil {
				log.Printf("StreamAudio: context canceled while writing to kafka conv=%s seq=%d: %v", chunk.ConversationId, chunk.Sequence, ctx.Err())
				break
			}
			backoff := time.Duration((attempt+1)*100) * time.Millisecond
			log.Printf("kafka write attempt %d failed for conv=%s seq=%d: %v; backing off %v", attempt+1, chunk.ConversationId, chunk.Sequence, writeErr, backoff)
			time.Sleep(backoff)
		}
		if writeErr != nil {
			// after retries failed; decide whether to drop or return error
			log.Printf("kafka write final failure conv=%s seq=%d: %v", chunk.ConversationId, chunk.Sequence, writeErr)
			// Option: return writeErr to close stream with an error; here we continue to accept more chunks
			continue
		}

		log.Printf("StreamAudio: sent to kafka conv=%s seq=%d bytes=%d", chunk.ConversationId, chunk.Sequence, len(chunk.Payload))
	}
}
