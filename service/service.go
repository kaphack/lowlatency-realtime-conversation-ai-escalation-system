package service

import (
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "lowlatency-realtime-conversation-ai-escalation-system/proto/convo"
)

// ConversationServer implements the proto service and writes audio chunks to Kafka.
type ConversationServer struct {
	pb.UnimplementedConversationServiceServer

	KafkaWriter *kafka.Writer
}

// NewServer creates a ConversationServer with a configured Kafka writer.
func NewServer(brokers []string, topic string) *ConversationServer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafka.Hash{}, // ensures same conversation key goes to same partition
		// You can tune BatchSize, BatchTimeout, RequiredAcks etc. here.
	})

	return &ConversationServer{
		KafkaWriter: writer,
	}
}

// StreamAudio receives client-streamed AudioChunk messages and writes each chunk into Kafka.
// The RPC is client-streaming: when the client finishes, we send a ProduceAck and return.
func (s *ConversationServer) StreamAudio(stream pb.ConversationService_StreamAudioServer) error {
	ctx := stream.Context()
	log.Println("StreamAudio: new client stream")

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// Client finished sending - respond with ack
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

		// Marshal the protobuf chunk to bytes for Kafka
		value, err := proto.Marshal(chunk)
		if err != nil {
			log.Printf("proto.Marshal error: %v", err)
			// decide: continue or return. We continue so other chunks are not lost.
			continue
		}

		// Write to Kafka (keyed by conversation id to maintain ordering per conversation)
		msg := kafka.Message{
			Key:   []byte(chunk.ConversationId),
			Value: value,
			Time:  time.Now(),
		}

		// Use context from stream so kafka write can be canceled if client disconnects.
		if err := s.KafkaWriter.WriteMessages(ctx, msg); err != nil {
			log.Printf("kafka write error for conv=%s seq=%d: %v", chunk.ConversationId, chunk.Sequence, err)
			// If Kafka is down you may want to retry or route to a DLQ. For now, continue.
			continue
		}

		log.Printf("StreamAudio: sent to kafka conv=%s seq=%d bytes=%d",
			chunk.ConversationId, chunk.Sequence, len(chunk.Payload))
	}
}
