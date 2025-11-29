package grpcserver

//nice
import (
	"io"
	"log"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/engine"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/workers"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
)

type ConversationServer struct {
	conversationv1.UnimplementedConversationStreamServer
	workerPool *workers.WorkerPool
	engine     *engine.Engine
}

func NewConversationServer() *ConversationServer {
	return &ConversationServer{
		workerPool: workers.NewWorkerPool(8),
		engine:     engine.NewEngine(),
	}
}

func (s *ConversationServer) StreamConversation(stream conversationv1.ConversationStream_StreamConversationServer) error {
	log.Println("Stream started")

	var lastSessionID string
	var lastMsgID string

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// Send ACK
			return stream.SendAndClose(&conversationv1.AnalyticsAck{
				SessionId:     lastSessionID,
				LastMessageId: lastMsgID,
				Success:       true,
				Message:       "Processed all chunks",
			})
		}
		if err != nil {
			log.Println("stream recv error:", err)
			return err
		}

		lastSessionID = chunk.SessionId
		lastMsgID = chunk.MessageId

		// Dispatch to worker pool
		s.workerPool.Dispatch(chunk.SessionId, func() {
			s.engine.ProcessChunk(chunk)
		})
	}
}
