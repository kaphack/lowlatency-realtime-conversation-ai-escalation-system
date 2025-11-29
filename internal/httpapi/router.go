package httpapi

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ProducerAPI struct {
	grpcClient conversationv1.ConversationStreamClient
}

func NewRouter() *gin.Engine {
	// Connect to gRPC server
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}

	client := conversationv1.NewConversationStreamClient(conn)
	api := &ProducerAPI{grpcClient: client}

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.POST("/api/v1/produce", api.ProduceHandler)

	return r
}

type ProduceRequest struct {
	SessionID string `json:"session_id"`
	Sender    string `json:"sender"`
	Text      string `json:"text"`
}

func (p *ProducerAPI) ProduceHandler(c *gin.Context) {
	var req ProduceRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	// Open client‑streaming RPC
	stream, err := p.grpcClient.StreamConversation(c)
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to open gRPC stream"})
		return
	}

	// Generate message ID
	msgID := time.Now().Format("20060102150405.000000")

	chunk := &conversationv1.ConversationChunk{
		SessionId:   req.SessionID,
		MessageId:   msgID,
		Sender:      req.Sender,
		Text:        req.Text,
		TimestampMs: time.Now().UnixMilli(),
		Metadata:    map[string]string{"source": "rest-api"},
	}

	if err := stream.Send(chunk); err != nil {
		c.JSON(500, gin.H{"error": "failed to send chunk to gRPC"})
		return
	}

	// Close stream and wait for ACK
	ack, err := stream.CloseAndRecv()
	if err != nil {
		c.JSON(500, gin.H{"error": "failed to receive ack"})
		return
	}

	c.JSON(200, gin.H{
		"ack_session": ack.SessionId,
		"last_msg":    ack.LastMessageId,
		"success":     ack.Success,
	})
}
