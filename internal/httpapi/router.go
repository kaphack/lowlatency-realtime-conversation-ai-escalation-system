package httpapi

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ProducerAPI holds the gRPC client and DB connection.
type ProducerAPI struct {
	grpcClient conversationv1.ConversationStreamClient
	db         *sql.DB
}

// NewRouter initializes the REST router, gRPC client, and DB connection.
func NewRouter() *gin.Engine {
	// ----- gRPC connection -----
	grpcConnection, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}

	grpcClient := conversationv1.NewConversationStreamClient(grpcConnection)

	if godotenv.Load() != nil {
		log.Println("No .env file found, using system env")
	}

	// ----- MySQL connection -----
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&loc=Local",
		user, pass, host, port, name,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to open MySQL connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping MySQL: %v", err)
	}

	log.Println("Connected to MySQL " + dsn)

	api := &ProducerAPI{
		grpcClient: grpcClient,
		db:         db,
	}

	// ----- Gin router -----
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
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

	// Open client‑streaming RPC (use HTTP request context)
	ctx := c.Request.Context()
	stream, err := p.grpcClient.StreamConversation(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to open gRPC stream"})
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
		Metadata: map[string]string{
			"source": "rest-api",
		},
	}

	// Send to gRPC stream
	if err := stream.Send(chunk); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to send chunk to gRPC"})
		return
	}

	// OPTIONAL: Store in DB (adjust table/columns as per your schema)
	//_, err = p.db.Exec(
	//	`INSERT INTO conversation_chunks (session_id, message_id, sender, text, created_at)
	//     VALUES (?, ?, ?, ?, NOW())`,
	//	req.SessionID, msgID, req.Sender, req.Text,
	//)
	//if err != nil {
	//	log.Printf("failed to insert conversation chunk into DB: %v", err)
	//	// don’t block the response for DB failure, just log
	//}

	// Close stream and wait for ACK
	ack, err := stream.CloseAndRecv()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to receive ack"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ack_session": ack.SessionId,
		"last_msg":    ack.LastMessageId,
		"success":     ack.Success,
	})
}
