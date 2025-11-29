package main

//nice
import (
	"log"
	"net"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/grpcserver"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	conversationv1.RegisterConversationStreamServer(grpcServer, grpcserver.NewConversationServer())

	log.Println("gRPC Conversation Stream Server running on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
