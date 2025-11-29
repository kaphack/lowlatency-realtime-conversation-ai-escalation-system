# Project name
APP_NAME := lowlatency-realtime-conversation-ai-escalation-system

# Directories
CMD_DIR := ./cmd
SERVER_CMD := $(CMD_DIR)/conversation-stream/server
PRODUCER_CMD := $(CMD_DIR)/conversation-stream/producer
PROTO_DIR := ./proto

# Proto files
PROTO_FILES := $(PROTO_DIR)/conversation.proto

.PHONY: all proto install-proto-tools run-server run-producer fmt tidy build clean

all: build

# Generate Go code from .proto definitions
proto:
	protoc --go_out=. --go-grpc_out=. $(PROTO_FILES)

# Install required protoc plugins
install-proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Run gRPC server (will be implemented in cmd/conversation-stream/server)
run-server:
	go run $(SERVER_CMD)

# Run conversation producer/client (will be implemented in cmd/conversation-stream/producer)
run-producer:
	go run $(PRODUCER_CMD)

# Format all Go files
fmt:
	go fmt ./...

# Go modules tidy
tidy:
	go mod tidy

# Build all Go packages
build:
	go build ./...

# Clean build artifacts (extend as needed later)
clean:
	rm -rf bin

run-rest:
	go run ./cmd/conversation-stream/rest
