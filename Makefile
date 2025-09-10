.PHONY: build run clean test docker-up docker-down

# Build the application
build:
	go build -o bin/graph-app ./cmd

# Run the application
run:
	go run ./cmd/main.go

# Clean build artifacts
clean:
	rm -rf bin/

# Run tests
test:
	go test -v ./...

# Start DynamoDB local via Docker Compose
docker-up:
	docker compose up -d

# Stop DynamoDB local
docker-down:
	docker compose down

# Start DynamoDB and run the application
demo: docker-up
	@echo "Waiting for DynamoDB to be ready..."
	@sleep 5
	@echo "Running demo application..."
	go run ./cmd/main.go

# Format code
fmt:
	go fmt ./...

# Download dependencies
deps:
	go mod download
	go mod tidy

# Show project structure
tree:
	@echo "Project Structure:"
	@find . -type f -name "*.go" -o -name "*.yml" -o -name "*.md" -o -name "go.mod" | grep -v ".git" | sort