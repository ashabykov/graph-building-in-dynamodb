# Graph Building in DynamoDB

A Golang project demonstrating graph data structure operations using Amazon DynamoDB as the storage backend.

## Project Structure

```
.
├── cmd/                     # Application entry points
│   └── main.go             # Main application demonstrating graph operations
├── domain/                  # Domain models and interfaces
│   └── edge.go             # Edge entity and repository interface
├── repository/              # Data access layer
│   └── dynamodb.go         # DynamoDB implementation of EdgeRepository
├── docker-compose.yml       # DynamoDB Local setup
├── Makefile                # Build and development commands
└── go.mod                  # Go module dependencies
```

## Features

- **Edge-based Graph Representation**: Store and manage directed graph edges in DynamoDB
- **Repository Pattern**: Clean separation between domain logic and data persistence
- **DynamoDB Integration**: Full AWS SDK v2 integration with local development support
- **Docker Compose Setup**: Easy local development with DynamoDB Local
- **Comprehensive Operations**: Create, read, update, delete, and query graph edges

## Prerequisites

- Go 1.19 or later
- Docker and Docker Compose
- Make (optional, for convenience commands)

## Quick Start

1. **Start DynamoDB Local**:
   ```bash
   make docker-up
   # or
   docker-compose up -d
   ```

2. **Run the demo application**:
   ```bash
   make demo
   # or
   go run ./cmd/main.go
   ```

3. **Access DynamoDB Admin (optional)**:
   Open http://localhost:8001 in your browser to view and manage DynamoDB tables.

## Available Make Commands

- `make build` - Build the application binary
- `make run` - Run the application directly
- `make demo` - Start DynamoDB and run the demo
- `make docker-up` - Start DynamoDB Local
- `make docker-down` - Stop DynamoDB Local
- `make test` - Run tests
- `make clean` - Clean build artifacts
- `make fmt` - Format Go code
- `make deps` - Download and tidy dependencies

## Domain Model

### Edge
Represents a directed edge in the graph:
```go
type Edge struct {
    SourceID  string    // Source node identifier
    TargetID  string    // Target node identifier  
    Weight    float64   // Edge weight (optional)
    Label     string    // Edge label (optional)
    CreatedAt time.Time // Creation timestamp
    UpdatedAt time.Time // Last update timestamp
}
```

## Repository Interface

The `EdgeRepository` interface provides the following operations:
- `CreateEdge(edge *Edge) error`
- `GetEdge(sourceID, targetID string) (*Edge, error)`
- `GetEdgesBySource(sourceID string) ([]*Edge, error)`
- `GetEdgesByTarget(targetID string) ([]*Edge, error)`
- `UpdateEdge(edge *Edge) error`
- `DeleteEdge(sourceID, targetID string) error`
- `ListAllEdges() ([]*Edge, error)`

## DynamoDB Schema

### Table: graph-edges
- **Partition Key**: `source_id` (String)
- **Sort Key**: `target_id` (String)
- **Global Secondary Index**: `target-id-index`
  - **Partition Key**: `target_id` (String)

This schema enables efficient queries for:
- Finding all edges from a source node (using main table)
- Finding all edges to a target node (using GSI)
- Direct edge lookup by source and target

## Environment Variables

- `DYNAMODB_ENDPOINT` - DynamoDB endpoint URL (defaults to `http://localhost:8000`)

## Development

1. **Clone the repository**
2. **Install dependencies**: `make deps`
3. **Start DynamoDB**: `make docker-up`
4. **Run the application**: `make run`

## Production Deployment

For production use:
1. Remove or modify the static credentials in `cmd/main.go`
2. Configure proper AWS credentials (IAM roles, environment variables, etc.)
3. Update the DynamoDB endpoint to point to AWS DynamoDB
4. Consider implementing proper error handling and logging
5. Add monitoring and observability

## License

This project is provided as-is for educational and demonstration purposes.