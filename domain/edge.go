package domain

import "time"

// Edge represents a directed edge in a graph stored in DynamoDB
type Edge struct {
	// SourceID is the identifier of the source node
	SourceID string `json:"source_id" dynamodbav:"source_id"`
	
	// TargetID is the identifier of the target node
	TargetID string `json:"target_id" dynamodbav:"target_id"`
	
	// Weight represents the weight/cost of the edge (optional)
	Weight float64 `json:"weight,omitempty" dynamodbav:"weight,omitempty"`
	
	// Label provides additional metadata for the edge (optional)
	Label string `json:"label,omitempty" dynamodbav:"label,omitempty"`
	
	// CreatedAt timestamp when the edge was created
	CreatedAt time.Time `json:"created_at" dynamodbav:"created_at"`
	
	// UpdatedAt timestamp when the edge was last updated
	UpdatedAt time.Time `json:"updated_at" dynamodbav:"updated_at"`
}

// EdgeRepository defines the interface for edge operations
type EdgeRepository interface {
	// CreateEdge creates a new edge in the graph
	CreateEdge(edge *Edge) error
	
	// GetEdge retrieves an edge by source and target IDs
	GetEdge(sourceID, targetID string) (*Edge, error)
	
	// GetEdgesBySource retrieves all edges from a source node
	GetEdgesBySource(sourceID string) ([]*Edge, error)
	
	// GetEdgesByTarget retrieves all edges to a target node
	GetEdgesByTarget(targetID string) ([]*Edge, error)
	
	// UpdateEdge updates an existing edge
	UpdateEdge(edge *Edge) error
	
	// DeleteEdge removes an edge from the graph
	DeleteEdge(sourceID, targetID string) error
	
	// ListAllEdges retrieves all edges (use with caution for large graphs)
	ListAllEdges() ([]*Edge, error)
}