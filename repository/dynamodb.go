package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ashabykov/graph-building-in-dynamodb/domain"
)

// DynamoDBEdgeRepository implements EdgeRepository using DynamoDB
type DynamoDBEdgeRepository struct {
	client    *dynamodb.Client
	tableName string
}

// NewDynamoDBEdgeRepository creates a new DynamoDB edge repository
func NewDynamoDBEdgeRepository(client *dynamodb.Client, tableName string) *DynamoDBEdgeRepository {
	return &DynamoDBEdgeRepository{
		client:    client,
		tableName: tableName,
	}
}

// CreateEdge creates a new edge in DynamoDB
func (r *DynamoDBEdgeRepository) CreateEdge(edge *domain.Edge) error {
	edge.CreatedAt = time.Now()
	edge.UpdatedAt = time.Now()

	item, err := attributevalue.MarshalMap(edge)
	if err != nil {
		return fmt.Errorf("failed to marshal edge: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      item,
	}

	_, err = r.client.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to create edge: %w", err)
	}

	return nil
}

// GetEdge retrieves an edge by source and target IDs
func (r *DynamoDBEdgeRepository) GetEdge(sourceID, targetID string) (*domain.Edge, error) {
	key := map[string]types.AttributeValue{
		"source_id": &types.AttributeValueMemberS{Value: sourceID},
		"target_id": &types.AttributeValueMemberS{Value: targetID},
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(r.tableName),
		Key:       key,
	}

	result, err := r.client.GetItem(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get edge: %w", err)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("edge not found")
	}

	var edge domain.Edge
	err = attributevalue.UnmarshalMap(result.Item, &edge)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
	}

	return &edge, nil
}

// GetEdgesBySource retrieves all edges from a source node
func (r *DynamoDBEdgeRepository) GetEdgesBySource(sourceID string) ([]*domain.Edge, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		KeyConditionExpression: aws.String("source_id = :source_id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":source_id": &types.AttributeValueMemberS{Value: sourceID},
		},
	}

	result, err := r.client.Query(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to query edges by source: %w", err)
	}

	var edges []*domain.Edge
	for _, item := range result.Items {
		var edge domain.Edge
		err = attributevalue.UnmarshalMap(item, &edge)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
		}
		edges = append(edges, &edge)
	}

	return edges, nil
}

// GetEdgesByTarget retrieves all edges to a target node using GSI
func (r *DynamoDBEdgeRepository) GetEdgesByTarget(targetID string) ([]*domain.Edge, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String("target-id-index"),
		KeyConditionExpression: aws.String("target_id = :target_id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":target_id": &types.AttributeValueMemberS{Value: targetID},
		},
	}

	result, err := r.client.Query(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to query edges by target: %w", err)
	}

	var edges []*domain.Edge
	for _, item := range result.Items {
		var edge domain.Edge
		err = attributevalue.UnmarshalMap(item, &edge)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
		}
		edges = append(edges, &edge)
	}

	return edges, nil
}

// UpdateEdge updates an existing edge
func (r *DynamoDBEdgeRepository) UpdateEdge(edge *domain.Edge) error {
	edge.UpdatedAt = time.Now()

	item, err := attributevalue.MarshalMap(edge)
	if err != nil {
		return fmt.Errorf("failed to marshal edge: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      item,
	}

	_, err = r.client.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to update edge: %w", err)
	}

	return nil
}

// DeleteEdge removes an edge from the graph
func (r *DynamoDBEdgeRepository) DeleteEdge(sourceID, targetID string) error {
	key := map[string]types.AttributeValue{
		"source_id": &types.AttributeValueMemberS{Value: sourceID},
		"target_id": &types.AttributeValueMemberS{Value: targetID},
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(r.tableName),
		Key:       key,
	}

	_, err := r.client.DeleteItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to delete edge: %w", err)
	}

	return nil
}

// ListAllEdges retrieves all edges (use with caution for large graphs)
func (r *DynamoDBEdgeRepository) ListAllEdges() ([]*domain.Edge, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(r.tableName),
	}

	result, err := r.client.Scan(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to scan edges: %w", err)
	}

	var edges []*domain.Edge
	for _, item := range result.Items {
		var edge domain.Edge
		err = attributevalue.UnmarshalMap(item, &edge)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
		}
		edges = append(edges, &edge)
	}

	return edges, nil
}