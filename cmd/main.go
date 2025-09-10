package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ashabykov/graph-building-in-dynamodb/domain"
	"github.com/ashabykov/graph-building-in-dynamodb/repository"
)

const (
	tableName = "graph-edges"
	region    = "us-east-1"
)

func main() {
	// Get DynamoDB endpoint from environment variable or use default for local
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8000" // Default DynamoDB Local endpoint
	}

	// Configure AWS SDK for DynamoDB Local
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "dummy",
				SecretAccessKey: "dummy",
				SessionToken:    "",
				Source:          "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}

	// Create DynamoDB client
	client := dynamodb.NewFromConfig(cfg)

	// Create repository
	repo := repository.NewDynamoDBEdgeRepository(client, tableName)

	// Ensure table exists
	if err := ensureTableExists(client, tableName); err != nil {
		log.Fatalf("Failed to ensure table exists: %v", err)
	}

	// Demonstrate basic operations
	fmt.Println("Graph Building in DynamoDB - Demo")
	fmt.Println("==================================")

	// Create some sample edges
	edges := []*domain.Edge{
		{
			SourceID: "node-1",
			TargetID: "node-2",
			Weight:   1.5,
			Label:    "connects",
		},
		{
			SourceID: "node-2",
			TargetID: "node-3",
			Weight:   2.0,
			Label:    "flows",
		},
		{
			SourceID: "node-1",
			TargetID: "node-3",
			Weight:   3.0,
			Label:    "direct",
		},
	}

	// Create edges
	fmt.Println("\nCreating edges...")
	for _, edge := range edges {
		if err := repo.CreateEdge(edge); err != nil {
			log.Printf("Failed to create edge %s -> %s: %v", edge.SourceID, edge.TargetID, err)
		} else {
			fmt.Printf("Created edge: %s -> %s (weight: %.1f, label: %s)\n",
				edge.SourceID, edge.TargetID, edge.Weight, edge.Label)
		}
	}

	// Query edges by source
	fmt.Println("\nQuerying edges from node-1...")
	sourceEdges, err := repo.GetEdgesBySource("node-1")
	if err != nil {
		log.Printf("Failed to get edges by source: %v", err)
	} else {
		for _, edge := range sourceEdges {
			fmt.Printf("Found edge: %s -> %s (weight: %.1f, label: %s)\n",
				edge.SourceID, edge.TargetID, edge.Weight, edge.Label)
		}
	}

	// List all edges
	fmt.Println("\nListing all edges...")
	allEdges, err := repo.ListAllEdges()
	if err != nil {
		log.Printf("Failed to list all edges: %v", err)
	} else {
		fmt.Printf("Total edges in graph: %d\n", len(allEdges))
		for _, edge := range allEdges {
			fmt.Printf("  %s -> %s (weight: %.1f, label: %s)\n",
				edge.SourceID, edge.TargetID, edge.Weight, edge.Label)
		}
	}

	fmt.Println("\nDemo completed successfully!")
}

// ensureTableExists creates the DynamoDB table if it doesn't exist
func ensureTableExists(client *dynamodb.Client, tableName string) error {
	// Check if table exists
	_, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		fmt.Printf("Table %s already exists\n", tableName)
		return nil
	}

	// Create table
	fmt.Printf("Creating table %s...\n", tableName)
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("source_id"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("target_id"),
				KeyType:       types.KeyTypeRange,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("source_id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("target_id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("target-id-index"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("target_id"),
						KeyType:       types.KeyTypeHash,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
	}

	_, err = client.CreateTable(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Wait for table to be active
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, time.Minute*2)
	if err != nil {
		return fmt.Errorf("failed waiting for table: %w", err)
	}

	fmt.Printf("Table %s created successfully\n", tableName)
	return nil
}