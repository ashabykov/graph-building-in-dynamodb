package main

import (
	"context"
	"log"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/repository/graph/dynamodb"
)

func Run() error {

	cfg, err := LoadConfig()

	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	dynamoDb, err := dynamodb.NewDatabase(cfg.LocalDynamoEndpoint, cfg.AwsConfig)
	if err != nil {
		log.Fatalf("Error creating DynamoDB client: %v", err)
	}

	if migrateErr := dynamoDb.Migrate(context.Background()); migrateErr != nil {
		log.Fatal("Failed to migrate the database")
		return err
	}

	graphRepo := adjacency_lists_with_gsi_for_reverse_lookup.New(dynamoDb.Client)

	return nil
}

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}
