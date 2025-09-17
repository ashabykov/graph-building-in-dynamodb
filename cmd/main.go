package main

import (
	"context"
	"log"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/repository/dynamodb"
	"github.com/ashabykov/graph-building-in-dynamodb/internal/repository/dynamodb/graph/based-on-gsi"
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

	graphRepo := based_on_gsi.New(dynamoDb.Client)

	return nil
}

func main() {
	if err := Run(); err != nil {
		log.Fatal(err)
	}
}
