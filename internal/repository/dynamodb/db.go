package dynamodb

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
)

type DynamoDb struct {
	Client        *dynamodb.Client
	TaggingClient *resourcegroupstaggingapi.Client
}

func NewDatabase(endpoint string, config aws.Config) (*DynamoDb, error) {

	var client *dynamodb.Client

	if endpoint != "" {
		// Создаём клиент с переопределённым endpoint для локального DynamoDB (Docker)
		// Используем BaseEndpoint для локального DynamoDB
		client = dynamodb.NewFromConfig(
			config, func(o *dynamodb.Options) {
				o.BaseEndpoint = aws.String(endpoint)
			},
		)
	} else {
		client = dynamodb.NewFromConfig(config)
	}

	if client == nil {
		log.Fatal("Failed to create DynamoDB client")
	}

	taggingClient := resourcegroupstaggingapi.NewFromConfig(config)
	if taggingClient == nil {
		log.Fatal("Failed to create Resource Groups Tagging API client")
	}
	return &DynamoDb{
		Client:        client,
		TaggingClient: taggingClient,
	}, nil
}

func NewTestDatabase() (*DynamoDb, error) {
	endpoint := "http://localhost:8000" // Локальный endpoint для DynamoDB
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	return NewDatabase(endpoint, cfg)
}
