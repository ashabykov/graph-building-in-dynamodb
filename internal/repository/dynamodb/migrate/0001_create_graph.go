package migrate

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	TableName = "graph"
	Version   = "20250405000000_graph_table"
)

type CreateGraphTable struct{}

func (m *CreateGraphTable) Version() string {
	return Version
}

func (m *CreateGraphTable) TableName() string {
	return TableName
}

func (m *CreateGraphTable) Up(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.CreateTableInput{
		// Define attribute definitions for the table
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"), // Partition Key
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("sk"), // Sort Key
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("ak"), // Доп. Sort Key для LSI
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		// Define the key schema for the table
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       types.KeyTypeRange,
			},
		},
		// Добавляем GSI по ak как partition key для быстрых запросов по area
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("sk-gsi"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("sk"),
						KeyType:       types.KeyTypeHash,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
			{
				IndexName: aws.String("ak-gsi"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("ak"),
						KeyType:       types.KeyTypeHash,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
		TableName: aws.String(TableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}

	// Add waiter after creating table to ensure it is active
	_, err := client.CreateTable(ctx, input)
	if err != nil {
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(TableName),
	}, 5*time.Minute)

	return err
}

func (m *CreateGraphTable) Down(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(TableName),
	}

	_, err := client.DeleteTable(ctx, input)
	return err
}
