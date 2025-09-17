package migrate

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type CreateBasedOnAdjacencyListTable struct{}

func (m *CreateBasedOnAdjacencyListTable) Version() string {
	return "20250405000000_graph_based_on_adjacency_list_table"
}

func (m *CreateBasedOnAdjacencyListTable) TableName() string {
	return "graph_based_on_adjacency_list_tbl"
}

func (m *CreateBasedOnAdjacencyListTable) Up(ctx context.Context, client *dynamodb.Client) error {
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
				AttributeName: aws.String("ak"), // Доп. Sort Key для GSI
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
					ReadCapacityUnits:  aws.Int64(1000),
					WriteCapacityUnits: aws.Int64(1000),
				},
			},
		},
		TableName: aws.String(m.TableName()),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1000),
			WriteCapacityUnits: aws.Int64(1000),
		},
	}

	// Add waiter after creating table to ensure it is active
	_, err := client.CreateTable(ctx, input)
	if err != nil {
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(m.TableName()),
	}, 5*time.Minute)

	return err
}

func (m *CreateBasedOnAdjacencyListTable) Down(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(m.TableName()),
	}

	_, err := client.DeleteTable(ctx, input)
	return err
}
