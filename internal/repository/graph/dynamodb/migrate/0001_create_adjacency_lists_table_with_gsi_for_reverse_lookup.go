package migrate

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type CreateAdjacencyListsTableWithGSI struct{}

func (m *CreateAdjacencyListsTableWithGSI) Version() string {
	return "20250405000000_graph_based_on_gsi_table"
}

func (m *CreateAdjacencyListsTableWithGSI) TableName() string {
	return "graph_based_on_gsi_tbl"
}

func (m *CreateAdjacencyListsTableWithGSI) Up(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.CreateTableInput{
		BillingMode: types.BillingModePayPerRequest,
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
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			// GSI для поиска по sk (обратный поиск)
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
			// GSI для поиска по ak (например, для поиска по области)
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
			},
		},
		TableName: aws.String(m.TableName()),
	}
	// Add waiter after creating table to ensure it is active
	_, err := client.CreateTable(ctx, input)
	if err != nil {
		return err
	}
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(
		ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(m.TableName()),
		},
		5*time.Minute,
	)
	return err
}

func (m *CreateAdjacencyListsTableWithGSI) Down(ctx context.Context, client *dynamodb.Client) error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(m.TableName()),
	}
	_, err := client.DeleteTable(ctx, input)
	if err != nil {
		return err
	}

	waiter := dynamodb.NewTableNotExistsWaiter(client)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(m.TableName()),
	}, 5*time.Minute)
}
