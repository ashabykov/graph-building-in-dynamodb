package dynamodb

import (
	"context"
	"fmt"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/repository/graph/dynamodb/migrate"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Migration interface {
	Up(ctx context.Context, client *dynamodb.Client) error
	Down(ctx context.Context, client *dynamodb.Client) error
	Version() string
	TableName() string
}

func (d *DynamoDb) Migrate(ctx context.Context) error {
	migrations := []Migration{
		&migrate.CreateAdjacencyListsTableWithGSI{},
	}
	for _, migration := range migrations {
		if err := migration.Up(ctx, d.Client); err != nil {
			return fmt.Errorf("could not apply migration %s: %w", migration.Version(), err)
		}
	}
	return nil
}

func (d *DynamoDb) Rollback(ctx context.Context) error {
	migrations := []Migration{
		&migrate.CreateAdjacencyListsTableWithGSI{},
	}
	for _, migration := range migrations {
		if err := migration.Down(ctx, d.Client); err != nil {
			return fmt.Errorf("could not revert migration %s: %w", migration.Version(), err)
		}
	}
	return nil
}
