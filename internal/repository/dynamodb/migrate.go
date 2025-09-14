package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/repository/dynamodb/migrate"
)

type Migration interface {
	Up(ctx context.Context, client *dynamodb.Client) error
	Down(ctx context.Context, client *dynamodb.Client) error
	Version() string
	TableName() string
}

func (d *DynamoDb) Migrate(ctx context.Context) error {
	migrations := []Migration{
		&migrate.CreateGraphTable{},
	}
	for _, migration := range migrations {
		//if applied, err := d.isMigrationApplied(ctx, migration.Version()); err != nil {
		//	return fmt.Errorf("could not check migration status: %w", err)
		//} else if applied {
		//	continue
		//}

		// Apply migration
		if err := migration.Up(ctx, d.Client); err != nil {
			return fmt.Errorf("could not apply migration %s: %w", migration.Version(), err)
		}

		// Record migration using tags
		//if err := d.recordMigration(ctx, migration.Version(), migration.TableName()); err != nil {
		//	return fmt.Errorf("could not record migration %s: %w", migration.Version(), err)
		//}

	}
	return nil
}

func (d *DynamoDb) RollbackMigration(ctx context.Context) error {
	migrations := []Migration{
		&migrate.CreateGraphTable{},
	}
	for _, migration := range migrations {
		//if applied, err := d.isMigrationApplied(ctx, migration.Version()); err != nil {
		//	return fmt.Errorf("could not check migration status: %w", err)
		//} else if !applied {
		//	continue
		//}

		// Revert migration
		if err := migration.Down(ctx, d.Client); err != nil {
			return fmt.Errorf("could not revert migration %s: %w", migration.Version(), err)
		}
		// Note: In a real-world scenario, you might want to remove the migration tag here.
		// However, AWS does not support untagging resources in the same way as tagging.
		// You would need to implement that logic if required.
	}
	return nil
}

func (d *DynamoDb) isMigrationApplied(ctx context.Context, version string) (bool, error) {
	input := &resourcegroupstaggingapi.GetResourcesInput{
		TagFilters: []types.TagFilter{
			{
				Key:    aws.String("Migration"),
				Values: []string{version},
			},
		},
		ResourceTypeFilters: []string{"dynamodb:table"},
	}
	result, err := d.TaggingClient.GetResources(ctx, input)
	if err != nil {
		return false, fmt.Errorf("failed to check migration tags: %w", err)
	}
	return len(result.ResourceTagMappingList) > 0, nil
}

func (d *DynamoDb) recordMigration(ctx context.Context, version string, tableName string) error {
	// Get table ARN
	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	tableDesc, err := d.Client.DescribeTable(ctx, describeInput)
	if err != nil {
		return fmt.Errorf("failed to get table ARN: %w", err)
	}

	// Tag the table
	input := &resourcegroupstaggingapi.TagResourcesInput{
		ResourceARNList: []string{*tableDesc.Table.TableArn},
		Tags: map[string]string{
			"Migration":  version,
			"MigratedAt": time.Now().UTC().Format(time.RFC3339),
		},
	}

	_, err = d.TaggingClient.TagResources(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to tag resource: %w", err)
	}

	return nil
}
