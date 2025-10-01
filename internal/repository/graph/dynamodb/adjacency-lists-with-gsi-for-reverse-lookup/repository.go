package adjacency_lists_with_gsi_for_reverse_lookup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/graph"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	TableName = "graph_based_on_gsi_tbl"
	batchSize = 25 // Максимальный размер партии для BatchWriteItem
)

type edgeDTO struct {
	PK    string  `dynamodbav:"pk"`    // DEMAND#{FromNodeName}
	SK    string  `dynamodbav:"sk"`    // SUPPLY#{ToNodeName}
	AK    string  `dynamodbav:"ak"`    // AREA#{AreaName}
	Score float64 `dynamodbav:"score"` // score of the edge
	TTL   int64   `dynamodbav:"ttl"`   // time to live (epoch time in seconds)
}

func parseDemand(pk string) graph.Node {
	return graph.Node(pk[7:]) // Убираем "DEMAND#"
}

func parseSupply(sk string) graph.Node {
	return graph.Node(sk[7:]) // Убираем "SUPPLY#"
}

func parseArea(ak string) graph.Area {
	return graph.Area(ak[5:]) // Убираем "AREA#"
}

type Repository struct {
	client *dynamodb.Client
}

func New(client *dynamodb.Client) *Repository {
	return &Repository{client: client}
}

func (r *Repository) Size(ctx context.Context) int {
	out, err := r.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(TableName),
	})
	if err != nil || out.Table == nil || out.Table.ItemCount == nil {
		return 0
	}
	return int(*out.Table.ItemCount)
}

// UpsertEdges adds or updates edges in the graph.
func (r *Repository) UpsertEdges(ctx context.Context, edges ...graph.Edge) error {
	if len(edges) == 0 {
		return nil
	}

	seen := make(map[string]types.WriteRequest, len(edges))
	for _, dto := range makeDTO(edges...) {
		av, err := attributevalue.MarshalMap(dto)
		if err != nil {
			return fmt.Errorf("failed to marshal edge: %w", err)
		}
		av["ttl"] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(dto.TTL, 10),
		}
		key := dto.PK + "|" + dto.SK
		seen[key] = types.WriteRequest{
			PutRequest: &types.PutRequest{Item: av},
		}
	}

	// convert map to slice
	writeRequests := make([]types.WriteRequest, 0, len(seen))
	for _, wr := range seen {
		writeRequests = append(writeRequests, wr)
	}

	var writeErr error
	for requests := range slices.Chunk(writeRequests, batchSize) {
		if _, err := r.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				TableName: requests,
			},
		}); err != nil {
			writeErr = errors.Join(writeErr, err)
		}
	}
	return writeErr
}

// ReadDemandEdges retrieves all edges from the demand node.
func (r *Repository) ReadDemandEdges(ctx context.Context, node graph.Node) ([]graph.Edge, error) {
	key := node.Demand()
	queryResult, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(TableName),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query edges: %w", err)
	}

	edges := make([]graph.Edge, 0, len(queryResult.Items))
	for _, item := range queryResult.Items {
		var dto edgeDTO

		err = attributevalue.UnmarshalMap(item, &dto)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
		}

		// Извлекаем имена узлов из ключей
		edges = append(edges, graph.Edge{
			From:  parseDemand(dto.PK),
			To:    parseSupply(dto.SK),
			Area:  parseArea(dto.AK),
			Score: graph.Score(dto.Score),
		})
	}
	return edges, nil
}

// ReadSupplyEdges retrieves all edges directed to the supply node.
func (r *Repository) ReadSupplyEdges(ctx context.Context, node graph.Node) ([]graph.Edge, error) {
	key := node.Supply()

	var last map[string]types.AttributeValue
	edges := make([]graph.Edge, 0)
	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(TableName),
			IndexName:              aws.String("sk-gsi"),
			KeyConditionExpression: aws.String("sk = :sk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":sk": &types.AttributeValueMemberS{
					Value: key,
				},
			},
			ExclusiveStartKey: last,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query edges by area: %w", err)
		}

		for _, item := range out.Items {
			var dto edgeDTO

			if err = attributevalue.UnmarshalMap(item, &dto); err != nil {
				return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
			}

			edges = append(edges, graph.Edge{
				From:  parseSupply(dto.PK),
				To:    parseDemand(dto.SK),
				Area:  parseArea(dto.AK),
				Score: graph.Score(dto.Score),
			})
		}

		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			break
		}
		last = out.LastEvaluatedKey
	}
	return edges, nil
}

// ReadAreaEdges retrieves all edges associated with a specific area.
func (r *Repository) ReadAreaEdges(ctx context.Context, area graph.Area) ([]graph.Edge, error) {
	key := area.Area()

	var last map[string]types.AttributeValue
	edges := make([]graph.Edge, 0)

	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(TableName),
			IndexName:              aws.String("ak-gsi"),
			KeyConditionExpression: aws.String("ak = :ak"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ak": &types.AttributeValueMemberS{
					Value: key,
				},
			},
			ExclusiveStartKey: last,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query edges by area: %w", err)
		}

		for _, item := range out.Items {
			var dto edgeDTO
			if err = attributevalue.UnmarshalMap(item, &dto); err != nil {
				return nil, fmt.Errorf("failed to unmarshal edge: %w", err)
			}
			edges = append(edges, graph.Edge{
				From:  parseDemand(dto.PK),
				To:    parseSupply(dto.SK),
				Area:  parseArea(dto.AK),
				Score: graph.Score(dto.Score),
			})
		}

		if out.LastEvaluatedKey == nil || len(out.LastEvaluatedKey) == 0 {
			break
		}
		last = out.LastEvaluatedKey
	}

	return edges, nil
}

// RemoveEdges removes specific edges from the graph.
func (r *Repository) RemoveEdges(ctx context.Context, edges ...graph.Edge) error {
	if len(edges) == 0 {
		return nil
	}

	seen := make(map[string]types.WriteRequest, len(edges))
	for _, dto := range makeDTO(edges...) {
		key := dto.PK + "|" + dto.SK
		seen[key] = types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{
						Value: dto.PK,
					},
					"sk": &types.AttributeValueMemberS{
						Value: dto.SK,
					},
				},
			},
		}
	}
	writeRequests := make([]types.WriteRequest, 0, len(seen))
	for _, edge := range seen {
		writeRequests = append(writeRequests, types.WriteRequest{
			DeleteRequest: edge.DeleteRequest,
		})
	}

	var deleteErr error
	for requests := range slices.Chunk(writeRequests, batchSize) {
		if _, err := r.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				TableName: requests,
			},
		}); err != nil {
			deleteErr = errors.Join(deleteErr, err)
		}
	}
	return deleteErr
}

// RemoveNodeEdges removes all edges associated with a specific node.
func (r *Repository) RemoveNodeEdges(ctx context.Context, node graph.Node) error {
	// Сначала получаем все рёбра от узла
	outEdges, err := r.ReadDemandEdges(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to query node edges: %w", err)
	}
	// Затем получаем все рёбра к узлу
	inEdges, err := r.ReadSupplyEdges(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to query node edges: %w", err)
	}
	edges := append(outEdges, inEdges...)
	if len(edges) == 0 {
		return nil // Нет рёбер для удаления
	}

	// Удаляем все рёбра данного узла
	err = r.RemoveEdges(ctx, edges...)
	if err != nil {
		return fmt.Errorf("failed to remove node edges: %w", err)
	}
	return nil
}

// RemoveDemandEdges удаляет все исходящие рёбра узла (pk = DEMAND#...).
// Делает Query только по ключам (pk, sk) и батчевое удаление.
func (r *Repository) RemoveDemandEdges(ctx context.Context, node graph.Node) error {
	pk := node.Demand()
	var last map[string]types.AttributeValue
	var removeErr error
	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(TableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: pk},
			},
			ProjectionExpression: aws.String("pk, sk"),
			ExclusiveStartKey:    last,
		})
		if err != nil {
			return fmt.Errorf("query out-edges: %w", err)
		}
		if len(out.Items) == 0 {
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
		}

		writeBatch := make([]types.WriteRequest, 0, len(out.Items))
		for _, item := range out.Items {
			pkAttr := item["pk"].(*types.AttributeValueMemberS).Value
			skAttr := item["sk"].(*types.AttributeValueMemberS).Value
			writeBatch = append(writeBatch, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: pkAttr},
						"sk": &types.AttributeValueMemberS{Value: skAttr},
					},
				},
			})
		}

		for requests := range slices.Chunk(writeBatch, batchSize) {
			if _, err = r.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{
					TableName: requests,
				},
			}); err != nil {
				removeErr = errors.Join(removeErr, err)
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		last = out.LastEvaluatedKey
	}

	if removeErr != nil {
		return fmt.Errorf("remove out-edges: %w", removeErr)
	}
	return nil
}

func makeDTO(edges ...graph.Edge) []edgeDTO {
	items := make([]edgeDTO, 0, len(edges))
	for _, edge := range edges {
		ttl := time.Now().UTC().Add(edge.TTL).Unix()
		dto := edgeDTO{
			PK:    edge.Demand(),
			SK:    edge.Supply(),
			AK:    edge.Area.Area(),
			Score: edge.Score.Float64(),
			TTL:   ttl,
		}
		items = append(items, dto)
	}
	return items
}
