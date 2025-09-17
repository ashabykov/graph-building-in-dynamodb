package adjacency_list

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/graph"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const tableName = "graph_based_on_adjacency_list_tbl"

type edgeDTO struct {
	PK    string  `dynamodbav:"pk"`    // NODE#{FromNodeName}
	SK    string  `dynamodbav:"sk"`    // EDGE#{ToNodeName}
	AK    string  `dynamodbav:"ak"`    // AREA#{AreaName}
	Score float64 `dynamodbav:"score"` // score of the edge
	TTL   int64   `dynamodbav:"ttl"`   // time to live (epoch time in seconds)
}

type Repository struct {
	client *dynamodb.Client
}

func New(client *dynamodb.Client) *Repository {
	return &Repository{client: client}
}

func (r *Repository) Size(ctx context.Context) int {
	out, err := r.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
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

	_, err := r.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to batch write edges: %w", err)
	}
	return nil
}

// ReadOutEdges retrieves all edges associated with a specific node.
func (r *Repository) ReadOutEdges(ctx context.Context, node graph.Node) ([]graph.Edge, error) {
	key := node.Node()
	queryResult, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
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
		fromName := dto.PK[5:] // Убираем "NODE#"
		toName := dto.SK[5:]   // Убираем "EDGE#"
		area := dto.AK[5:]     // Убираем "AREA#"

		edges = append(edges, graph.Edge{
			From:  graph.Node(fromName),
			To:    graph.Node(toName),
			Score: graph.Score(dto.Score),
			Area:  graph.Area(area),
		})
	}
	return edges, nil
}

// ReadInEdges retrieves all edges directed to a specific node.
func (r *Repository) ReadInEdges(ctx context.Context, node graph.Node) ([]graph.Edge, error) {
	key := node.Edge()

	var last map[string]types.AttributeValue
	edges := make([]graph.Edge, 0)
	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
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

			fromName := ""
			if len(dto.PK) > 5 {
				fromName = dto.PK[5:]
			}
			toName := ""
			if len(dto.SK) > 5 {
				toName = dto.SK[5:]
			}
			areaName := ""
			if len(dto.AK) > 5 {
				areaName = dto.AK[5:]
			}

			edges = append(edges, graph.Edge{
				From:  graph.Node(fromName),
				To:    graph.Node(toName),
				Score: graph.Score(dto.Score),
				Area:  graph.Area(areaName),
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
	key := area.Key()

	var last map[string]types.AttributeValue
	edges := make([]graph.Edge, 0)

	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
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

			fromName := ""
			if len(dto.PK) > 5 {
				fromName = dto.PK[5:]
			}
			toName := ""
			if len(dto.SK) > 5 {
				toName = dto.SK[5:]
			}
			areaName := ""
			if len(dto.AK) > 5 {
				areaName = dto.AK[5:]
			}

			edges = append(edges, graph.Edge{
				From:  graph.Node(fromName),
				To:    graph.Node(toName),
				Score: graph.Score(dto.Score),
				Area:  graph.Area(areaName),
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
	_, err := r.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to batch write edges: %w", err)
	}
	return nil
}

// RemoveNodeEdges removes all edges associated with a specific node.
func (r *Repository) RemoveNodeEdges(ctx context.Context, node graph.Node) error {
	// Сначала получаем все рёбра от узла
	outEdges, err := r.ReadOutEdges(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to query node edges: %w", err)
	}
	// Затем получаем все рёбра к узлу
	inEdges, err := r.ReadInEdges(ctx, node)
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

func makeDTO(edges ...graph.Edge) []edgeDTO {
	items := make([]edgeDTO, 0, len(edges))
	for _, edge := range edges {
		ttl := time.Now().UTC().Add(edge.TTL).Unix()
		dto := edgeDTO{
			PK:    edge.From.Node(),
			SK:    edge.To.Edge(),
			AK:    edge.Area.Key(),
			Score: edge.Score.Float64(),
			TTL:   ttl,
		}
		items = append(items, dto)
	}
	return items
}
