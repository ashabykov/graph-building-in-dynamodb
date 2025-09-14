package supply

import (
	"context"
	"errors"
	"time"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/demand"
	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/graph"
	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/supply"
)

type demandReader interface {
	FindBy(ctx context.Context, contractor supply.Supply) ([]demand.Demand, error)
}

type graphBuilder interface {
	UpsertEdges(ctx context.Context, edges ...graph.Edge) error
	RemoveEdges(ctx context.Context, edges ...graph.Edge) error
	ReadEdgesToNode(ctx context.Context, node graph.Node) ([]graph.Edge, error)
}

type UseCase struct {
	demandReader demandReader
	graphBuilder graphBuilder
}

func (uc *UseCase) Update(ctx context.Context, user supply.Supply) error {
	orders, err := uc.demandReader.FindBy(ctx, user)
	if err != nil {
		return err
	}
	if len(orders) == 0 {
		return nil
	}

	orderMap := make(map[string]struct{}, len(orders))
	for _, order := range orders {
		orderMap[order.ID] = struct{}{}
	}

	oldEdges, err := uc.graphBuilder.ReadEdgesToNode(ctx, graph.Node(user.ID))
	if err != nil {
		return err
	}

	toRem := make([]graph.Edge, len(oldEdges))
	for _, e := range oldEdges {
		if _, ok := orderMap[e.From.String()]; !ok {
			toRem = append(toRem, e)
		}
	}

	var (
		score = graph.Score(0.6576564) // TODO: set score based on some logic
		area  = graph.Area("area")     // TODO: set area
		ttl   = 15 * time.Minute       // TODO: make TTL configurable
	)

	addEdges := make([]graph.Edge, 0, len(orders))
	for _, order := range orders {
		addEdges = append(addEdges, graph.Edge{
			From:  graph.Node(order.ID),
			To:    graph.Node(user.ID),
			Score: score,
			Area:  area,
			TTL:   ttl,
		})
	}
	var errr error
	if err = uc.graphBuilder.UpsertEdges(ctx, addEdges...); err != nil {
		errr = errors.Join(errr, err)
	}
	if err = uc.graphBuilder.RemoveEdges(ctx, toRem...); err != nil {
		errr = errors.Join(errr, err)
	}
	return errr
}
