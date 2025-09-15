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
	ReadInEdges(ctx context.Context, node graph.Node) ([]graph.Edge, error)
}

type UseCase struct {
	demandReader demandReader
	graphBuilder graphBuilder
}

// Update обновляет ребра графа на основе нового события из топика водителей.
func (uc *UseCase) Update(ctx context.Context, user supply.Supply) error {
	orders, err := uc.demandReader.FindBy(ctx, user)
	if err != nil {
		return err
	}
	if len(orders) == 0 {
		return nil
	}

	oldEdges, err := uc.graphBuilder.ReadInEdges(ctx, graph.Node(user.ID))
	if err != nil {
		return err
	}

	var errr error

	// Remove old edges that are not in the current orders
	if len(oldEdges) > 0 {
		newOrders := make(map[string]struct{}, len(orders))
		for _, order := range orders {
			newOrders[order.ID] = struct{}{}
		}

		toRem := make([]graph.Edge, len(oldEdges))
		for _, e := range oldEdges {
			if _, ok := newOrders[e.From.String()]; !ok {
				toRem = append(toRem, e)
			}
		}
		if err = uc.graphBuilder.RemoveEdges(ctx, toRem...); err != nil {
			errr = errors.Join(errr, err)
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
	if err = uc.graphBuilder.UpsertEdges(ctx, addEdges...); err != nil {
		errr = errors.Join(errr, err)
	}
	return errr
}
