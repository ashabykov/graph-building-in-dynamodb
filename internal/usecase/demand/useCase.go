package demand

import (
	"context"
	"time"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/demand"
	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/graph"
	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/supply"
)

type supplyReader interface {
	FindBy(ctx context.Context, event demand.Demand) ([]supply.Supply, error)
}

type graphBuilder interface {
	UpsertEdges(ctx context.Context, edges ...graph.Edge) error
}

type UseCase struct {
	supplyReader supplyReader
	graphBuilder graphBuilder
}

func (uc *UseCase) Update(ctx context.Context, order demand.Demand) error {
	contractors, err := uc.supplyReader.FindBy(ctx, order)
	if err != nil {
		return err
	}
	if len(contractors) == 0 {
		return nil
	}
	var (
		score = graph.Score(0.6576564) // TODO: set score based on some logic
		area  = graph.Area("area")     // TODO: set area
		ttl   = 15 * time.Minute       // TODO: make TTL configurable
	)
	edges := make([]graph.Edge, 0, len(contractors))
	for _, contractor := range contractors {
		edges = append(edges, graph.Edge{
			From:  graph.Node(order.ID),
			To:    graph.Node(contractor.ID),
			Score: score,
			Area:  area,
			TTL:   ttl,
		})
	}
	return uc.graphBuilder.UpsertEdges(ctx, edges...)
}
