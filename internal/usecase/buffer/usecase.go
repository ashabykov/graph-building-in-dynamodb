package buffer

import (
	"context"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/graph"
)

type graphBuilder interface {
	ReadAreaEdges(ctx context.Context, area graph.Area) ([]graph.Edge, error)
}

type matchMaker interface {
	Match(g map[graph.Node][]graph.Node) error
}

type UseCase struct {
	graphBuilder graphBuilder
	matchMaker   matchMaker
}

func (uc *UseCase) Tick(area graph.Area) error {
	edges, err := uc.graphBuilder.ReadAreaEdges(context.Background(), area)
	if err != nil {
		return nil
	}
	if len(edges) == 0 {
		return nil
	}
	G := make(map[graph.Node][]graph.Node)
	for _, e := range edges {
		G[e.From] = append(G[e.From], e.To)
		G[e.To] = append(G[e.To], e.From)
	}
	return uc.matchMaker.Match(G)
}
