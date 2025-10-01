package graph

import "time"

type (
	Node  string
	Area  string
	Score float64

	Edge struct {
		From, To Node
		Score    Score

		// Metadata for graph
		Area Area
		TTL  time.Duration
	}
)

func (e Edge) Demand() string {
	return e.From.Demand()
}

func (e Edge) Supply() string {
	return e.To.Supply()
}

func (n Node) String() string {
	return string(n)
}

func (n Node) Demand() string {
	return "DEMAND#" + n.String()
}

func (n Node) Supply() string {
	return "SUPPLY#" + n.String()
}

func (a Area) Area() string {
	return "AREA#" + string(a)
}

func (s Score) Float64() float64 {
	return float64(s)
}
