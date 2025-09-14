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

func (n Node) String() string {
	return string(n)
}

func (n Node) Node() string {
	return "NODE#" + n.String()
}

func (n Node) Edge() string {
	return "EDGE#" + n.String()
}

func (a Area) Key() string {
	return "AREA#" + string(a)
}

func (s Score) Float64() float64 {
	return float64(s)
}
