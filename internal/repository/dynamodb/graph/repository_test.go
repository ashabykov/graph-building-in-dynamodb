package graph

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ashabykov/graph-building-in-dynamodb/internal/domain/graph"
	"github.com/ashabykov/graph-building-in-dynamodb/internal/repository/dynamodb"
)

func TestRepository_UpsertEdges(t *testing.T) {
	testCases := []struct {
		name         string
		edges        []graph.Edge
		expectedSize int
	}{
		{
			name: "Insert single edge",
			edges: []graph.Edge{
				{
					From:  "A",
					To:    "B",
					Area:  "Area1",
					Score: 10,
					TTL:   24 * time.Hour,
				},
			},
			expectedSize: 1,
		},
		{
			name: "Insert multiple edges",
			edges: []graph.Edge{
				{
					From:  "B",
					To:    "C",
					Area:  "Area2",
					Score: 20,
					TTL:   24 * time.Hour,
				},
				{
					From:  "C",
					To:    "B",
					Area:  "Area2",
					Score: 20,
					TTL:   24 * time.Hour,
				},
				{
					From:  "A",
					To:    "D",
					Area:  "Area2",
					Score: 15,
					TTL:   24 * time.Hour,
				},
				{
					From:  "D",
					To:    "E",
					Area:  "Area2",
					Score: 25,
				},
				{
					From:  "E",
					To:    "F",
					Area:  "Area2",
					Score: 30,
					TTL:   24 * time.Hour,
				},
				{
					From:  "F",
					To:    "G",
					Area:  "Area2",
					Score: 35,
					TTL:   24 * time.Hour,
				},
			},
			expectedSize: 6,
		},
		{
			name: "Insert duplicate edges",
			edges: []graph.Edge{
				{
					From:  "A",
					To:    "B",
					Area:  "Area1",
					Score: 10,
					TTL:   24 * time.Hour,
				},
				{
					From:  "A",
					To:    "B",
					Area:  "Area1",
					Score: 15, // Updated score
					TTL:   24 * time.Hour,
				},
			},
			expectedSize: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			db, err := dynamodb.NewTestDatabase()

			assert.NoError(t, err)

			err = db.Migrate(context.Background())

			assert.NoError(t, err)

			defer db.RollbackMigration(context.Background())

			repo := New(db.Client)

			got := repo.UpsertEdges(context.Background(), tc.edges...)
			assert.NoError(t, got)

			size := repo.Size(context.Background())
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestRepository_UpdateEdges(t *testing.T) {
	t.Run("Update existing edge", func(t *testing.T) {
		db, err := dynamodb.NewTestDatabase()

		assert.NoError(t, err)

		err = db.Migrate(context.Background())

		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edge := graph.Edge{
			From:  "A",
			To:    "B",
			Area:  "Area1",
			Score: 10,
			TTL:   24 * time.Hour,
		}

		err = repo.UpsertEdges(context.Background(), edge)
		assert.NoError(t, err)

		// Update the edge with a new score
		updatedEdge := graph.Edge{
			From:  "A",
			To:    "B",
			Area:  "Area1",
			Score: 20, // Updated score
			TTL:   24 * time.Hour,
		}

		err = repo.UpsertEdges(context.Background(), updatedEdge)
		assert.NoError(t, err)

		size := repo.Size(context.Background())
		assert.Equal(t, 1, size)

		edges, err := repo.ReadOutEdges(context.Background(), "A")
		assert.NoError(t, err)
		assert.Len(t, edges, 1)
		assert.Equal(t, graph.Score(20), edges[0].Score) // Score should be updated to 20
	})
	t.Run("Update existing edge with diff area", func(t *testing.T) {
		db, err := dynamodb.NewTestDatabase()
		assert.NoError(t, err)

		err = db.Migrate(context.Background())
		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edge := graph.Edge{
			From:  "A",
			To:    "B",
			Area:  "Area1",
			Score: 10,
			TTL:   24 * time.Hour,
		}
		err = repo.UpsertEdges(context.Background(), edge)
		assert.NoError(t, err)

		// Update the edge with a different area
		updatedEdge := graph.Edge{
			From:  "A",
			To:    "B",
			Area:  "Area2", // Different area
			Score: 10,
			TTL:   24 * time.Hour,
		}
		err = repo.UpsertEdges(context.Background(), updatedEdge)
		assert.NoError(t, err)

		size := repo.Size(context.Background())
		assert.Equal(t, 1, size)

		edges, err := repo.ReadOutEdges(context.Background(), "A")
		assert.NoError(t, err)
		assert.Len(t, edges, 1)
		assert.Equal(t, graph.Area("Area2"), edges[0].Area) // Area should be updated to Area2
	})
}

func TestRepository_ReadOutEdges(t *testing.T) {
	t.Run("Get edges from dynamodb", func(t *testing.T) {

		db, err := dynamodb.NewTestDatabase()

		assert.NoError(t, err)

		err = db.Migrate(context.Background())

		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edges := []graph.Edge{
			{
				From:  "A", // 1
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
				TTL:   24 * time.Hour,
			},
			{
				From:  "B",
				To:    "C",
				Area:  "Area1",
				Score: 20,
			},
			{
				From:  "C",
				To:    "D",
				Area:  "Area1",
				Score: 0.4556456,
				TTL:   24 * time.Hour,
			},
			{
				From:  "A", // 2
				To:    "С",
				Area:  "Area1",
				Score: 45.54656,
				TTL:   24 * time.Hour,
			},
			{
				From:  "A", // 3
				To:    "G",
				Area:  "Area1",
				Score: 344,
				TTL:   24 * time.Hour,
			},
		}

		expected := []graph.Edge{
			{
				From:  "A",
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
			},
			{
				From:  "A", // 3
				To:    "G",
				Area:  "Area1",
				Score: 344,
			},
			{
				From:  "A", // 2
				To:    "С",
				Area:  "Area1",
				Score: 45.54656,
			},
		}

		err = repo.UpsertEdges(context.Background(), edges...)
		assert.NoError(t, err)

		retrievedEdges, err := repo.ReadOutEdges(context.Background(), "A")
		assert.NoError(t, err)

		assert.EqualValues(t, expected, retrievedEdges)
	})
}

func TestRepository_ReadInEdges(t *testing.T) {
	t.Run("Get edges from dynamodb", func(t *testing.T) {
		db, err := dynamodb.NewTestDatabase()
		assert.NoError(t, err)

		err = db.Migrate(context.Background())
		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edges := []graph.Edge{
			{
				From:  "A",
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
				TTL:   24 * time.Hour,
			},
			{
				From:  "B",
				To:    "C",
				Area:  "Area1",
				Score: 20,
				TTL:   24 * time.Hour,
			},
			{
				From:  "C",
				To:    "D",
				Area:  "Area1",
				Score: 0.4556456,
				TTL:   24 * time.Hour,
			},
			{
				From:  "E",
				To:    "B",
				Area:  "Area1",
				Score: 45.54656,
				TTL:   24 * time.Hour,
			},
		}

		expected := []graph.Edge{
			{
				From:  "A",
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
			},
			{
				From:  "E",
				To:    "B",
				Area:  "Area1",
				Score: 45.54656,
			},
		}

		err = repo.UpsertEdges(context.Background(), edges...)
		assert.NoError(t, err)

		retrievedEdges, err := repo.ReadInEdges(context.Background(), "B")
		assert.NoError(t, err)
		sort.Slice(retrievedEdges, func(i, j int) bool {
			return retrievedEdges[i].From < retrievedEdges[j].From && retrievedEdges[i].To < retrievedEdges[j].To
		})
		assert.EqualValues(t, expected, retrievedEdges)
	})
}

func TestRepository_ReadAreaEdges(t *testing.T) {
	t.Run("Get edges from dynamodb", func(t *testing.T) {
		db, err := dynamodb.NewTestDatabase()

		assert.NoError(t, err)

		err = db.Migrate(context.Background())

		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edges := []graph.Edge{
			{
				From:  "A", // 1
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
				TTL:   24 * time.Hour,
			},
			{
				From:  "ttt",
				To:    "ggg",
				Area:  "Area2",
				Score: 0.4556456,
				TTL:   24 * time.Hour,
			},
			{
				From:  "C",
				To:    "B",
				Area:  "Area1",
				Score: 25,
				TTL:   24 * time.Hour,
			},
		}

		expected := []graph.Edge{
			{
				From:  "A",
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
			},
			{
				From:  "C",
				To:    "B",
				Area:  "Area1",
				Score: 25,
			},
		}

		err = repo.UpsertEdges(context.Background(), edges...)
		assert.NoError(t, err)

		retrievedEdges, err := repo.ReadAreaEdges(context.Background(), "Area1")
		assert.NoError(t, err)
		sort.Slice(retrievedEdges, func(i, j int) bool {
			return retrievedEdges[i].From < retrievedEdges[j].From && retrievedEdges[i].To < retrievedEdges[j].To
		})
		assert.EqualValues(t, expected, retrievedEdges)
	})
}

func TestRepository_RemoveEdges(t *testing.T) {
	t.Run("Remove edges from dynamodb", func(t *testing.T) {
		db, err := dynamodb.NewTestDatabase()

		assert.NoError(t, err)

		err = db.Migrate(context.Background())

		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edges := []graph.Edge{
			{
				From:  "A", // 1
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
				TTL:   24 * time.Hour,
			},
			{
				From:  "B",
				To:    "C",
				Area:  "Area1",
				Score: 20,
			},
			{
				From:  "C",
				To:    "D",
				Area:  "Area1",
				Score: 0.4556456,
				TTL:   24 * time.Hour,
			},
		}

		remove := []graph.Edge{
			{
				From: "A",
				To:   "B",
				Area: "Area1",
			},
			{
				From: "R", // Non-existing edge
				To:   "Q",
				Area: "Area1",
			},
		}

		err = repo.UpsertEdges(context.Background(), edges...)
		assert.NoError(t, err)

		size := repo.Size(context.Background())
		assert.Equal(t, 3, size)

		err = repo.RemoveEdges(context.Background(), remove...)
		assert.NoError(t, err)

		size = repo.Size(context.Background())
		assert.Equal(t, 2, size)

		retrievedEdges, err := repo.ReadOutEdges(context.Background(), "A")
		assert.NoError(t, err)
		assert.Len(t, retrievedEdges, 0) // All edges from A should be removed

		retrievedEdges, err = repo.ReadOutEdges(context.Background(), "B")
		assert.NoError(t, err)
		assert.Len(t, retrievedEdges, 1) // Only edge to C should remain
		assert.Equal(t, graph.Node("C"), retrievedEdges[0].To)
	})
}

func TestRepository_RemoveNodeEdges(t *testing.T) {
	t.Run("Remove all edges of a node from dynamodb", func(t *testing.T) {

		db, err := dynamodb.NewTestDatabase()
		assert.NoError(t, err)

		err = db.Migrate(context.Background())
		assert.NoError(t, err)

		defer db.RollbackMigration(context.Background())

		repo := New(db.Client)

		edges := []graph.Edge{
			{
				From:  "A",
				To:    "B",
				Area:  "Area1",
				Score: 67.77868,
				TTL:   24 * time.Hour,
			},
			{
				From:  "B",
				To:    "C",
				Area:  "Area1",
				Score: 20,
			},
			{
				From:  "C",
				To:    "D",
				Area:  "Area1",
				Score: 0.4556456,
				TTL:   24 * time.Hour,
			},
			{
				From:  "E",
				To:    "B",
				Area:  "Area1",
				Score: 45.54656,
				TTL:   24 * time.Hour,
			},
		}

		err = repo.UpsertEdges(context.Background(), edges...)
		assert.NoError(t, err)

		size := repo.Size(context.Background())
		assert.Equal(t, 4, size)

		// Remove all edges associated with node "B"
		err = repo.RemoveNodeEdges(context.Background(), "B")
		assert.NoError(t, err)

		size = repo.Size(context.Background())
		assert.Equal(t, 1, size) // Only edges not related to "B" should remain

		// Verify no edges from "B"
		retrievedEdges, err := repo.ReadOutEdges(context.Background(), "B")
		assert.NoError(t, err)
		assert.Len(t, retrievedEdges, 0)

		// Verify edges from "A"
		retrievedEdges, err = repo.ReadOutEdges(context.Background(), "A")
		assert.NoError(t, err)
		assert.Len(t, retrievedEdges, 0)

		// Verify edges from "C"
		retrievedEdges, err = repo.ReadOutEdges(context.Background(), "C")
		assert.NoError(t, err)
		assert.Len(t, retrievedEdges, 1)
		assert.Equal(t, graph.Node("D"), retrievedEdges[0].To) // Edge to D should still exist
	})
}
