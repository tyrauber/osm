package osmpbf_test

import (
	"fmt"
	"os"
	"time"

	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// Example_encoder demonstrates how to use the encoder to write OSM data to a PBF file.
func Example_encoder() {
	// Open a file for writing
	file, err := os.Create("example.osm.pbf")
	if err != nil {
		fmt.Printf("could not create file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create an encoder with options
	encoder := osmpbf.NewEncoder(file,
		osmpbf.WithWritingProgram("osmpbf-example"),
		osmpbf.WithCompression(true),
		osmpbf.WithBoundingBox(51.5, -0.2, 51.6, -0.1), // London area
	)

	// Start the encoder
	errChan, err := encoder.Start()
	if err != nil {
		fmt.Printf("could not start encoder: %v", err)
		os.Exit(1)
	}

	// Create and write some nodes
	for i := 1; i <= 10; i++ {
		node := &osm.Node{
			ID:          osm.NodeID(i),
			Lat:         51.5 + float64(i)*0.01,
			Lon:         -0.2 + float64(i)*0.01,
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "example_user",
			Visible:     true,
			Tags: osm.Tags{
				{Key: "example", Value: fmt.Sprintf("node_%d", i)},
			},
		}
		encoder.WriteNode(node)
	}

	// Create and write a way
	way := &osm.Way{
		ID:          osm.WayID(100),
		Version:     1,
		Timestamp:   time.Now(),
		ChangesetID: osm.ChangesetID(123),
		UserID:      osm.UserID(456),
		User:        "example_user",
		Visible:     true,
		Nodes: osm.WayNodes{
			{ID: osm.NodeID(1)},
			{ID: osm.NodeID(2)},
			{ID: osm.NodeID(3)},
			{ID: osm.NodeID(4)},
			{ID: osm.NodeID(5)},
		},
		Tags: osm.Tags{
			{Key: "highway", Value: "residential"},
			{Key: "name", Value: "Example Street"},
		},
	}
	encoder.WriteWay(way)

	// Create and write a relation
	relation := &osm.Relation{
		ID:          osm.RelationID(200),
		Version:     1,
		Timestamp:   time.Now(),
		ChangesetID: osm.ChangesetID(123),
		UserID:      osm.UserID(456),
		User:        "example_user",
		Visible:     true,
		Members: osm.Members{
			{Type: osm.TypeWay, Ref: 100, Role: "outer"},
		},
		Tags: osm.Tags{
			{Key: "type", Value: "multipolygon"},
			{Key: "landuse", Value: "residential"},
		},
	}
	encoder.WriteRelation(relation)

	// Close the encoder
	if err := encoder.Close(); err != nil {
		fmt.Printf("could not close encoder: %v", err)
		os.Exit(1)
	}

	// Check for errors
	for err := range errChan {
		fmt.Printf("encoder error: %v", err)
		os.Exit(1)
	}

	fmt.Println("Successfully wrote OSM data to example.osm.pbf")
	// Output: Successfully wrote OSM data to example.osm.pbf
}
