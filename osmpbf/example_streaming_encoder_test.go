package osmpbf_test

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

// Example_streamingEncoder demonstrates how to use the streaming encoder for continuous processing.
func Example_streamingEncoder() {
	// Open a file for writing
	file, err := os.Create("example_streaming.osm.pbf")
	if err != nil {
		log.Fatalf("could not create file: %v", err)
	}
	defer file.Close()

	// Create a streaming encoder with options
	encoder := osmpbf.NewStreamingEncoder(file,
		osmpbf.WithWritingProgram("streaming-example"),
		osmpbf.WithCompression(true),
		osmpbf.WithBoundingBox(51.5, -0.2, 51.6, -0.1), // London area
		osmpbf.WithBatchSize(5000),                     // Flush every 5000 elements
		osmpbf.WithErrorHandler(func(err error) {
			log.Printf("Encoder error: %v", err)
		}),
	)

	// Start the encoder
	errChan, err := encoder.Start()
	if err != nil {
		log.Fatalf("could not start encoder: %v", err)
	}

	// Handle encoder errors in a separate goroutine
	go func() {
		for err := range errChan {
			log.Printf("Encoder error: %v", err)
		}
	}()

	// Create and write a large number of nodes
	for i := 1; i <= 10000; i++ {
		node := &osm.Node{
			ID:          osm.NodeID(i),
			Lat:         51.5 + float64(i%100)*0.001,
			Lon:         -0.2 + float64(i%100)*0.001,
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

		// Write the node - this will automatically flush when batch size is reached
		if err := encoder.WriteNode(node); err != nil {
			log.Printf("Failed to write node: %v", err)
		}

		// Simulate some processing time
		if i%1000 == 0 {
			fmt.Printf("Processed %d nodes\n", i)
		}
	}

	// Create and write some ways
	for i := 1; i <= 1000; i++ {
		way := &osm.Way{
			ID:          osm.WayID(i),
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "example_user",
			Visible:     true,
			Nodes: osm.WayNodes{
				{ID: osm.NodeID(i * 10)},
				{ID: osm.NodeID(i*10 + 1)},
				{ID: osm.NodeID(i*10 + 2)},
				{ID: osm.NodeID(i*10 + 3)},
				{ID: osm.NodeID(i*10 + 4)},
			},
			Tags: osm.Tags{
				{Key: "highway", Value: "residential"},
				{Key: "name", Value: fmt.Sprintf("Street %d", i)},
			},
		}

		// Write the way
		if err := encoder.WriteWay(way); err != nil {
			log.Printf("Failed to write way: %v", err)
		}

		if i%100 == 0 {
			fmt.Printf("Processed %d ways\n", i)
		}
	}

	// Create and write some relations
	for i := 1; i <= 100; i++ {
		relation := &osm.Relation{
			ID:          osm.RelationID(i),
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "example_user",
			Visible:     true,
			Members: osm.Members{
				{Type: osm.TypeWay, Ref: int64(i * 10), Role: "outer"},
				{Type: osm.TypeWay, Ref: int64(i*10 + 1), Role: "inner"},
			},
			Tags: osm.Tags{
				{Key: "type", Value: "multipolygon"},
				{Key: "landuse", Value: "residential"},
				{Key: "name", Value: fmt.Sprintf("Area %d", i)},
			},
		}

		// Write the relation
		if err := encoder.WriteRelation(relation); err != nil {
			log.Printf("Failed to write relation: %v", err)
		}
	}

	// Manually flush any remaining data (optional, as Close will also flush)
	encoder.Flush()

	// Close the encoder
	if err := encoder.Close(); err != nil {
		log.Fatalf("could not close encoder: %v", err)
	}

	fmt.Println("Successfully wrote OSM data to example_streaming.osm.pbf")
	// Output: Successfully wrote OSM data to example_streaming.osm.pbf
}
