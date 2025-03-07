package osmpbf

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/paulmach/osm"
)

// bufferWriteCloser wraps a bytes.Buffer to implement io.WriteCloser
type bufferWriteCloser struct {
	*bytes.Buffer
}

func (bwc *bufferWriteCloser) Close() error {
	return nil
}

func TestEncoder(t *testing.T) {
	// Create a buffer to write to
	var buf bytes.Buffer
	writeCloser := &bufferWriteCloser{&buf}

	// Create an encoder
	encoder := NewEncoder(writeCloser,
		WithWritingProgram("osmpbf-test"),
		WithCompression(true),
		WithBoundingBox(51.5, -0.2, 51.6, -0.1), // London area
	)

	// Start the encoder
	errChan, err := encoder.Start()
	if err != nil {
		t.Fatalf("Failed to start encoder: %v", err)
	}

	// Create some test data
	node := &osm.Node{
		ID:          osm.NodeID(1),
		Lat:         51.5074,
		Lon:         -0.1278,
		Version:     1,
		Timestamp:   time.Now(),
		ChangesetID: osm.ChangesetID(123),
		UserID:      osm.UserID(456),
		User:        "testuser",
		Visible:     true,
		Tags: osm.Tags{
			{Key: "amenity", Value: "pub"},
			{Key: "name", Value: "The Crown"},
		},
	}

	way := &osm.Way{
		ID:          osm.WayID(2),
		Version:     1,
		Timestamp:   time.Now(),
		ChangesetID: osm.ChangesetID(123),
		UserID:      osm.UserID(456),
		User:        "testuser",
		Visible:     true,
		Nodes: osm.WayNodes{
			{ID: osm.NodeID(1)},
			{ID: osm.NodeID(3)},
			{ID: osm.NodeID(5)},
		},
		Tags: osm.Tags{
			{Key: "highway", Value: "residential"},
			{Key: "name", Value: "Main Street"},
		},
	}

	relation := &osm.Relation{
		ID:          osm.RelationID(3),
		Version:     1,
		Timestamp:   time.Now(),
		ChangesetID: osm.ChangesetID(123),
		UserID:      osm.UserID(456),
		User:        "testuser",
		Visible:     true,
		Members: osm.Members{
			{Type: osm.TypeWay, Ref: 2, Role: "outer"},
			{Type: osm.TypeWay, Ref: 4, Role: "inner"},
		},
		Tags: osm.Tags{
			{Key: "type", Value: "multipolygon"},
			{Key: "landuse", Value: "residential"},
		},
	}

	// Write the test data
	encoder.WriteNode(node)
	encoder.WriteWay(way)
	encoder.WriteRelation(relation)

	// Test the Flush method
	encoder.Flush()

	// Write more data to test batching
	for i := 10; i < 20; i++ {
		n := &osm.Node{
			ID:          osm.NodeID(i),
			Lat:         51.5 + float64(i)*0.01,
			Lon:         -0.2 + float64(i)*0.01,
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "testuser",
			Visible:     true,
			Tags: osm.Tags{
				{Key: "test", Value: "batch"},
			},
		}
		encoder.WriteNode(n)
	}

	// Close the encoder
	if err := encoder.Close(); err != nil {
		t.Fatalf("Failed to close encoder: %v", err)
	}

	// Check for errors
	for err := range errChan {
		t.Fatalf("Encoder error: %v", err)
	}

	// Now read back the data to verify it was encoded correctly
	r := bytes.NewReader(buf.Bytes())
	scanner := New(context.Background(), r, 1)
	defer scanner.Close()

	// Check header
	header, err := scanner.Header()
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	// Verify header contents
	if header.WritingProgram != "osmpbf-test" {
		t.Errorf("Expected writing program 'osmpbf-test', got '%s'", header.WritingProgram)
	}

	// Verify bounding box
	if header.Bounds == nil {
		t.Error("Expected bounding box, got nil")
	} else {
		if header.Bounds.MinLat != 51.5 || header.Bounds.MinLon != -0.2 ||
			header.Bounds.MaxLat != 51.6 || header.Bounds.MaxLon != -0.1 {
			t.Errorf("Bounding box doesn't match: expected (51.5,-0.2,51.6,-0.1), got (%f,%f,%f,%f)",
				header.Bounds.MinLat, header.Bounds.MinLon, header.Bounds.MaxLat, header.Bounds.MaxLon)
		}
	}

	// Scan and verify objects
	var foundNode, foundWay, foundRelation bool
	var nodeCount, wayCount, relationCount int

	for scanner.Scan() {
		obj := scanner.Object()
		switch o := obj.(type) {
		case *osm.Node:
			nodeCount++
			if o.ID == node.ID {
				foundNode = true
				// Verify node properties with epsilon for floating-point comparison
				const epsilon = 1e-6
				if math.Abs(o.Lat-node.Lat) > epsilon || math.Abs(o.Lon-node.Lon) > epsilon {
					t.Errorf("Node coordinates don't match: expected (%f,%f), got (%f,%f)",
						node.Lat, node.Lon, o.Lat, o.Lon)
				}

				// Verify tags
				if len(o.Tags) != len(node.Tags) {
					t.Errorf("Node tags count doesn't match: expected %d, got %d",
						len(node.Tags), len(o.Tags))
				} else {
					for i, tag := range node.Tags {
						if o.Tags[i].Key != tag.Key || o.Tags[i].Value != tag.Value {
							t.Errorf("Node tag doesn't match: expected (%s=%s), got (%s=%s)",
								tag.Key, tag.Value, o.Tags[i].Key, o.Tags[i].Value)
						}
					}
				}

				// Verify metadata
				if o.Version != node.Version {
					t.Errorf("Node version doesn't match: expected %d, got %d",
						node.Version, o.Version)
				}

				if o.UserID != node.UserID {
					t.Errorf("Node user ID doesn't match: expected %d, got %d",
						node.UserID, o.UserID)
				}

				if o.User != node.User {
					t.Errorf("Node user doesn't match: expected %s, got %s",
						node.User, o.User)
				}

				if o.Visible != node.Visible {
					t.Errorf("Node visibility doesn't match: expected %t, got %t",
						node.Visible, o.Visible)
				}
			}
		case *osm.Way:
			wayCount++
			if o.ID == way.ID {
				foundWay = true
				// Verify way properties
				if len(o.Nodes) != len(way.Nodes) {
					t.Errorf("Way nodes count doesn't match: expected %d, got %d",
						len(way.Nodes), len(o.Nodes))
				} else {
					for i, node := range way.Nodes {
						if o.Nodes[i].ID != node.ID {
							t.Errorf("Way node ID doesn't match: expected %d, got %d",
								node.ID, o.Nodes[i].ID)
						}
					}
				}

				// Verify tags
				if len(o.Tags) != len(way.Tags) {
					t.Errorf("Way tags count doesn't match: expected %d, got %d",
						len(way.Tags), len(o.Tags))
				} else {
					for i, tag := range way.Tags {
						if o.Tags[i].Key != tag.Key || o.Tags[i].Value != tag.Value {
							t.Errorf("Way tag doesn't match: expected (%s=%s), got (%s=%s)",
								tag.Key, tag.Value, o.Tags[i].Key, o.Tags[i].Value)
						}
					}
				}
			}
		case *osm.Relation:
			relationCount++
			if o.ID == relation.ID {
				foundRelation = true
				// Verify relation properties
				if len(o.Members) != len(relation.Members) {
					t.Errorf("Relation members count doesn't match: expected %d, got %d",
						len(relation.Members), len(o.Members))
				} else {
					for i, member := range relation.Members {
						if o.Members[i].Type != member.Type || o.Members[i].Ref != member.Ref || o.Members[i].Role != member.Role {
							t.Errorf("Relation member doesn't match: expected (%s,%d,%s), got (%s,%d,%s)",
								member.Type, member.Ref, member.Role, o.Members[i].Type, o.Members[i].Ref, o.Members[i].Role)
						}
					}
				}

				// Verify tags
				if len(o.Tags) != len(relation.Tags) {
					t.Errorf("Relation tags count doesn't match: expected %d, got %d",
						len(relation.Tags), len(o.Tags))
				} else {
					for i, tag := range relation.Tags {
						if o.Tags[i].Key != tag.Key || o.Tags[i].Value != tag.Value {
							t.Errorf("Relation tag doesn't match: expected (%s=%s), got (%s=%s)",
								tag.Key, tag.Value, o.Tags[i].Key, o.Tags[i].Value)
						}
					}
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("Scanner error: %v", err)
	}

	// Verify we found all our objects
	if !foundNode {
		t.Error("Node not found in encoded data")
	}
	if !foundWay {
		t.Error("Way not found in encoded data")
	}
	if !foundRelation {
		t.Error("Relation not found in encoded data")
	}

	// Verify counts
	if nodeCount != 11 { // 1 original + 10 batch nodes
		t.Errorf("Expected 11 nodes, got %d", nodeCount)
	}
	if wayCount != 1 {
		t.Errorf("Expected 1 way, got %d", wayCount)
	}
	if relationCount != 1 {
		t.Errorf("Expected 1 relation, got %d", relationCount)
	}
}

func TestStringTable(t *testing.T) {
	// Test building string table
	stringMap := map[string]int{
		"highway":     0,
		"residential": 0,
		"name":        0,
		"Main Street": 0,
	}

	table := buildStringTable(stringMap)

	// First entry should be empty
	if table[0] != "" {
		t.Errorf("Expected empty string at index 0, got '%s'", table[0])
	}

	// Check all strings are in the table
	found := make(map[string]bool)
	for _, s := range table {
		found[s] = true
	}

	for s := range stringMap {
		if !found[s] {
			t.Errorf("String '%s' not found in table", s)
		}
	}

	// Test finding string index
	for s := range stringMap {
		idx := findStringIndex(table, s)
		if idx == 0 && s != "" {
			t.Errorf("String '%s' not found in table", s)
		}
		if table[idx] != s {
			t.Errorf("Expected string '%s' at index %d, got '%s'", s, idx, table[idx])
		}
	}
}

func TestWriteObject(t *testing.T) {
	// Create a buffer to write to
	var buf bytes.Buffer
	writeCloser := &bufferWriteCloser{&buf}

	// Create an encoder
	encoder := NewEncoder(writeCloser)

	// Start the encoder
	errChan, err := encoder.Start()
	if err != nil {
		t.Fatalf("Failed to start encoder: %v", err)
	}

	// Create test objects
	node := &osm.Node{
		ID:  osm.NodeID(1),
		Lat: 51.5074,
		Lon: -0.1278,
	}

	way := &osm.Way{
		ID: osm.WayID(2),
		Nodes: osm.WayNodes{
			{ID: osm.NodeID(1)},
		},
	}

	relation := &osm.Relation{
		ID: osm.RelationID(3),
		Members: osm.Members{
			{Type: osm.TypeWay, Ref: 2, Role: "outer"},
		},
	}

	// Test WriteObject method
	if err := encoder.WriteObject(node); err != nil {
		t.Errorf("Failed to write node: %v", err)
	}

	if err := encoder.WriteObject(way); err != nil {
		t.Errorf("Failed to write way: %v", err)
	}

	if err := encoder.WriteObject(relation); err != nil {
		t.Errorf("Failed to write relation: %v", err)
	}

	// Test with unsupported object type
	// Instead of directly passing an invalid type, we'll use a type assertion
	// to trigger the error path in the WriteObject method
	invalidObj := "not an osm object"
	err = encoder.WriteObject(invalidObj)
	if err == nil {
		t.Error("Expected error when writing unsupported object type, got nil")
	} else {
		// Verify the error message contains information about the invalid type
		expectedErrMsg := "unsupported object type: string"
		if err.Error() != expectedErrMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
		}
	}

	// Close the encoder
	if err := encoder.Close(); err != nil {
		t.Fatalf("Failed to close encoder: %v", err)
	}

	// Check for errors
	for err := range errChan {
		t.Fatalf("Encoder error: %v", err)
	}
}

// discardWriteCloser implements io.WriteCloser and discards all data
type discardWriteCloser struct{}

func (dwc *discardWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (dwc *discardWriteCloser) Close() error {
	return nil
}

// BenchmarkEncoder measures the performance of the encoder
func BenchmarkEncoder(b *testing.B) {
	// Create a discard writer to avoid I/O overhead
	writeCloser := &discardWriteCloser{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		encoder := NewEncoder(writeCloser, WithCompression(true))
		errChan, err := encoder.Start()
		if err != nil {
			b.Fatalf("Failed to start encoder: %v", err)
		}

		// Create test data
		nodes := make([]*osm.Node, 1000)
		for j := 0; j < 1000; j++ {
			nodes[j] = &osm.Node{
				ID:          osm.NodeID(j),
				Lat:         51.5 + float64(j)*0.0001,
				Lon:         -0.2 + float64(j)*0.0001,
				Version:     1,
				Timestamp:   time.Now(),
				ChangesetID: osm.ChangesetID(123),
				UserID:      osm.UserID(456),
				User:        "benchmark_user",
				Visible:     true,
				Tags: osm.Tags{
					{Key: "benchmark", Value: "node"},
				},
			}
		}

		ways := make([]*osm.Way, 100)
		for j := 0; j < 100; j++ {
			wayNodes := make(osm.WayNodes, 10)
			for k := 0; k < 10; k++ {
				wayNodes[k] = osm.WayNode{ID: osm.NodeID(j*10 + k)}
			}

			ways[j] = &osm.Way{
				ID:          osm.WayID(j),
				Version:     1,
				Timestamp:   time.Now(),
				ChangesetID: osm.ChangesetID(123),
				UserID:      osm.UserID(456),
				User:        "benchmark_user",
				Visible:     true,
				Nodes:       wayNodes,
				Tags: osm.Tags{
					{Key: "benchmark", Value: "way"},
				},
			}
		}

		relations := make([]*osm.Relation, 10)
		for j := 0; j < 10; j++ {
			members := make(osm.Members, 5)
			for k := 0; k < 5; k++ {
				members[k] = osm.Member{
					Type: osm.TypeWay,
					Ref:  int64(j*5 + k),
					Role: "outer",
				}
			}

			relations[j] = &osm.Relation{
				ID:          osm.RelationID(j),
				Version:     1,
				Timestamp:   time.Now(),
				ChangesetID: osm.ChangesetID(123),
				UserID:      osm.UserID(456),
				User:        "benchmark_user",
				Visible:     true,
				Members:     members,
				Tags: osm.Tags{
					{Key: "benchmark", Value: "relation"},
					{Key: "type", Value: "multipolygon"},
				},
			}
		}

		b.StartTimer()

		// Write all the data
		for _, node := range nodes {
			encoder.WriteNode(node)
		}

		for _, way := range ways {
			encoder.WriteWay(way)
		}

		for _, relation := range relations {
			encoder.WriteRelation(relation)
		}

		// Close the encoder
		encoder.Close()

		// Check for errors
		for err := range errChan {
			b.Fatalf("Encoder error: %v", err)
		}
	}
}

// BenchmarkEncoder_NoCompression measures the performance of the encoder without compression
func BenchmarkEncoder_NoCompression(b *testing.B) {
	// Create a discard writer to avoid I/O overhead
	writeCloser := &discardWriteCloser{}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		encoder := NewEncoder(writeCloser, WithCompression(false))
		errChan, err := encoder.Start()
		if err != nil {
			b.Fatalf("Failed to start encoder: %v", err)
		}

		// Create test data (same as BenchmarkEncoder)
		nodes := make([]*osm.Node, 1000)
		for j := 0; j < 1000; j++ {
			nodes[j] = &osm.Node{
				ID:          osm.NodeID(j),
				Lat:         51.5 + float64(j)*0.0001,
				Lon:         -0.2 + float64(j)*0.0001,
				Version:     1,
				Timestamp:   time.Now(),
				ChangesetID: osm.ChangesetID(123),
				UserID:      osm.UserID(456),
				User:        "benchmark_user",
				Visible:     true,
				Tags: osm.Tags{
					{Key: "benchmark", Value: "node"},
				},
			}
		}

		b.StartTimer()

		// Write all the nodes
		for _, node := range nodes {
			encoder.WriteNode(node)
		}

		// Close the encoder
		encoder.Close()

		// Check for errors
		for err := range errChan {
			b.Fatalf("Encoder error: %v", err)
		}
	}
}

func TestStreamingEncoder(t *testing.T) {
	// Create a buffer to write to
	var buf bytes.Buffer
	writeCloser := &bufferWriteCloser{&buf}

	// Create a streaming encoder with a small batch size
	encoder := NewStreamingEncoder(writeCloser,
		WithWritingProgram("streaming-test"),
		WithBatchSize(5), // Small batch size to test flushing
	)

	// Start the encoder
	errChan, err := encoder.Start()
	if err != nil {
		t.Fatalf("Failed to start encoder: %v", err)
	}

	// Create and write some nodes
	for i := 1; i <= 20; i++ {
		node := &osm.Node{
			ID:          osm.NodeID(i),
			Lat:         51.5 + float64(i)*0.01,
			Lon:         -0.2 + float64(i)*0.01,
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "testuser",
			Visible:     true,
			Tags: osm.Tags{
				{Key: "test", Value: fmt.Sprintf("node_%d", i)},
			},
		}
		if err := encoder.WriteNode(node); err != nil {
			t.Fatalf("Failed to write node: %v", err)
		}
	}

	// Create and write some ways
	for i := 1; i <= 10; i++ {
		way := &osm.Way{
			ID:          osm.WayID(i),
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "testuser",
			Visible:     true,
			Nodes: osm.WayNodes{
				{ID: osm.NodeID(i)},
				{ID: osm.NodeID(i + 1)},
				{ID: osm.NodeID(i + 2)},
			},
			Tags: osm.Tags{
				{Key: "test", Value: fmt.Sprintf("way_%d", i)},
			},
		}
		if err := encoder.WriteWay(way); err != nil {
			t.Fatalf("Failed to write way: %v", err)
		}
	}

	// Create and write some relations
	for i := 1; i <= 5; i++ {
		relation := &osm.Relation{
			ID:          osm.RelationID(i),
			Version:     1,
			Timestamp:   time.Now(),
			ChangesetID: osm.ChangesetID(123),
			UserID:      osm.UserID(456),
			User:        "testuser",
			Visible:     true,
			Members: osm.Members{
				{Type: osm.TypeWay, Ref: int64(i), Role: "outer"},
				{Type: osm.TypeWay, Ref: int64(i + 1), Role: "inner"},
			},
			Tags: osm.Tags{
				{Key: "test", Value: fmt.Sprintf("relation_%d", i)},
			},
		}
		if err := encoder.WriteRelation(relation); err != nil {
			t.Fatalf("Failed to write relation: %v", err)
		}
	}

	// Test WriteObject method
	node := &osm.Node{
		ID:  osm.NodeID(100),
		Lat: 51.5,
		Lon: -0.1,
	}
	if err := encoder.WriteObject(node); err != nil {
		t.Fatalf("Failed to write object: %v", err)
	}

	// Close the encoder
	if err := encoder.Close(); err != nil {
		t.Fatalf("Failed to close encoder: %v", err)
	}

	// Check for errors
	for err := range errChan {
		t.Fatalf("Encoder error: %v", err)
	}

	// Now read back the data to verify it was encoded correctly
	r := bytes.NewReader(buf.Bytes())
	scanner := New(context.Background(), r, 1)
	defer scanner.Close()

	// Check header
	header, err := scanner.Header()
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	// Verify header contents
	if header.WritingProgram != "streaming-test" {
		t.Errorf("Expected writing program 'streaming-test', got '%s'", header.WritingProgram)
	}

	// Count objects
	var nodeCount, wayCount, relationCount int
	for scanner.Scan() {
		obj := scanner.Object()
		switch obj.(type) {
		case *osm.Node:
			nodeCount++
		case *osm.Way:
			wayCount++
		case *osm.Relation:
			relationCount++
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("Scanner error: %v", err)
	}

	// Verify counts
	if nodeCount != 21 { // 20 regular nodes + 1 from WriteObject
		t.Errorf("Expected 21 nodes, got %d", nodeCount)
	}
	if wayCount != 10 {
		t.Errorf("Expected 10 ways, got %d", wayCount)
	}
	if relationCount != 5 {
		t.Errorf("Expected 5 relations, got %d", relationCount)
	}
}
