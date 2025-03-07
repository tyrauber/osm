package osmpbf

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf/internal/osmpbf"
	"google.golang.org/protobuf/proto"
)

// Encoder writes OSM data to a PBF file.
type Encoder struct {
	// Configuration options
	writingProgram   string
	enableZlib       bool
	requiredFeatures []string
	optionalFeatures []string
	bbox             *osmpbf.HeaderBBox

	// Internal state
	writer    io.WriteCloser
	writeBuf  chan []byte
	nodesBuf  chan *osm.Node
	waysBuf   chan *osm.Way
	relsBuf   chan *osm.Relation
	nodeFlush chan chan struct{}
	wayFlush  chan chan struct{}
	relFlush  chan chan struct{}
	errs      chan error
	wg        sync.WaitGroup
	errWg     sync.WaitGroup
	logger    Logger
}

// EncoderOption is a function that configures an Encoder.
type EncoderOption func(*Encoder)

// WithWritingProgram sets the writing program in the header.
func WithWritingProgram(program string) EncoderOption {
	return func(e *Encoder) {
		e.writingProgram = program
	}
}

// WithCompression enables or disables zlib compression.
func WithCompression(enable bool) EncoderOption {
	return func(e *Encoder) {
		e.enableZlib = enable
	}
}

// WithBoundingBox sets the bounding box in the header.
func WithBoundingBox(minLat, minLon, maxLat, maxLon float64) EncoderOption {
	return func(e *Encoder) {
		e.bbox = &osmpbf.HeaderBBox{
			Left:   proto.Int64(int64(minLon * 1e9)),
			Right:  proto.Int64(int64(maxLon * 1e9)),
			Top:    proto.Int64(int64(maxLat * 1e9)),
			Bottom: proto.Int64(int64(minLat * 1e9)),
		}
	}
}

// WithRequiredFeatures sets the required features in the header.
func WithRequiredFeatures(features []string) EncoderOption {
	return func(e *Encoder) {
		e.requiredFeatures = features
	}
}

// WithOptionalFeatures sets the optional features in the header.
func WithOptionalFeatures(features []string) EncoderOption {
	return func(e *Encoder) {
		e.optionalFeatures = features
	}
}

// NewEncoder creates a new encoder that writes to the given writer.
func NewEncoder(w io.WriteCloser, options ...EncoderOption) *Encoder {
	e := &Encoder{
		writingProgram:   "github.com/paulmach/osm/osmpbf",
		enableZlib:       true,
		requiredFeatures: []string{"OsmSchema-V0.6"},
		optionalFeatures: []string{},
		writer:           w,
		writeBuf:         make(chan []byte, 8),
		nodesBuf:         make(chan *osm.Node, 8000),
		waysBuf:          make(chan *osm.Way, 8000),
		relsBuf:          make(chan *osm.Relation, 8000),
		nodeFlush:        make(chan chan struct{}, 1),
		wayFlush:         make(chan chan struct{}, 1),
		relFlush:         make(chan chan struct{}, 1),
		errs:             make(chan error, 10),
		logger:           defaultLogger{},
	}

	for _, opt := range options {
		opt(e)
	}

	return e
}

// Start begins the encoding process and returns a channel for errors.
func (e *Encoder) Start() (<-chan error, error) {
	// Start the writer goroutine
	e.errWg.Add(1)
	go func() {
		defer e.errWg.Done()
		for data := range e.writeBuf {
			if _, err := e.writer.Write(data); err != nil {
				e.errs <- fmt.Errorf("write data: %w", err)
				return
			}
		}
	}()

	// Start the processing goroutines
	e.wg.Add(3)
	go e.processNodes()
	go e.processWays()
	go e.processRelations()

	// Write file header
	header := &osmpbf.HeaderBlock{
		Bbox:             e.bbox,
		RequiredFeatures: e.requiredFeatures,
		OptionalFeatures: e.optionalFeatures,
		Writingprogram:   &e.writingProgram,
	}

	encodedHeader, err := proto.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("marshal file header: %w", err)
	}

	if err := e.encodeBlockToBlob(encodedHeader, "OSMHeader"); err != nil {
		return nil, fmt.Errorf("encode blob header: %w", err)
	}

	return e.errs, nil
}

// Close will stop consuming the channel and close the writer.
func (e *Encoder) Close() error {
	close(e.nodesBuf)
	close(e.waysBuf)
	close(e.relsBuf)
	e.wg.Wait()
	close(e.writeBuf)
	e.errWg.Wait()
	close(e.errs)
	return e.writer.Close()
}

// WriteNode adds a node to the encoder queue.
func (e *Encoder) WriteNode(node *osm.Node) {
	e.nodesBuf <- node
}

// WriteWay adds a way to the encoder queue.
func (e *Encoder) WriteWay(way *osm.Way) {
	e.waysBuf <- way
}

// WriteRelation adds a relation to the encoder queue.
func (e *Encoder) WriteRelation(relation *osm.Relation) {
	e.relsBuf <- relation
}

// WriteObject adds an OSM object to the encoder queue.
func (e *Encoder) WriteObject(obj interface{}) error {
	switch o := obj.(type) {
	case *osm.Node:
		e.WriteNode(o)
	case *osm.Way:
		e.WriteWay(o)
	case *osm.Relation:
		e.WriteRelation(o)
	default:
		return fmt.Errorf("unsupported object type: %T", obj)
	}
	return nil
}

// Flush forces the encoder to flush any pending data.
func (e *Encoder) Flush() {
	// Flush nodes
	done := make(chan struct{})
	e.nodeFlush <- done
	<-done

	// Flush ways
	done = make(chan struct{})
	e.wayFlush <- done
	<-done

	// Flush relations
	done = make(chan struct{})
	e.relFlush <- done
	<-done
}

// encodeBlockToBlob wraps the encoded data into a blob and writes it to the output.
func (e *Encoder) encodeBlockToBlob(data []byte, blobType string) error {
	// Create blob
	blob := &osmpbf.Blob{}

	// Set raw size
	rawSize := int32(len(data))
	blob.RawSize = &rawSize

	// Compress data if enabled
	if e.enableZlib {
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("compress data: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("close zlib writer: %w", err)
		}
		blob.ZlibData = b.Bytes()
	} else {
		blob.Raw = data
	}

	// Marshal blob
	encodedBlob, err := proto.Marshal(blob)
	if err != nil {
		return fmt.Errorf("marshal blob: %w", err)
	}

	// Create blob header
	blobHeader := &osmpbf.BlobHeader{
		Type:     proto.String(blobType),
		Datasize: proto.Int32(int32(len(encodedBlob))),
	}

	// Marshal blob header
	encodedBlobHeader, err := proto.Marshal(blobHeader)
	if err != nil {
		return fmt.Errorf("marshal blob header: %w", err)
	}

	// Write blob header size (4 bytes, big endian)
	headerSize := uint32(len(encodedBlobHeader))
	headerSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerSizeBytes, headerSize)

	// Write header size, header, and blob
	e.writeBuf <- headerSizeBytes
	e.writeBuf <- encodedBlobHeader
	e.writeBuf <- encodedBlob

	return nil
}

// buildStringTable builds a string table from a map of strings.
func buildStringTable(stringMap map[string]int) []string {
	// First string is always empty
	table := []string{""}

	// Add all strings from the map
	i := 1
	for s := range stringMap {
		stringMap[s] = i
		table = append(table, s)
		i++
	}

	return table
}

// findStringIndex finds the index of a string in the string table.
// It uses the provided map for faster lookups if the indices have been set.
func findStringIndex(stringTable []string, s string) int {
	// First check if it's the empty string
	if s == "" {
		return 0
	}

	// Linear search through the string table
	for i, str := range stringTable {
		if str == s {
			return i
		}
	}

	// Should not happen if string table was built correctly
	return 0
}

// findStringIndexWithMap finds the index of a string in the string table using a map for faster lookups.
// This is an optimization that can be used when the stringMap has been populated with indices.
func findStringIndexWithMap(stringTable []string, s string, stringMap map[string]int) int {
	// First check if it's the empty string
	if s == "" {
		return 0
	}

	// Use the map for lookup if available
	if idx, ok := stringMap[s]; ok && idx > 0 {
		return idx
	}

	// Fall back to linear search
	return findStringIndex(stringTable, s)
}

// Logger is an interface for logging.
type Logger interface {
	Printf(format string, v ...interface{})
}

// defaultLogger is a simple logger that writes to stdout.
type defaultLogger struct{}

func (l defaultLogger) Printf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
}

// processNodes processes nodes from the node buffer.
func (e *Encoder) processNodes() {
	defer e.wg.Done()

	var nodes []*osm.Node
	stringTable := make(map[string]int)

	for {
		select {
		case node, ok := <-e.nodesBuf:
			if !ok {
				// Channel closed, flush remaining nodes
				if len(nodes) > 0 {
					if err := e.encodeNodes(nodes, stringTable); err != nil {
						e.errs <- err
					}
				}
				return
			}

			nodes = append(nodes, node)

			// Add node strings to string table
			if node.User != "" {
				stringTable[node.User] = 0
			}
			for _, tag := range node.Tags {
				stringTable[tag.Key] = 0
				stringTable[tag.Value] = 0
			}

			// Flush if we have enough nodes
			if len(nodes) >= 8000 {
				if err := e.encodeNodes(nodes, stringTable); err != nil {
					e.errs <- err
				}
				nodes = nodes[:0]
				stringTable = make(map[string]int)
			}

		case done := <-e.nodeFlush:
			// Flush on demand
			if len(nodes) > 0 {
				if err := e.encodeNodes(nodes, stringTable); err != nil {
					e.errs <- err
				}
				nodes = nodes[:0]
				stringTable = make(map[string]int)
			}
			close(done)
		}
	}
}

// processWays processes ways from the way buffer.
func (e *Encoder) processWays() {
	defer e.wg.Done()

	var ways []*osm.Way
	stringTable := make(map[string]int)

	for {
		select {
		case way, ok := <-e.waysBuf:
			if !ok {
				// Channel closed, flush remaining ways
				if len(ways) > 0 {
					if err := e.encodeWays(ways, stringTable); err != nil {
						e.errs <- err
					}
				}
				return
			}

			ways = append(ways, way)

			// Add way strings to string table
			if way.User != "" {
				stringTable[way.User] = 0
			}
			for _, tag := range way.Tags {
				stringTable[tag.Key] = 0
				stringTable[tag.Value] = 0
			}

			// Flush if we have enough ways
			if len(ways) >= 8000 {
				if err := e.encodeWays(ways, stringTable); err != nil {
					e.errs <- err
				}
				ways = ways[:0]
				stringTable = make(map[string]int)
			}

		case done := <-e.wayFlush:
			// Flush on demand
			if len(ways) > 0 {
				if err := e.encodeWays(ways, stringTable); err != nil {
					e.errs <- err
				}
				ways = ways[:0]
				stringTable = make(map[string]int)
			}
			close(done)
		}
	}
}

// processRelations processes relations from the relation buffer.
func (e *Encoder) processRelations() {
	defer e.wg.Done()

	var relations []*osm.Relation
	stringTable := make(map[string]int)

	for {
		select {
		case relation, ok := <-e.relsBuf:
			if !ok {
				// Channel closed, flush remaining relations
				if len(relations) > 0 {
					if err := e.encodeRelations(relations, stringTable); err != nil {
						e.errs <- err
					}
				}
				return
			}

			relations = append(relations, relation)

			// Add relation strings to string table
			if relation.User != "" {
				stringTable[relation.User] = 0
			}
			for _, tag := range relation.Tags {
				stringTable[tag.Key] = 0
				stringTable[tag.Value] = 0
			}
			for _, member := range relation.Members {
				stringTable[member.Role] = 0
			}

			// Flush if we have enough relations
			if len(relations) >= 8000 {
				if err := e.encodeRelations(relations, stringTable); err != nil {
					e.errs <- err
				}
				relations = relations[:0]
				stringTable = make(map[string]int)
			}

		case done := <-e.relFlush:
			// Flush on demand
			if len(relations) > 0 {
				if err := e.encodeRelations(relations, stringTable); err != nil {
					e.errs <- err
				}
				relations = relations[:0]
				stringTable = make(map[string]int)
			}
			close(done)
		}
	}
}

// encodeNodes encodes a slice of nodes to a PrimitiveBlock.
func (e *Encoder) encodeNodes(nodes []*osm.Node, stringTableMap map[string]int) error {
	// Build string table
	stringTable := buildStringTable(stringTableMap)

	// Create primitive block
	block := &osmpbf.PrimitiveBlock{
		Stringtable: &osmpbf.StringTable{S: stringTable},
		Granularity: proto.Int32(100),
		LatOffset:   proto.Int64(0),
		LonOffset:   proto.Int64(0),
	}

	// Use dense nodes format for efficiency
	dense := &osmpbf.DenseNodes{
		Id:       make([]int64, len(nodes)),
		Lat:      make([]int64, len(nodes)),
		Lon:      make([]int64, len(nodes)),
		KeysVals: []int32{},
	}

	// Add metadata if available
	denseInfo := &osmpbf.DenseInfo{
		Version:   make([]int32, len(nodes)),
		Timestamp: make([]int64, len(nodes)),
		Changeset: make([]int64, len(nodes)),
		Uid:       make([]int32, len(nodes)),
		UserSid:   make([]int32, len(nodes)),
		Visible:   make([]bool, len(nodes)),
	}

	// Delta encoding state
	var lastID, lastLat, lastLon int64
	var lastTimestamp, lastChangeset int64
	var lastUID, lastUserSID int32

	// Process each node
	for i, node := range nodes {
		// Delta encode IDs and coordinates
		nodeID := int64(node.ID)
		dense.Id[i] = nodeID - lastID
		lastID = nodeID

		lat := int64(node.Lat * 1e7)
		lon := int64(node.Lon * 1e7)

		dense.Lat[i] = lat - lastLat
		dense.Lon[i] = lon - lastLon

		lastLat = lat
		lastLon = lon

		// Add tags
		if len(node.Tags) > 0 {
			for _, tag := range node.Tags {
				keyID := findStringIndexWithMap(stringTable, tag.Key, stringTableMap)
				valID := findStringIndexWithMap(stringTable, tag.Value, stringTableMap)
				dense.KeysVals = append(dense.KeysVals, int32(keyID), int32(valID))
			}
		}
		dense.KeysVals = append(dense.KeysVals, 0) // End of tags marker

		// Add metadata
		denseInfo.Version[i] = int32(node.Version)

		timestamp := node.Timestamp.Unix()
		denseInfo.Timestamp[i] = timestamp - lastTimestamp
		lastTimestamp = timestamp

		changesetID := int64(node.ChangesetID)
		denseInfo.Changeset[i] = changesetID - lastChangeset
		lastChangeset = changesetID

		userID := int32(node.UserID)
		denseInfo.Uid[i] = userID - lastUID
		lastUID = userID

		userSID := findStringIndexWithMap(stringTable, node.User, stringTableMap)
		denseInfo.UserSid[i] = int32(userSID) - lastUserSID
		lastUserSID = int32(userSID)

		denseInfo.Visible[i] = node.Visible
	}

	// Add dense nodes to primitive group
	group := &osmpbf.PrimitiveGroup{
		Dense: dense,
	}

	// Add dense info if we have metadata
	dense.Denseinfo = denseInfo

	// Add primitive group to block
	block.Primitivegroup = []*osmpbf.PrimitiveGroup{group}

	// Marshal and encode the block
	data, err := proto.Marshal(block)
	if err != nil {
		return fmt.Errorf("marshal primitive block: %w", err)
	}

	return e.encodeBlockToBlob(data, "OSMData")
}

// encodeWays encodes a slice of ways to a PrimitiveBlock.
func (e *Encoder) encodeWays(ways []*osm.Way, stringTableMap map[string]int) error {
	// Build string table
	stringTable := buildStringTable(stringTableMap)

	// Create primitive block
	block := &osmpbf.PrimitiveBlock{
		Stringtable: &osmpbf.StringTable{S: stringTable},
		Granularity: proto.Int32(100),
		LatOffset:   proto.Int64(0),
		LonOffset:   proto.Int64(0),
	}

	// Create primitive group
	group := &osmpbf.PrimitiveGroup{
		Ways: make([]*osmpbf.Way, len(ways)),
	}

	// Process each way
	for i, way := range ways {
		pbfWay := &osmpbf.Way{
			Id:   proto.Int64(int64(way.ID)),
			Keys: make([]uint32, 0, len(way.Tags)),
			Vals: make([]uint32, 0, len(way.Tags)),
			Refs: make([]int64, len(way.Nodes)),
		}

		// Add tags
		for _, tag := range way.Tags {
			keyID := findStringIndexWithMap(stringTable, tag.Key, stringTableMap)
			valID := findStringIndexWithMap(stringTable, tag.Value, stringTableMap)
			pbfWay.Keys = append(pbfWay.Keys, uint32(keyID))
			pbfWay.Vals = append(pbfWay.Vals, uint32(valID))
		}

		// Add node references with delta encoding
		var lastRef int64
		for j, node := range way.Nodes {
			nodeID := int64(node.ID)
			delta := nodeID - lastRef
			pbfWay.Refs[j] = delta
			lastRef = nodeID
		}

		// Add metadata if available
		if way.Version > 0 {
			pbfWay.Info = &osmpbf.Info{
				Version:   proto.Int32(int32(way.Version)),
				Timestamp: proto.Int64(way.Timestamp.Unix()),
				Changeset: proto.Int64(int64(way.ChangesetID)),
				Uid:       proto.Int32(int32(way.UserID)),
				UserSid:   proto.Uint32(uint32(findStringIndexWithMap(stringTable, way.User, stringTableMap))),
				Visible:   proto.Bool(way.Visible),
			}
		}

		group.Ways[i] = pbfWay
	}

	// Add primitive group to block
	block.Primitivegroup = []*osmpbf.PrimitiveGroup{group}

	// Marshal and encode the block
	data, err := proto.Marshal(block)
	if err != nil {
		return fmt.Errorf("marshal primitive block: %w", err)
	}

	return e.encodeBlockToBlob(data, "OSMData")
}

// encodeRelations encodes a slice of relations to a PrimitiveBlock.
func (e *Encoder) encodeRelations(relations []*osm.Relation, stringTableMap map[string]int) error {
	// Build string table
	stringTable := buildStringTable(stringTableMap)

	// Create primitive block
	block := &osmpbf.PrimitiveBlock{
		Stringtable: &osmpbf.StringTable{S: stringTable},
		Granularity: proto.Int32(100),
		LatOffset:   proto.Int64(0),
		LonOffset:   proto.Int64(0),
	}

	// Create primitive group
	group := &osmpbf.PrimitiveGroup{
		Relations: make([]*osmpbf.Relation, len(relations)),
	}

	// Process each relation
	for i, relation := range relations {
		pbfRelation := &osmpbf.Relation{
			Id:       proto.Int64(int64(relation.ID)),
			Keys:     make([]uint32, 0, len(relation.Tags)),
			Vals:     make([]uint32, 0, len(relation.Tags)),
			RolesSid: make([]int32, len(relation.Members)),
			Memids:   make([]int64, len(relation.Members)),
			Types:    make([]osmpbf.Relation_MemberType, len(relation.Members)),
		}

		// Add tags
		for _, tag := range relation.Tags {
			keyID := findStringIndexWithMap(stringTable, tag.Key, stringTableMap)
			valID := findStringIndexWithMap(stringTable, tag.Value, stringTableMap)
			pbfRelation.Keys = append(pbfRelation.Keys, uint32(keyID))
			pbfRelation.Vals = append(pbfRelation.Vals, uint32(valID))
		}

		// Add members with delta encoding
		var lastID int64
		for j, member := range relation.Members {
			// Add role
			roleID := findStringIndexWithMap(stringTable, member.Role, stringTableMap)
			pbfRelation.RolesSid[j] = int32(roleID)

			// Add member ID with delta encoding
			memberRef := int64(member.Ref)
			delta := memberRef - lastID
			pbfRelation.Memids[j] = delta
			lastID = memberRef

			// Add member type
			switch member.Type {
			case osm.TypeNode:
				pbfRelation.Types[j] = osmpbf.Relation_NODE
			case osm.TypeWay:
				pbfRelation.Types[j] = osmpbf.Relation_WAY
			case osm.TypeRelation:
				pbfRelation.Types[j] = osmpbf.Relation_RELATION
			}
		}

		// Add metadata if available
		if relation.Version > 0 {
			pbfRelation.Info = &osmpbf.Info{
				Version:   proto.Int32(int32(relation.Version)),
				Timestamp: proto.Int64(relation.Timestamp.Unix()),
				Changeset: proto.Int64(int64(relation.ChangesetID)),
				Uid:       proto.Int32(int32(relation.UserID)),
				UserSid:   proto.Uint32(uint32(findStringIndexWithMap(stringTable, relation.User, stringTableMap))),
				Visible:   proto.Bool(relation.Visible),
			}
		}

		group.Relations[i] = pbfRelation
	}

	// Add primitive group to block
	block.Primitivegroup = []*osmpbf.PrimitiveGroup{group}

	// Marshal and encode the block
	data, err := proto.Marshal(block)
	if err != nil {
		return fmt.Errorf("marshal primitive block: %w", err)
	}

	return e.encodeBlockToBlob(data, "OSMData")
}

// StreamingEncoder is an optimized encoder for continuous processing of OSM data.
// It automatically flushes data when batch size limits are reached.
type StreamingEncoder struct {
	encoder    *Encoder
	batchSize  int
	nodeCount  int
	wayCount   int
	relCount   int
	errHandler func(error)
}

// StreamingEncoderOption is a function that configures a StreamingEncoder.
type StreamingEncoderOption func(*StreamingEncoder)

// WithBatchSize sets the number of elements to batch before flushing.
func WithBatchSize(size int) StreamingEncoderOption {
	return func(e *StreamingEncoder) {
		e.batchSize = size
	}
}

// WithErrorHandler sets a custom error handler function.
func WithErrorHandler(handler func(error)) StreamingEncoderOption {
	return func(e *StreamingEncoder) {
		e.errHandler = handler
	}
}

// NewStreamingEncoder creates a new streaming encoder that writes to the given writer.
func NewStreamingEncoder(w io.WriteCloser, options ...interface{}) *StreamingEncoder {
	// Separate encoder options from streaming options
	var encoderOpts []EncoderOption
	var streamingOpts []StreamingEncoderOption

	for _, opt := range options {
		switch o := opt.(type) {
		case EncoderOption:
			encoderOpts = append(encoderOpts, o)
		case StreamingEncoderOption:
			streamingOpts = append(streamingOpts, o)
		}
	}

	se := &StreamingEncoder{
		encoder:   NewEncoder(w, encoderOpts...),
		batchSize: 8000, // Default batch size
		errHandler: func(err error) {
			// Default error handler just logs the error
			fmt.Printf("StreamingEncoder error: %v\n", err)
		},
	}

	// Apply streaming options
	for _, opt := range streamingOpts {
		opt(se)
	}

	return se
}

// Start begins the encoding process and returns a channel for errors.
func (se *StreamingEncoder) Start() (<-chan error, error) {
	return se.encoder.Start()
}

// Close flushes any remaining data and closes the encoder.
func (se *StreamingEncoder) Close() error {
	// Flush any remaining data
	se.Flush()
	return se.encoder.Close()
}

// Flush forces the encoder to flush any pending data.
func (se *StreamingEncoder) Flush() {
	se.encoder.Flush()
	se.nodeCount = 0
	se.wayCount = 0
	se.relCount = 0
}

// WriteNode adds a node to the encoder queue and flushes if batch size is reached.
func (se *StreamingEncoder) WriteNode(node *osm.Node) error {
	se.encoder.WriteNode(node)
	se.nodeCount++

	// Flush if we've reached the batch size
	if se.nodeCount >= se.batchSize {
		done := make(chan struct{})
		se.encoder.nodeFlush <- done
		<-done
		se.nodeCount = 0
	}

	return nil
}

// WriteWay adds a way to the encoder queue and flushes if batch size is reached.
func (se *StreamingEncoder) WriteWay(way *osm.Way) error {
	se.encoder.WriteWay(way)
	se.wayCount++

	// Flush if we've reached the batch size
	if se.wayCount >= se.batchSize {
		done := make(chan struct{})
		se.encoder.wayFlush <- done
		<-done
		se.wayCount = 0
	}

	return nil
}

// WriteRelation adds a relation to the encoder queue and flushes if batch size is reached.
func (se *StreamingEncoder) WriteRelation(relation *osm.Relation) error {
	se.encoder.WriteRelation(relation)
	se.relCount++

	// Flush if we've reached the batch size
	if se.relCount >= se.batchSize {
		done := make(chan struct{})
		se.encoder.relFlush <- done
		<-done
		se.relCount = 0
	}

	return nil
}

// WriteObject adds an OSM object to the encoder queue.
func (se *StreamingEncoder) WriteObject(obj interface{}) error {
	switch o := obj.(type) {
	case *osm.Node:
		return se.WriteNode(o)
	case *osm.Way:
		return se.WriteWay(o)
	case *osm.Relation:
		return se.WriteRelation(o)
	default:
		err := fmt.Errorf("unsupported object type: %T", obj)
		if se.errHandler != nil {
			se.errHandler(err)
		}
		return err
	}
}
