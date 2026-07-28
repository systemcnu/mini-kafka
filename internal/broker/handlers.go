// Per-message handlers: validate at the edge (names, caps — each rejection
// carries its pinned code), call storage, encode the response. This is the
// only place wire meets storage.
package broker

import (
	"errors"
	"time"

	"github.com/systemcnu/mini-kafka/internal/storage"
	"github.com/systemcnu/mini-kafka/internal/wire"
)

// Live input caps (D-SL0-8). Frame caps live in wire; storage owns the
// topic/partition-count caps.
const (
	MaxPayload        = 1 << 20 // 1 MiB
	MaxFetchWaitMs    = 30_000
	DefaultFetchWait  = 5_000
	MaxFetchBytes     = 4 << 20 // 4 MiB
	DefaultFetchBytes = 1 << 20 // 1 MiB
	MaxFetchEntries   = 16
)

// dispatch routes one request frame. On werr != nil the caller serves an
// Error frame; closeAfter tells it to drop the connection afterwards
// (unknown type, canceled park, unservable read).
func (s *Server) dispatch(typ byte, payload []byte, cancel <-chan struct{}) (respType byte, respBody []byte, werr *wire.Error, closeAfter bool) {
	switch typ {
	case wire.TypeProduce:
		respType, respBody, werr = s.handleProduce(payload)
		return respType, respBody, werr, false
	case wire.TypeFetch:
		return s.handleFetch(payload, cancel)
	case wire.TypeCreateTopic:
		respType, respBody, werr = s.handleCreateTopic(payload)
		return respType, respBody, werr, false
	case wire.TypeListTopics:
		respType, respBody, werr = s.handleListTopics(payload)
		return respType, respBody, werr, false
	default:
		// Unknown type: Error then close (D-SL0-2). Types 9+ are SL2's.
		return 0, nil, wire.Errf(wire.CodeMalformed, "unknown type %d", typ), true
	}
}

func (s *Server) handleProduce(payload []byte) (byte, []byte, *wire.Error) {
	m, werr := wire.DecodeProduce(payload)
	if werr != nil {
		return 0, nil, werr
	}
	if err := wire.ValidateName(m.Topic); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	// PROD-3: reject BEFORE touching storage so nothing is written.
	if len(m.Payload) > MaxPayload {
		return 0, nil, wire.Errf(wire.CodeMsgTooLarge, "payload of %d bytes exceeds cap %d", len(m.Payload), MaxPayload)
	}
	p, err := s.store.Partition(m.Topic, m.Partition)
	if err != nil {
		return 0, nil, storageError(err)
	}
	off, err := p.Append(m.Payload)
	if err != nil {
		return 0, nil, storageError(err)
	}
	return wire.TypeProduceResp, wire.ProduceResp{Offset: off}.Encode(), nil
}

func (s *Server) handleFetch(payload []byte, cancel <-chan struct{}) (byte, []byte, *wire.Error, bool) {
	m, werr := wire.DecodeFetch(payload)
	if werr != nil {
		return 0, nil, werr, false
	}
	// All entries validated up front; any invalid → whole-frame Error,
	// nothing served (D-SL0-3).
	if err := wire.ValidateName(m.Topic); err != nil {
		return 0, nil, err.(*wire.Error), false
	}
	if len(m.Entries) == 0 {
		return 0, nil, wire.Errf(wire.CodeMalformed, "fetch requires at least one entry"), false
	}
	if len(m.Entries) > MaxFetchEntries {
		return 0, nil, wire.Errf(wire.CodeFetchTooWide, "%d entries exceeds cap %d", len(m.Entries), MaxFetchEntries), false
	}
	if len(m.Entries) > 1 {
		// G7: the wire shape is final; multi-entry service arrives with
		// groups in SL2.
		return 0, nil, wire.Errf(wire.CodeCapExceeded, "multi-entry fetch arrives with groups"), false
	}
	if m.MaxWaitMs > MaxFetchWaitMs {
		return 0, nil, wire.Errf(wire.CodeCapExceeded, "maxWaitMs %d exceeds cap %d", m.MaxWaitMs, MaxFetchWaitMs), false
	}
	if m.MaxBytes > MaxFetchBytes {
		return 0, nil, wire.Errf(wire.CodeCapExceeded, "maxBytes %d exceeds cap %d", m.MaxBytes, MaxFetchBytes), false
	}
	maxWait := time.Duration(m.MaxWaitMs) * time.Millisecond
	if m.MaxWaitMs == 0 {
		maxWait = DefaultFetchWait * time.Millisecond
	}
	maxBytes := m.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultFetchBytes
	}
	entry := m.Entries[0]
	p, err := s.store.Partition(m.Topic, entry.Partition)
	if err != nil {
		return 0, nil, storageError(err), false
	}
	recs, err := p.Fetch(entry.Offset, maxBytes, maxWait, s.stopping, cancel)
	if err != nil {
		// Canceled park during conn teardown, or a read failure on durable
		// data (which has no protocol code): drop the conn, don't invent.
		return 0, nil, nil, true
	}
	group := wire.FetchGroup{Partition: entry.Partition, Recs: make([]wire.Rec, 0, len(recs))}
	for _, r := range recs {
		group.Recs = append(group.Recs, wire.Rec{Offset: r.Offset, Payload: r.Payload})
	}
	resp := wire.FetchResp{Groups: []wire.FetchGroup{group}}
	return wire.TypeFetchResp, resp.Encode(), nil, false
}

func (s *Server) handleCreateTopic(payload []byte) (byte, []byte, *wire.Error) {
	m, werr := wire.DecodeCreateTopic(payload)
	if werr != nil {
		return 0, nil, werr
	}
	if err := wire.ValidateName(m.Topic); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	if err := s.store.CreateTopic(m.Topic, m.Partitions); err != nil {
		return 0, nil, storageError(err)
	}
	return wire.TypeCreateTopicResp, nil, nil
}

func (s *Server) handleListTopics(payload []byte) (byte, []byte, *wire.Error) {
	if len(payload) != 0 {
		return 0, nil, wire.Errf(wire.CodeMalformed, "list-topics takes no body")
	}
	topics := s.store.Topics()
	resp := wire.ListTopicsResp{Topics: make([]wire.TopicInfo, 0, len(topics))}
	for _, t := range topics {
		resp.Topics = append(resp.Topics, wire.TopicInfo{Name: t.Name, Partitions: t.Partitions})
	}
	return wire.TypeListTopicsResp, resp.Encode(), nil
}

// storageError maps storage sentinels onto their pinned wire codes.
func storageError(err error) *wire.Error {
	switch {
	case errors.Is(err, storage.ErrUnknownTopic):
		return wire.Errf(wire.CodeUnknownTopic, "%v", err)
	case errors.Is(err, storage.ErrBadPartition):
		return wire.Errf(wire.CodeBadPartition, "%v", err)
	case errors.Is(err, storage.ErrTopicExists):
		return wire.Errf(wire.CodeTopicExists, "%v", err)
	case errors.Is(err, storage.ErrTooManyTopics), errors.Is(err, storage.ErrBadPartitionCount):
		return wire.Errf(wire.CodeCapExceeded, "%v", err)
	case errors.Is(err, storage.ErrWriteRejected):
		return wire.Errf(wire.CodeWriteFailed, "%v", err)
	case errors.Is(err, storage.ErrStopped):
		return wire.Errf(wire.CodeShuttingDown, "%v", err)
	default:
		return wire.Errf(wire.CodeWriteFailed, "internal storage error: %v", err)
	}
}
