// Per-message handlers: validate at the edge (names, caps — each rejection
// carries its pinned code), call storage, encode the response. This is the
// only place wire meets storage.
package broker

import (
	"errors"
	"time"

	"github.com/systemcnu/mini-kafka/internal/group"
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
func (s *Server) dispatch(typ byte, payload []byte, cancel <-chan struct{}, connID uint64) (respType byte, respBody []byte, werr *wire.Error, closeAfter bool) {
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
	case wire.TypeJoinGroup:
		respType, respBody, werr = s.handleJoinGroup(payload, connID)
		return respType, respBody, werr, false
	case wire.TypeHeartbeat:
		respType, respBody, werr = s.handleHeartbeat(payload)
		return respType, respBody, werr, false
	case wire.TypeCommitOffsets:
		respType, respBody, werr = s.handleCommitOffsets(payload)
		return respType, respBody, werr, false
	case wire.TypeLeaveGroup:
		respType, respBody, werr = s.handleLeaveGroup(payload)
		return respType, respBody, werr, false
	case wire.TypeGroupFetch:
		return s.handleGroupFetch(payload, cancel)
	default:
		// Unknown type: Error then close (D-SL0-2).
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
	if len(m.Entries) == 1 {
		// Raw single-entry fetch keeps its SL0 Partition.Fetch path
		// untouched (D-SL2-7); the loop below is the n≥2 + GroupFetch path.
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
	targets, werr := s.resolveTargets(m.Topic, m.Entries)
	if werr != nil {
		return 0, nil, werr, false
	}
	return s.fetchLoop(targets, maxBytes, maxWait, cancel, nil)
}

// fetchTarget is one resolved entry of the multi-entry fetch loop.
type fetchTarget struct {
	partition uint32
	offset    uint64
	p         *storage.Partition
}

// resolveTargets validates every entry up front; any invalid partition →
// whole-frame Error, nothing served (D-SL0-3).
func (s *Server) resolveTargets(topic string, entries []wire.FetchEntry) ([]fetchTarget, *wire.Error) {
	targets := make([]fetchTarget, 0, len(entries))
	for _, e := range entries {
		p, err := s.store.Partition(topic, e.Partition)
		if err != nil {
			return nil, storageError(err)
		}
		targets = append(targets, fetchTarget{partition: e.Partition, offset: e.Offset, p: p})
	}
	return targets, nil
}

// fetchLoop is D-SL2-7's multi-partition server: TryFetch every entry with
// maxBytes budgeted across the whole response in request order; respond if
// ANY records; otherwise park one goroutine per entry on the captured
// notify channels. refence, when non-nil, is GroupFetch's serve-time fence
// (DD-12) — run before every serve attempt, including each wake.
func (s *Server) fetchLoop(targets []fetchTarget, maxBytes uint32, maxWait time.Duration, cancel <-chan struct{}, refence func() *wire.Error) (byte, []byte, *wire.Error, bool) {
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	for {
		if refence != nil {
			if werr := refence(); werr != nil {
				return 0, nil, werr, false
			}
		}
		groups := make([]wire.FetchGroup, 0, len(targets))
		notifies := make([]<-chan struct{}, len(targets))
		budget := int64(maxBytes)
		served := false
		for i, tgt := range targets {
			group := wire.FetchGroup{Partition: tgt.partition, Recs: []wire.Rec{}}
			if budget <= 0 {
				// Spent by earlier entries (only reachable once served):
				// this entry's legal zero-rec group, no read needed.
				groups = append(groups, group)
				continue
			}
			recs, notify, err := tgt.p.TryFetch(tgt.offset, uint32(budget))
			if err != nil {
				// Read failure on durable data has no protocol code: drop
				// the conn, don't invent (same rule as single-entry).
				return 0, nil, nil, true
			}
			notifies[i] = notify
			for _, r := range recs {
				sz := int64(12 + len(r.Payload))
				if served && sz > budget {
					// Min-one applies to the RESPONSE, not per entry: only
					// the response's FIRST record may exceed the budget (so
					// an oversized record cannot wedge a consumer); after
					// that the budget is strict, keeping the total under
					// the response frame cap.
					break
				}
				group.Recs = append(group.Recs, wire.Rec{Offset: r.Offset, Payload: r.Payload})
				budget -= sz
				served = true
			}
			groups = append(groups, group)
		}
		if served {
			return wire.TypeFetchResp, wire.FetchResp{Groups: groups}.Encode(), nil, false
		}

		// Nothing anywhere (a park round always starts with a full budget,
		// so data can never be hidden by budgeting): park one goroutine per
		// entry. The wake chan is buffered to len(targets) so a multi-wake
		// round can never block a loser mid-send where close(done) cannot
		// reach it; done is FRESH per round (reuse = double-close panic).
		wake := make(chan struct{}, len(targets))
		done := make(chan struct{})
		for i, tgt := range targets {
			unpark := tgt.p.TrackPark()
			go func(notify <-chan struct{}) {
				defer unpark()
				select {
				case <-notify:
					wake <- struct{}{}
				case <-done:
				}
			}(notifies[i])
		}
		// done closes on EVERY exit path of the round (D-SL2-7/F4).
		select {
		case <-wake:
			close(done)
		case <-timer.C:
			close(done)
			return wire.TypeFetchResp, emptyFetchShape(targets), nil, false
		case <-s.stopping:
			close(done)
			return wire.TypeFetchResp, emptyFetchShape(targets), nil, false
		case <-cancel:
			close(done)
			return 0, nil, nil, true
		}
	}
}

// emptyFetchShape is F7's pinned empty response: exactly one zero-rec
// group per requested entry, in request order.
func emptyFetchShape(targets []fetchTarget) []byte {
	groups := make([]wire.FetchGroup, 0, len(targets))
	for _, tgt := range targets {
		groups = append(groups, wire.FetchGroup{Partition: tgt.partition, Recs: []wire.Rec{}})
	}
	return wire.FetchResp{Groups: groups}.Encode()
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

func (s *Server) handleJoinGroup(payload []byte, connID uint64) (byte, []byte, *wire.Error) {
	m, werr := wire.DecodeJoinGroup(payload)
	if werr != nil {
		return 0, nil, werr
	}
	if err := wire.ValidateName(m.Group); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	if err := wire.ValidateName(m.Topic); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	// Unknown topic → UNKNOWN_TOPIC before any group state changes (D-SL2-6).
	partitions, err := s.store.TopicPartitions(m.Topic)
	if err != nil {
		return 0, nil, storageError(err)
	}
	res, err := s.coord.Join(connID, m.Group, m.Topic, partitions)
	if err != nil {
		return 0, nil, groupError(err)
	}
	resp := wire.JoinGroupResp{MemberID: res.MemberID, Generation: res.Generation,
		Assigned: make([]wire.AssignedPartition, 0, len(res.Assigned))}
	for _, a := range res.Assigned {
		resp.Assigned = append(resp.Assigned, wire.AssignedPartition{Partition: a.Partition, NextOffset: a.Next})
	}
	return wire.TypeJoinGroupResp, resp.Encode(), nil
}

func (s *Server) handleHeartbeat(payload []byte) (byte, []byte, *wire.Error) {
	m, werr := wire.DecodeHeartbeat(payload)
	if werr != nil {
		return 0, nil, werr
	}
	if err := wire.ValidateName(m.Group); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	// m.Generation is carried but deliberately NOT fenced (D-SL2-6/F1):
	// the REJOIN level is derived server-side; fencing heartbeats would
	// make REJOIN undeliverable and falsely sweep live members.
	rejoin, err := s.coord.Heartbeat(m.Group, m.MemberID)
	if err != nil {
		return 0, nil, groupError(err)
	}
	var flags uint8
	if rejoin {
		flags |= wire.HeartbeatRejoin
	}
	return wire.TypeHeartbeatResp, wire.HeartbeatResp{Flags: flags}.Encode(), nil
}

func (s *Server) handleCommitOffsets(payload []byte) (byte, []byte, *wire.Error) {
	m, werr := wire.DecodeCommitOffsets(payload)
	if werr != nil {
		return 0, nil, werr
	}
	if err := wire.ValidateName(m.Group); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	offsets := make(map[uint32]uint64, len(m.Entries))
	for _, e := range m.Entries {
		offsets[e.Partition] = e.Next
	}
	// The ack frame below is only reachable after the atomicWrite (CONS-3).
	if err := s.coord.Commit(m.Group, m.MemberID, m.Generation, offsets); err != nil {
		return 0, nil, groupError(err)
	}
	return wire.TypeCommitOffsetsResp, nil, nil
}

func (s *Server) handleLeaveGroup(payload []byte) (byte, []byte, *wire.Error) {
	m, werr := wire.DecodeLeaveGroup(payload)
	if werr != nil {
		return 0, nil, werr
	}
	if err := wire.ValidateName(m.Group); err != nil {
		return 0, nil, err.(*wire.Error)
	}
	if err := s.coord.Leave(m.Group, m.MemberID); err != nil {
		return 0, nil, groupError(err)
	}
	return wire.TypeLeaveGroupResp, nil, nil
}

func (s *Server) handleGroupFetch(payload []byte, cancel <-chan struct{}) (byte, []byte, *wire.Error, bool) {
	m, werr := wire.DecodeGroupFetch(payload)
	if werr != nil {
		return 0, nil, werr, false
	}
	if err := wire.ValidateName(m.Group); err != nil {
		return 0, nil, err.(*wire.Error), false
	}
	if len(m.Entries) == 0 {
		return 0, nil, wire.Errf(wire.CodeMalformed, "group fetch requires at least one entry"), false
	}
	if len(m.Entries) > MaxFetchEntries {
		return 0, nil, wire.Errf(wire.CodeFetchTooWide, "%d entries exceeds cap %d", len(m.Entries), MaxFetchEntries), false
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
	// The serve-time fence (DD-12) doubles as the topic lookup — GroupFetch
	// carries no topic field; the group's binding supplies it (D-SL2-2).
	refence := func() *wire.Error {
		if _, err := s.coord.ValidateFetch(m.Group, m.MemberID, m.Generation); err != nil {
			return groupError(err)
		}
		return nil
	}
	topic, err := s.coord.ValidateFetch(m.Group, m.MemberID, m.Generation)
	if err != nil {
		return 0, nil, groupError(err), false
	}
	targets, werr := s.resolveTargets(topic, m.Entries)
	if werr != nil {
		return 0, nil, werr, false
	}
	return s.fetchLoop(targets, maxBytes, maxWait, cancel, refence)
}

// groupError maps group-coordinator sentinels onto their pinned wire codes
// (the group package never imports wire). Precedence is decided in the
// coordinator (13 before 12, D-SL2-6); this is a pure translation.
func groupError(err error) *wire.Error {
	switch {
	case errors.Is(err, group.ErrUnknownMember):
		return wire.Errf(wire.CodeUnknownMember, "%v", err)
	case errors.Is(err, group.ErrStaleGeneration):
		return wire.Errf(wire.CodeStaleGeneration, "%v", err)
	case errors.Is(err, group.ErrTopicMismatch), errors.Is(err, group.ErrCorruptCommits):
		return wire.Errf(wire.CodeMalformed, "%v", err)
	case errors.Is(err, group.ErrTooManyGroups), errors.Is(err, group.ErrTooManyMembers):
		return wire.Errf(wire.CodeCapExceeded, "%v", err)
	default:
		// Commit-file persistence failures land here: the write failed, no
		// ack was sent (CONS-3's contract holds).
		return wire.Errf(wire.CodeWriteFailed, "internal group error: %v", err)
	}
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
