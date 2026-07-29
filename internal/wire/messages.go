// Typed encode/decode per message body (D-SL0-3). Decodes are strict: any
// truncation, hostile length, or trailing byte is MALFORMED.
package wire

// Produce is message type 1: [str topic][u32 partition][blob payload].
type Produce struct {
	Topic     string
	Partition uint32
	Payload   []byte
}

// Encode serializes the message body (no frame envelope).
func (m Produce) Encode() []byte {
	var w buf
	w.str(m.Topic)
	w.u32(m.Partition)
	w.blob(m.Payload)
	return w.b
}

// DecodeProduce parses a Produce body.
func DecodeProduce(b []byte) (Produce, *Error) {
	r := reader{b: b}
	m := Produce{Topic: r.str(), Partition: r.u32(), Payload: r.blob()}
	return m, r.done()
}

// ProduceResp is message type 2: [u64 offset].
type ProduceResp struct {
	Offset uint64
}

// Encode serializes the message body (no frame envelope).
func (m ProduceResp) Encode() []byte {
	var w buf
	w.u64(m.Offset)
	return w.b
}

// DecodeProduceResp parses a ProduceResp body.
func DecodeProduceResp(b []byte) (ProduceResp, *Error) {
	r := reader{b: b}
	m := ProduceResp{Offset: r.u64()}
	return m, r.done()
}

// CreateTopic is message type 5: [str topic][u32 partitions].
type CreateTopic struct {
	Topic      string
	Partitions uint32
}

// Encode serializes the message body (no frame envelope).
func (m CreateTopic) Encode() []byte {
	var w buf
	w.str(m.Topic)
	w.u32(m.Partitions)
	return w.b
}

// DecodeCreateTopic parses a CreateTopic body.
func DecodeCreateTopic(b []byte) (CreateTopic, *Error) {
	r := reader{b: b}
	m := CreateTopic{Topic: r.str(), Partitions: r.u32()}
	return m, r.done()
}

// CreateTopicResp (type 6) and ListTopics (type 7) have empty bodies.

// TopicInfo is one entry of a ListTopicsResp.
type TopicInfo struct {
	Name       string
	Partitions uint32
}

// ListTopicsResp is message type 8: [u32 n]{[str name][u32 partitions]}.
type ListTopicsResp struct {
	Topics []TopicInfo
}

// Encode serializes the message body (no frame envelope).
func (m ListTopicsResp) Encode() []byte {
	var w buf
	w.u32(uint32(len(m.Topics)))
	for _, t := range m.Topics {
		w.str(t.Name)
		w.u32(t.Partitions)
	}
	return w.b
}

// DecodeListTopicsResp parses a ListTopicsResp body.
func DecodeListTopicsResp(b []byte) (ListTopicsResp, *Error) {
	r := reader{b: b}
	n := r.u32()
	m := ListTopicsResp{}
	// No preallocation from the count: a hostile n fails on its first
	// missing element instead of driving a giant make().
	for i := uint32(0); i < n && r.err == nil; i++ {
		m.Topics = append(m.Topics, TopicInfo{Name: r.str(), Partitions: r.u32()})
	}
	return m, r.done()
}

// FetchEntry names one (partition, offset) a Fetch asks for.
type FetchEntry struct {
	Partition uint32
	Offset    uint64
}

// Fetch is message type 3:
// [str topic][u32 nEntries]{[u32 partition][u64 offset]}[u32 maxWaitMs][u32 maxBytes].
type Fetch struct {
	Topic     string
	Entries   []FetchEntry
	MaxWaitMs uint32
	MaxBytes  uint32
}

// Encode serializes the message body (no frame envelope).
func (m Fetch) Encode() []byte {
	var w buf
	w.str(m.Topic)
	w.u32(uint32(len(m.Entries)))
	for _, e := range m.Entries {
		w.u32(e.Partition)
		w.u64(e.Offset)
	}
	w.u32(m.MaxWaitMs)
	w.u32(m.MaxBytes)
	return w.b
}

// DecodeFetch parses a Fetch body.
func DecodeFetch(b []byte) (Fetch, *Error) {
	r := reader{b: b}
	m := Fetch{Topic: r.str()}
	n := r.u32()
	for i := uint32(0); i < n && r.err == nil; i++ {
		m.Entries = append(m.Entries, FetchEntry{Partition: r.u32(), Offset: r.u64()})
	}
	m.MaxWaitMs = r.u32()
	m.MaxBytes = r.u32()
	return m, r.done()
}

// Rec is one record of a FetchResp group.
type Rec struct {
	Offset  uint64
	Payload []byte
}

// FetchGroup is the per-partition group of a FetchResp; zero records is the
// legal empty-at-timeout shape.
type FetchGroup struct {
	Partition uint32
	Recs      []Rec
}

// FetchResp is message type 4:
// [u32 nGroups]{[u32 partition][u32 nRecs]{[u64 offset][blob payload]}}.
type FetchResp struct {
	Groups []FetchGroup
}

// Encode serializes the message body (no frame envelope).
func (m FetchResp) Encode() []byte {
	var w buf
	w.u32(uint32(len(m.Groups)))
	for _, g := range m.Groups {
		w.u32(g.Partition)
		w.u32(uint32(len(g.Recs)))
		for _, rec := range g.Recs {
			w.u64(rec.Offset)
			w.blob(rec.Payload)
		}
	}
	return w.b
}

// DecodeFetchResp parses a FetchResp body.
func DecodeFetchResp(b []byte) (FetchResp, *Error) {
	r := reader{b: b}
	m := FetchResp{}
	nGroups := r.u32()
	for i := uint32(0); i < nGroups && r.err == nil; i++ {
		g := FetchGroup{Partition: r.u32(), Recs: []Rec{}}
		nRecs := r.u32()
		for j := uint32(0); j < nRecs && r.err == nil; j++ {
			g.Recs = append(g.Recs, Rec{Offset: r.u64(), Payload: r.blob()})
		}
		m.Groups = append(m.Groups, g)
	}
	return m, r.done()
}

// JoinGroup is message type 9: [str group][str topic]. Joining an existing
// group with a different topic is MALFORMED (one topic per group, D15).
type JoinGroup struct {
	Group string
	Topic string
}

// Encode serializes the message body (no frame envelope).
func (m JoinGroup) Encode() []byte {
	var w buf
	w.str(m.Group)
	w.str(m.Topic)
	return w.b
}

// DecodeJoinGroup parses a JoinGroup body.
func DecodeJoinGroup(b []byte) (JoinGroup, *Error) {
	r := reader{b: b}
	m := JoinGroup{Group: r.str(), Topic: r.str()}
	return m, r.done()
}

// AssignedPartition is one entry of a JoinGroupResp: an owned partition and
// the committed next-to-read offset the member resumes from (DD-14).
type AssignedPartition struct {
	Partition  uint32
	NextOffset uint64
}

// JoinGroupResp is message type 10:
// [str memberID][u64 generation][u32 n]{[u32 partition][u64 nextOffset]} —
// join carries the whole resume state, no separate offset-fetch round.
type JoinGroupResp struct {
	MemberID   string
	Generation uint64
	Assigned   []AssignedPartition
}

// Encode serializes the message body (no frame envelope).
func (m JoinGroupResp) Encode() []byte {
	var w buf
	w.str(m.MemberID)
	w.u64(m.Generation)
	w.u32(uint32(len(m.Assigned)))
	for _, a := range m.Assigned {
		w.u32(a.Partition)
		w.u64(a.NextOffset)
	}
	return w.b
}

// DecodeJoinGroupResp parses a JoinGroupResp body.
func DecodeJoinGroupResp(b []byte) (JoinGroupResp, *Error) {
	r := reader{b: b}
	m := JoinGroupResp{MemberID: r.str(), Generation: r.u64()}
	n := r.u32()
	for i := uint32(0); i < n && r.err == nil; i++ {
		m.Assigned = append(m.Assigned, AssignedPartition{Partition: r.u32(), NextOffset: r.u64()})
	}
	return m, r.done()
}

// Heartbeat is message type 11: [str group][str memberID][u64 generation].
// Heartbeats are exempt from the generation fence (D-SL2-6): only an unknown
// member errors one.
type Heartbeat struct {
	Group      string
	MemberID   string
	Generation uint64
}

// Encode serializes the message body (no frame envelope).
func (m Heartbeat) Encode() []byte {
	var w buf
	w.str(m.Group)
	w.str(m.MemberID)
	w.u64(m.Generation)
	return w.b
}

// DecodeHeartbeat parses a Heartbeat body.
func DecodeHeartbeat(b []byte) (Heartbeat, *Error) {
	r := reader{b: b}
	m := Heartbeat{Group: r.str(), MemberID: r.str(), Generation: r.u64()}
	return m, r.done()
}

// HeartbeatRejoin is HeartbeatResp's flags bit0: set while the member's
// joined generation trails the group's — level-triggered, never a
// consumable flag (D-SL2-3).
const HeartbeatRejoin uint8 = 1

// HeartbeatResp is message type 12: [u8 flags] (bit0 = REJOIN).
type HeartbeatResp struct {
	Flags uint8
}

// Encode serializes the message body (no frame envelope).
func (m HeartbeatResp) Encode() []byte {
	var w buf
	w.u8(m.Flags)
	return w.b
}

// DecodeHeartbeatResp parses a HeartbeatResp body.
func DecodeHeartbeatResp(b []byte) (HeartbeatResp, *Error) {
	r := reader{b: b}
	m := HeartbeatResp{Flags: r.u8()}
	return m, r.done()
}

// CommitEntry is one partition's committed position — next-to-read,
// SPEC §1b/D13.
type CommitEntry struct {
	Partition uint32
	Next      uint64
}

// CommitOffsets is message type 13:
// [str group][str memberID][u64 generation][u32 n]{[u32 partition][u64 next]}.
// Fenced at serve time (DD-12); the ack means the commit is durable (CONS-3).
type CommitOffsets struct {
	Group      string
	MemberID   string
	Generation uint64
	Entries    []CommitEntry
}

// Encode serializes the message body (no frame envelope).
func (m CommitOffsets) Encode() []byte {
	var w buf
	w.str(m.Group)
	w.str(m.MemberID)
	w.u64(m.Generation)
	w.u32(uint32(len(m.Entries)))
	for _, e := range m.Entries {
		w.u32(e.Partition)
		w.u64(e.Next)
	}
	return w.b
}

// DecodeCommitOffsets parses a CommitOffsets body.
func DecodeCommitOffsets(b []byte) (CommitOffsets, *Error) {
	r := reader{b: b}
	m := CommitOffsets{Group: r.str(), MemberID: r.str(), Generation: r.u64()}
	n := r.u32()
	for i := uint32(0); i < n && r.err == nil; i++ {
		m.Entries = append(m.Entries, CommitEntry{Partition: r.u32(), Next: r.u64()})
	}
	return m, r.done()
}

// CommitOffsetsResp (type 14) and LeaveGroupResp (type 16) have empty bodies.

// LeaveGroup is message type 15: [str group][str memberID].
type LeaveGroup struct {
	Group    string
	MemberID string
}

// Encode serializes the message body (no frame envelope).
func (m LeaveGroup) Encode() []byte {
	var w buf
	w.str(m.Group)
	w.str(m.MemberID)
	return w.b
}

// DecodeLeaveGroup parses a LeaveGroup body.
func DecodeLeaveGroup(b []byte) (LeaveGroup, *Error) {
	r := reader{b: b}
	m := LeaveGroup{Group: r.str(), MemberID: r.str()}
	return m, r.done()
}

// GroupFetch is message type 17: [str group][str memberID][u64 generation]
// plus Fetch's exact entry/maxWait/maxBytes tail
// ([u32 nEntries]{[u32 partition][u64 offset]}[u32 maxWaitMs][u32 maxBytes]).
// It exists because DD-12 requires group fetches to carry (memberID,
// generation) and the shipped Fetch shape has no such fields (D-SL2-2); the
// topic is implied by the group's binding. Answered by FetchResp (type 4,
// reused — one decode path).
type GroupFetch struct {
	Group      string
	MemberID   string
	Generation uint64
	Entries    []FetchEntry
	MaxWaitMs  uint32
	MaxBytes   uint32
}

// Encode serializes the message body (no frame envelope).
func (m GroupFetch) Encode() []byte {
	var w buf
	w.str(m.Group)
	w.str(m.MemberID)
	w.u64(m.Generation)
	w.u32(uint32(len(m.Entries)))
	for _, e := range m.Entries {
		w.u32(e.Partition)
		w.u64(e.Offset)
	}
	w.u32(m.MaxWaitMs)
	w.u32(m.MaxBytes)
	return w.b
}

// DecodeGroupFetch parses a GroupFetch body.
func DecodeGroupFetch(b []byte) (GroupFetch, *Error) {
	r := reader{b: b}
	m := GroupFetch{Group: r.str(), MemberID: r.str(), Generation: r.u64()}
	n := r.u32()
	for i := uint32(0); i < n && r.err == nil; i++ {
		m.Entries = append(m.Entries, FetchEntry{Partition: r.u32(), Offset: r.u64()})
	}
	m.MaxWaitMs = r.u32()
	m.MaxBytes = r.u32()
	return m, r.done()
}

// ErrorMsg is message type 255: [u16 code][str msg].
type ErrorMsg struct {
	Code uint16
	Msg  string
}

// Encode serializes the message body (no frame envelope).
func (m ErrorMsg) Encode() []byte {
	var w buf
	w.u16(m.Code)
	w.str(m.Msg)
	return w.b
}

// DecodeErrorMsg parses an Error body.
func DecodeErrorMsg(b []byte) (ErrorMsg, *Error) {
	r := reader{b: b}
	m := ErrorMsg{Code: r.u16(), Msg: r.str()}
	return m, r.done()
}
