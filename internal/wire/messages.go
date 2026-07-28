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
