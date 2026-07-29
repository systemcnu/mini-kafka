// Encode/decode roundtrip tests per message body type (D-SL0-3).
package wire

import (
	"bytes"
	"reflect"
	"testing"
)

func TestProduceRoundtrip(t *testing.T) {
	in := Produce{Topic: "demo", Partition: 3, Payload: []byte("hello")}
	out, err := DecodeProduce(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestProduceEmptyPayloadRoundtrip(t *testing.T) {
	in := Produce{Topic: "t", Partition: 0, Payload: []byte{}}
	out, err := DecodeProduce(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Topic != "t" || out.Partition != 0 || len(out.Payload) != 0 {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestProduceRespRoundtrip(t *testing.T) {
	in := ProduceResp{Offset: 1<<40 + 7}
	out, err := DecodeProduceResp(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestCreateTopicRoundtrip(t *testing.T) {
	in := CreateTopic{Topic: "orders", Partitions: 16}
	out, err := DecodeCreateTopic(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestListTopicsRespRoundtrip(t *testing.T) {
	in := ListTopicsResp{Topics: []TopicInfo{
		{Name: "a", Partitions: 1},
		{Name: "b.v2", Partitions: 16},
	}}
	out, err := DecodeListTopicsResp(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
	// Empty list is legal.
	empty, err := DecodeListTopicsResp(ListTopicsResp{}.Encode())
	if err != nil || len(empty.Topics) != 0 {
		t.Fatalf("empty roundtrip = %+v, %v", empty, err)
	}
}

func TestFetchRoundtrip(t *testing.T) {
	in := Fetch{
		Topic:     "demo",
		Entries:   []FetchEntry{{Partition: 2, Offset: 41}},
		MaxWaitMs: 5000,
		MaxBytes:  1 << 20,
	}
	out, err := DecodeFetch(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestFetchMultiEntryRoundtrip(t *testing.T) {
	// The wire shape is multi-entry even though SL0 only serves one entry.
	in := Fetch{
		Topic: "demo",
		Entries: []FetchEntry{
			{Partition: 0, Offset: 1},
			{Partition: 1, Offset: 2},
			{Partition: 2, Offset: 3},
		},
		MaxWaitMs: 100,
		MaxBytes:  4096,
	}
	out, err := DecodeFetch(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestFetchRespRoundtrip(t *testing.T) {
	in := FetchResp{Groups: []FetchGroup{
		{Partition: 2, Recs: []Rec{
			{Offset: 41, Payload: []byte("a")},
			{Offset: 42, Payload: []byte("bb")},
		}},
	}}
	out, err := DecodeFetchResp(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
	// A zero-rec group IS the empty-at-timeout shape — must roundtrip.
	timeoutShape := FetchResp{Groups: []FetchGroup{{Partition: 0, Recs: []Rec{}}}}
	out, err = DecodeFetchResp(timeoutShape.Encode())
	if err != nil {
		t.Fatalf("decode empty group: %v", err)
	}
	if len(out.Groups) != 1 || len(out.Groups[0].Recs) != 0 {
		t.Fatalf("empty group roundtrip = %+v", out)
	}
}

func TestJoinGroupRoundtrip(t *testing.T) {
	in := JoinGroup{Group: "workers", Topic: "orders"}
	out, err := DecodeJoinGroup(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestJoinGroupRespRoundtrip(t *testing.T) {
	in := JoinGroupResp{
		MemberID:   "m3",
		Generation: 7,
		Assigned: []AssignedPartition{
			{Partition: 0, NextOffset: 12},
			{Partition: 1, NextOffset: 0},
		},
	}
	out, err := DecodeJoinGroupResp(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
	// A member can legally own zero partitions (more members than partitions).
	empty, err := DecodeJoinGroupResp(JoinGroupResp{MemberID: "m9", Generation: 1}.Encode())
	if err != nil || empty.MemberID != "m9" || len(empty.Assigned) != 0 {
		t.Fatalf("empty-assignment roundtrip = %+v, %v", empty, err)
	}
}

func TestHeartbeatRoundtrip(t *testing.T) {
	in := Heartbeat{Group: "workers", MemberID: "m3", Generation: 7}
	out, err := DecodeHeartbeat(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestHeartbeatRespRoundtrip(t *testing.T) {
	for _, flags := range []uint8{0, HeartbeatRejoin} {
		in := HeartbeatResp{Flags: flags}
		out, err := DecodeHeartbeatResp(in.Encode())
		if err != nil {
			t.Fatalf("decode flags %d: %v", flags, err)
		}
		if out != in {
			t.Fatalf("roundtrip = %+v, want %+v", out, in)
		}
	}
}

func TestCommitOffsetsRoundtrip(t *testing.T) {
	in := CommitOffsets{
		Group:      "workers",
		MemberID:   "m3",
		Generation: 7,
		Entries: []CommitEntry{
			{Partition: 0, Next: 42},
			{Partition: 3, Next: 1},
		},
	}
	out, err := DecodeCommitOffsets(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestLeaveGroupRoundtrip(t *testing.T) {
	in := LeaveGroup{Group: "workers", MemberID: "m3"}
	out, err := DecodeLeaveGroup(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestGroupFetchRoundtrip(t *testing.T) {
	in := GroupFetch{
		Group:      "workers",
		MemberID:   "m3",
		Generation: 7,
		Entries: []FetchEntry{
			{Partition: 0, Offset: 5},
			{Partition: 2, Offset: 0},
			{Partition: 3, Offset: 99},
		},
		MaxWaitMs: 5000,
		MaxBytes:  1 << 20,
	}
	out, err := DecodeGroupFetch(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

// TestGroupDecodesRejectTruncationAndTrailingBytes runs every group message
// type through the strict-decode gauntlet (D-SL0-3 discipline for the SL2
// types).
func TestGroupDecodesRejectTruncationAndTrailingBytes(t *testing.T) {
	cases := []struct {
		name   string
		enc    []byte
		decode func([]byte) *Error
	}{
		{"JoinGroup", JoinGroup{Group: "g", Topic: "t"}.Encode(),
			func(b []byte) *Error { _, e := DecodeJoinGroup(b); return e }},
		{"JoinGroupResp", JoinGroupResp{MemberID: "m1", Generation: 1, Assigned: []AssignedPartition{{Partition: 0, NextOffset: 1}}}.Encode(),
			func(b []byte) *Error { _, e := DecodeJoinGroupResp(b); return e }},
		{"Heartbeat", Heartbeat{Group: "g", MemberID: "m1", Generation: 1}.Encode(),
			func(b []byte) *Error { _, e := DecodeHeartbeat(b); return e }},
		{"HeartbeatResp", HeartbeatResp{Flags: 1}.Encode(),
			func(b []byte) *Error { _, e := DecodeHeartbeatResp(b); return e }},
		{"CommitOffsets", CommitOffsets{Group: "g", MemberID: "m1", Generation: 1, Entries: []CommitEntry{{Partition: 0, Next: 1}}}.Encode(),
			func(b []byte) *Error { _, e := DecodeCommitOffsets(b); return e }},
		{"LeaveGroup", LeaveGroup{Group: "g", MemberID: "m1"}.Encode(),
			func(b []byte) *Error { _, e := DecodeLeaveGroup(b); return e }},
		{"GroupFetch", GroupFetch{Group: "g", MemberID: "m1", Generation: 1, Entries: []FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1, MaxBytes: 1}.Encode(),
			func(b []byte) *Error { _, e := DecodeGroupFetch(b); return e }},
	}
	for _, tc := range cases {
		if err := tc.decode(tc.enc[:len(tc.enc)-1]); err == nil || err.Code != CodeMalformed {
			t.Errorf("%s truncated: err = %v, want MALFORMED", tc.name, err)
		}
		if err := tc.decode(append(bytes.Clone(tc.enc), 0x00)); err == nil || err.Code != CodeMalformed {
			t.Errorf("%s trailing: err = %v, want MALFORMED", tc.name, err)
		}
	}
}

// Hostile element counts in the counted group bodies must fail on the first
// missing element, never drive an allocation.
func TestGroupDecodesRejectHostileCounts(t *testing.T) {
	// JoinGroupResp: count sits after [u16 2]"m1" + u64 generation.
	jr := JoinGroupResp{MemberID: "m1", Generation: 1}.Encode()
	countAt := 2 + 2 + 8
	for i := 0; i < 4; i++ {
		jr[countAt+i] = 0xFF
	}
	if _, err := DecodeJoinGroupResp(jr); err == nil || err.Code != CodeMalformed {
		t.Errorf("JoinGroupResp hostile count: err = %v, want MALFORMED", err)
	}
	// CommitOffsets: count after [u16 1]"g" [u16 2]"m1" u64 generation.
	co := CommitOffsets{Group: "g", MemberID: "m1", Generation: 1}.Encode()
	countAt = 3 + 4 + 8
	for i := 0; i < 4; i++ {
		co[countAt+i] = 0xFF
	}
	if _, err := DecodeCommitOffsets(co); err == nil || err.Code != CodeMalformed {
		t.Errorf("CommitOffsets hostile count: err = %v, want MALFORMED", err)
	}
	// GroupFetch: count after [u16 1]"g" [u16 2]"m1" u64 generation.
	gf := GroupFetch{Group: "g", MemberID: "m1", Generation: 1, MaxWaitMs: 1, MaxBytes: 1}.Encode()
	for i := 0; i < 4; i++ {
		gf[countAt+i] = 0xFF
	}
	if _, err := DecodeGroupFetch(gf); err == nil || err.Code != CodeMalformed {
		t.Errorf("GroupFetch hostile count: err = %v, want MALFORMED", err)
	}
}

func TestErrorMsgRoundtrip(t *testing.T) {
	in := ErrorMsg{Code: uint16(CodeShuttingDown), Msg: "shutting down"}
	out, err := DecodeErrorMsg(in.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestDecodeRejectsTruncationAndTrailingBytes(t *testing.T) {
	enc := Produce{Topic: "demo", Partition: 1, Payload: []byte("xyz")}.Encode()
	if _, err := DecodeProduce(enc[:len(enc)-1]); err == nil || err.Code != CodeMalformed {
		t.Fatalf("truncated: err = %v, want MALFORMED", err)
	}
	if _, err := DecodeProduce(append(bytes.Clone(enc), 0x00)); err == nil || err.Code != CodeMalformed {
		t.Fatalf("trailing: err = %v, want MALFORMED", err)
	}
	// A hostile element count larger than the body can hold must fail, not
	// allocate.
	f := Fetch{Topic: "t", Entries: []FetchEntry{{Partition: 0, Offset: 0}}, MaxWaitMs: 1, MaxBytes: 1}
	b := f.Encode()
	// nEntries sits right after [u16 len]"t": bytes 0,1 are the strlen, 2 is 't', 3..6 the count.
	b[3], b[4], b[5], b[6] = 0xFF, 0xFF, 0xFF, 0xFF
	if _, err := DecodeFetch(b); err == nil || err.Code != CodeMalformed {
		t.Fatalf("hostile count: err = %v, want MALFORMED", err)
	}
}
