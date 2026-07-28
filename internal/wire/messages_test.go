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
