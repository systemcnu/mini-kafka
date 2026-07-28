// Frame envelope tests: roundtrip, bounded decode, pinned rejection
// behavior (D-SL0-2, D-SL0-8).
package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundtrip(t *testing.T) {
	cases := []struct {
		name    string
		typ     byte
		payload []byte
	}{
		{"empty payload", TypeListTopics, nil},
		{"small payload", TypeProduce, []byte{0x01, 0x02, 0x03}},
		{"error type", TypeError, []byte{0x00, 0x07}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, c.typ, c.payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			typ, payload, err := ReadFrame(&buf, MaxRequestFrame)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if typ != c.typ {
				t.Errorf("type = %d, want %d", typ, c.typ)
			}
			if !bytes.Equal(payload, c.payload) {
				t.Errorf("payload = %x, want %x", payload, c.payload)
			}
		})
	}
}

// rawFrame hand-builds a frame so tests can produce invalid envelopes.
func rawFrame(length uint32, rest []byte) *bytes.Reader {
	b := make([]byte, 4+len(rest))
	binary.BigEndian.PutUint32(b, length)
	copy(b[4:], rest)
	return bytes.NewReader(b)
}

func TestReadFrameTooLarge(t *testing.T) {
	// Total frame size (4-byte prefix + len) over the cap must be rejected
	// before any payload allocation, with the pinned code.
	r := rawFrame(MaxRequestFrame-4+1, nil)
	_, _, err := ReadFrame(r, MaxRequestFrame)
	var werr *Error
	if !errors.As(err, &werr) || werr.Code != CodeFrameTooLarge {
		t.Fatalf("err = %v, want Error{FRAME_TOO_LARGE}", err)
	}
}

func TestReadFrameAtCapOK(t *testing.T) {
	// A frame exactly at the cap is legal.
	payload := make([]byte, MaxRequestFrame-4-2)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, TypeProduce, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if _, got, err := ReadFrame(&buf, MaxRequestFrame); err != nil || len(got) != len(payload) {
		t.Fatalf("ReadFrame = len %d, %v; want len %d, nil", len(got), err, len(payload))
	}
}

func TestReadFrameLenBelowMinimum(t *testing.T) {
	// len covers ver+type, so anything under 2 is malformed.
	for _, l := range []uint32{0, 1} {
		_, _, err := ReadFrame(rawFrame(l, []byte{Version}), MaxRequestFrame)
		var werr *Error
		if !errors.As(err, &werr) || werr.Code != CodeMalformed {
			t.Fatalf("len %d: err = %v, want Error{MALFORMED}", l, err)
		}
	}
}

func TestReadFrameBadVersion(t *testing.T) {
	_, _, err := ReadFrame(rawFrame(2, []byte{Version + 1, TypeProduce}), MaxRequestFrame)
	var werr *Error
	if !errors.As(err, &werr) || werr.Code != CodeMalformed {
		t.Fatalf("err = %v, want Error{MALFORMED} for unsupported version", err)
	}
}

func TestReadFramePartialAtEOF(t *testing.T) {
	// A clean close between frames is io.EOF; a mid-frame close is an
	// unexpected EOF. Both are plain I/O errors, not protocol Errors.
	_, _, err := ReadFrame(bytes.NewReader(nil), MaxRequestFrame)
	if err != io.EOF {
		t.Fatalf("empty stream: err = %v, want io.EOF", err)
	}
	_, _, err = ReadFrame(rawFrame(100, []byte{Version, TypeProduce, 0x01}), MaxRequestFrame)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated frame: err = %v, want unexpected EOF", err)
	}
	var werr *Error
	if errors.As(err, &werr) {
		t.Fatalf("truncated frame must not be a protocol Error, got %v", err)
	}
}
