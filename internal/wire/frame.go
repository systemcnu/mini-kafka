// Frame envelope (D-SL0-2): [u32 len][u8 ver][u8 type][payload], big-endian,
// len covering everything after itself; plus the bounded byte-level
// encode/decode helpers the message bodies build on.
package wire

import (
	"encoding/binary"
	"io"
)

// Version is the only protocol version SL0 speaks.
const Version = 1

// Frame type values (D-SL0-2, pinned). Types 9–17 are the SL2 group
// messages (D-SL2-1, additive only).
const (
	TypeProduce           byte = 1
	TypeProduceResp       byte = 2
	TypeFetch             byte = 3
	TypeFetchResp         byte = 4
	TypeCreateTopic       byte = 5
	TypeCreateTopicResp   byte = 6
	TypeListTopics        byte = 7
	TypeListTopicsResp    byte = 8
	TypeJoinGroup         byte = 9
	TypeJoinGroupResp     byte = 10
	TypeHeartbeat         byte = 11
	TypeHeartbeatResp     byte = 12
	TypeCommitOffsets     byte = 13
	TypeCommitOffsetsResp byte = 14
	TypeLeaveGroup        byte = 15
	TypeLeaveGroupResp    byte = 16
	TypeGroupFetch        byte = 17
	TypeError             byte = 255
)

// Frame caps (D-SL0-8): total on-the-wire frame size including the 4-byte
// length prefix. Asymmetric so a max-maxBytes fetch response stays legal.
const (
	MaxRequestFrame  = 1<<20 + 4<<10
	MaxResponseFrame = 4<<20 + 64<<10
)

// WriteFrame writes one frame. The payload must already fit the peer's cap;
// WriteFrame itself only refuses lengths that cannot be represented.
func WriteFrame(w io.Writer, typ byte, payload []byte) error {
	b := make([]byte, 4+2+len(payload))
	binary.BigEndian.PutUint32(b, uint32(2+len(payload)))
	b[4] = Version
	b[5] = typ
	copy(b[6:], payload)
	_, err := w.Write(b)
	return err
}

// ReadFrame reads one frame, enforcing max (total frame bytes) BEFORE
// allocating the payload. It returns *Error{FRAME_TOO_LARGE} on an oversized
// frame and *Error{MALFORMED} on a short length or unsupported version;
// plain I/O errors (io.EOF for a clean close between frames,
// io.ErrUnexpectedEOF mid-frame) pass through untouched.
func ReadFrame(r io.Reader, max uint32) (typ byte, payload []byte, err error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < 2 {
		return 0, nil, Errf(CodeMalformed, "frame length %d below minimum", length)
	}
	if 4+uint64(length) > uint64(max) {
		return 0, nil, Errf(CodeFrameTooLarge, "frame of %d bytes exceeds cap %d", 4+uint64(length), max)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			// Header arrived but the body did not: still a mid-frame cut.
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	if body[0] != Version {
		return 0, nil, Errf(CodeMalformed, "unsupported version %d", body[0])
	}
	return body[1], body[2:], nil
}

// buf accumulates a message body: big-endian primitives, [u16 n] strings,
// [u32 n] blobs.
type buf struct{ b []byte }

func (w *buf) u8(v uint8)    { w.b = append(w.b, v) }
func (w *buf) u16(v uint16)  { w.b = binary.BigEndian.AppendUint16(w.b, v) }
func (w *buf) u32(v uint32)  { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *buf) u64(v uint64)  { w.b = binary.BigEndian.AppendUint64(w.b, v) }
func (w *buf) str(s string)  { w.u16(uint16(len(s))); w.b = append(w.b, s...) }
func (w *buf) blob(p []byte) { w.u32(uint32(len(p))); w.b = append(w.b, p...) }

// reader decodes a message body. Every length is checked against the bytes
// actually present (the buffer is already frame-cap bounded), so a hostile
// length can never drive an allocation past the frame it arrived in. The
// first failure sticks; check err after decoding.
type reader struct {
	b   []byte
	off int
	err *Error
}

func (r *reader) fail() {
	if r.err == nil {
		r.err = Errf(CodeMalformed, "truncated message body at byte %d", r.off)
	}
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.off+n > len(r.b) {
		r.fail()
		return nil
	}
	out := r.b[r.off : r.off+n]
	r.off += n
	return out
}

func (r *reader) u8() uint8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (r *reader) u16() uint16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}

func (r *reader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (r *reader) str() string  { return string(r.take(int(r.u16()))) }
func (r *reader) blob() []byte { return append([]byte(nil), r.take(int(r.u32()))...) }

// done returns the sticky decode error, treating unconsumed trailing bytes
// as malformed — a strict decode keeps the registry honest.
func (r *reader) done() *Error {
	if r.err == nil && r.off != len(r.b) {
		r.err = Errf(CodeMalformed, "%d trailing bytes after message body", len(r.b)-r.off)
	}
	return r.err
}
