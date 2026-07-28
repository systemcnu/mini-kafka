// Error code registry (D-SL0-8): values are pinned and only ever added to.
package wire

import "fmt"

// Code is a stable protocol error code carried in Error frames.
type Code uint16

// Pinned code values (D-SL0-8). SL2 adds 12 STALE_GENERATION and
// 13 UNKNOWN_MEMBER; nothing here may be renumbered.
const (
	CodeUnknownTopic  Code = 1
	CodeTopicExists   Code = 2
	CodeBadPartition  Code = 3
	CodeInvalidName   Code = 4
	CodeMsgTooLarge   Code = 5
	CodeFrameTooLarge Code = 6
	CodeMalformed     Code = 7
	CodeCapExceeded   Code = 8
	CodeFetchTooWide  Code = 9
	CodeShuttingDown  Code = 10
	CodeWriteFailed   Code = 11
)

// Error is a protocol error; it crosses the wire as message type 255.
type Error struct {
	Code Code
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("wire error %d: %s", e.Code, e.Msg) }

// Errf builds an *Error with a formatted message.
func Errf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}
