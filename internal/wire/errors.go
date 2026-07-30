// Error code registry (D-SL0-8): values are pinned and only ever added to.
package wire

import "fmt"

// Code is a stable protocol error code carried in Error frames.
type Code uint16

// Pinned code values (D-SL0-8); 12/13 activated by SL2 (D-SL2-1). Nothing
// here may be renumbered.
const (
	CodeUnknownTopic    Code = 1
	CodeTopicExists     Code = 2
	CodeBadPartition    Code = 3
	CodeInvalidName     Code = 4
	CodeMsgTooLarge     Code = 5
	CodeFrameTooLarge   Code = 6
	CodeMalformed       Code = 7
	CodeCapExceeded     Code = 8
	CodeFetchTooWide    Code = 9
	CodeShuttingDown    Code = 10
	CodeWriteFailed     Code = 11
	CodeStaleGeneration Code = 12
	CodeUnknownMember   Code = 13
)

// AllCodes returns every registered code, ascending. Maintained adjacent to
// the const block above; errors_test.go parses that block from source and
// fails if a declared constant is missing here (D-SL4-1), and the broker
// battery elicits every entry live — so this list is the machine-readable
// registry PROT-1's diff will consume.
func AllCodes() []Code {
	return []Code{
		CodeUnknownTopic, CodeTopicExists, CodeBadPartition, CodeInvalidName,
		CodeMsgTooLarge, CodeFrameTooLarge, CodeMalformed, CodeCapExceeded,
		CodeFetchTooWide, CodeShuttingDown, CodeWriteFailed,
		CodeStaleGeneration, CodeUnknownMember,
	}
}

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
