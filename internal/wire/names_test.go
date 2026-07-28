// INVALID_NAME table test for DD-18's name rule.
package wire

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	valid := []string{
		"a",
		"0",
		"demo",
		"a1",
		"a.b-c_d",
		"topic.v2",
		strings.Repeat("x", 128), // max length
	}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{
		"",                       // empty
		"A",                      // uppercase
		"Topic",                  // uppercase
		"-a",                     // bad first char
		".a",                     // bad first char
		"_a",                     // bad first char
		"..",                     // bad first char (path traversal shape)
		"a/b",                    // path separator
		"a\\b",                   // path separator
		"a b",                    // space
		"a\x00b",                 // NUL
		"a\n",                    // newline
		"café",                   // non-ASCII
		strings.Repeat("x", 129), // over max length
		"../etc",                 // traversal
	}
	for _, name := range invalid {
		err := ValidateName(name)
		var werr *Error
		if !errors.As(err, &werr) || werr.Code != CodeInvalidName {
			t.Errorf("ValidateName(%q) = %v, want Error{INVALID_NAME}", name, err)
		}
	}
}
