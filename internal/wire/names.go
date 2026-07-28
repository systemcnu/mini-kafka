// Name validation (DD-18): enforced in the protocol layer before any
// filesystem path is formed, so `..`, separators, empty and NUL are
// structurally impossible downstream.
package wire

import "regexp"

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// ValidateName returns *Error{INVALID_NAME} unless name matches the pinned
// topic/group name rule (lowercase alphanumeric start, ≤128 bytes).
func ValidateName(name string) error {
	if !nameRE.MatchString(name) {
		return Errf(CodeInvalidName, "invalid name %q", name)
	}
	return nil
}
