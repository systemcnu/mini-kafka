// -render-readme: the pure report→section function and the separate README
// marker splice (D-SL5-4).
package main

// The marker-delimited README section the renderer owns (D-SL5-4).
const (
	markerBegin = "<!-- bench:begin -->"
	markerEnd   = "<!-- bench:end -->"
)

// render is D-SL5-4's pure function: the whole marker-delimited section
// from one Report. SKELETON: markers only.
func render(Report) string { return markerBegin + "\n" + markerEnd }

// spliceReadme replaces the marker-delimited section (or appends it at the
// pinned anchor when markers are absent). SKELETON: identity.
func spliceReadme(readme []byte, _ string) ([]byte, error) { return readme, nil }
