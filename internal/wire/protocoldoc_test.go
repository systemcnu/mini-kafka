// Registry-diff test (D-SL6-3, PROT-1): docs/PROTOCOL.md's two
// marker-bounded registry tables are machine-diffed against this package's
// own source, both directions, on every run. Unlike the bench render gate's
// bootstrap skip (cmd/bench/render_test.go — legal only while no report is
// committed), there is NO bootstrap state for the protocol doc: a missing
// file, a missing marker, or an unparseable table is a hard t.Fatal naming
// what is absent, never a skip — absence of the doc is exactly the drift
// this test exists to catch.
//
// Grammar (D-SL6-2, restated in situ inside each begin marker): between
// <!-- registry:<name>:begin --> and <!-- registry:<name>:end --> the only
// legal line classes are blank lines, the in-situ grammar HTML comment,
// exactly one header row, exactly one separator row, and data rows whose
// first cell parses as a base-10 integer. Cells split on UNESCAPED `|` only
// (`\|` is a literal pipe inside a cell). Parsed row count must EQUAL the
// registry's cardinality, so a duplicated or deleted row is a failure.
// Doc names derive mechanically from the Go identifiers (ledger row 3):
// CodeMsgTooLarge → MSG_TOO_LARGE, TypeGroupFetch → GroupFetch — no
// hand-maintained name map anywhere.
package wire

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

var (
	codesHeader = []string{"code", "name", "meaning", "when sent"}
	typesHeader = []string{"type", "name", "direction", "response", "body summary"}
)

func TestProtocolDoc(t *testing.T) {
	data, err := os.ReadFile("../../docs/PROTOCOL.md")
	if err != nil {
		t.Fatalf("reading ../../docs/PROTOCOL.md: %v", err)
	}
	doc := string(data)

	codeRows := parseRegistryTable(t, doc, "codes", codesHeader)
	typeRows := parseRegistryTable(t, doc, "types", typesHeader)

	t.Run("legA codes table ≡ errors.go ≡ AllCodes", func(t *testing.T) {
		declared := declaredCodes(t) // errors_test.go's go/parser precedent (D-SL4-1)
		if len(declared) == 0 {
			t.Fatal("parsed no Code constants from errors.go")
		}
		all := AllCodes()
		if len(all) != len(declared) {
			t.Fatalf("AllCodes() lists %d codes, errors.go declares %d", len(all), len(declared))
		}
		listed := make(map[Code]bool, len(all))
		for _, c := range all {
			if listed[c] {
				t.Fatalf("AllCodes() lists code %d twice", c)
			}
			listed[c] = true
			if _, ok := declared[c]; !ok {
				t.Fatalf("AllCodes() lists %d, which no declared constant carries", c)
			}
		}
		if len(codeRows) != len(declared) {
			t.Fatalf("codes table has %d rows, registry has %d", len(codeRows), len(declared))
		}
		docCodes := make(map[uint64]string, len(codeRows))
		for _, row := range codeRows {
			v, err := strconv.ParseUint(row[0], 10, 16)
			if err != nil {
				t.Fatalf("codes row key %q does not parse as u16: %v", row[0], err)
			}
			if prev, dup := docCodes[v]; dup {
				t.Fatalf("codes table lists code %d twice (%s and %s)", v, prev, row[1])
			}
			docCodes[v] = row[1]
		}
		for v, ident := range declared {
			want := upperSnake(strings.TrimPrefix(ident, "Code"))
			got, ok := docCodes[uint64(v)]
			if !ok {
				t.Errorf("declared code %s = %d has no doc row", ident, v)
				continue
			}
			if got != want {
				t.Errorf("doc names code %d %q, want %q (derived from %s)", v, got, want, ident)
			}
		}
		for v, name := range docCodes {
			if _, ok := declared[Code(v)]; !ok {
				t.Errorf("doc codes table lists %d (%s), which no declared constant carries", v, name)
			}
		}
	})

	t.Run("legB types table ≡ package-wide Type consts", func(t *testing.T) {
		parsed := declaredTypes(t)
		if len(parsed) == 0 {
			t.Fatal("parsed no Type byte constants from the wire package sources")
		}
		// Contiguity: exactly {1..17, 255}, 18 entries (values derived by
		// command at plan time; a disagreement means the TREE changed).
		if len(parsed) != 18 {
			t.Errorf("wire package declares %d message types, want 18 (1..17 + 255)", len(parsed))
		}
		for v := uint64(1); v <= 17; v++ {
			if _, ok := parsed[v]; !ok {
				t.Errorf("type value %d is missing: the registry must be contiguous 1..17 plus 255", v)
			}
		}
		if _, ok := parsed[255]; !ok {
			t.Errorf("type value 255 (Error) is missing from the parsed registry")
		}
		if len(typeRows) != len(parsed) {
			t.Fatalf("types table has %d rows, registry has %d", len(typeRows), len(parsed))
		}
		docTypes := make(map[uint64]string, len(typeRows))
		for _, row := range typeRows {
			v, err := strconv.ParseUint(row[0], 10, 8)
			if err != nil {
				t.Fatalf("types row key %q does not parse as u8: %v", row[0], err)
			}
			if prev, dup := docTypes[v]; dup {
				t.Fatalf("types table lists type %d twice (%s and %s)", v, prev, row[1])
			}
			docTypes[v] = row[1]
		}
		for v, ident := range parsed {
			want := strings.TrimPrefix(ident, "Type")
			got, ok := docTypes[v]
			if !ok {
				t.Errorf("declared type %s = %d has no doc row", ident, v)
				continue
			}
			if got != want {
				t.Errorf("doc names type %d %q, want %q (derived from %s)", v, got, want, ident)
			}
		}
		for v, name := range docTypes {
			if _, ok := parsed[v]; !ok {
				t.Errorf("doc types table lists %d (%s), which no declared constant carries", v, name)
			}
		}
	})

	t.Run("legC every prose cell non-empty", func(t *testing.T) {
		for _, row := range codeRows {
			for i := 2; i < len(codesHeader); i++ {
				if row[i] == "" {
					t.Errorf("codes row %s (%s): %q cell is empty — a row that names a code but explains nothing is a failure", row[0], row[1], codesHeader[i])
				}
			}
		}
		for _, row := range typeRows {
			for i := 2; i < len(typesHeader); i++ {
				if row[i] == "" {
					t.Errorf("types row %s (%s): %q cell is empty — a row that names a type but explains nothing is a failure", row[0], row[1], typesHeader[i])
				}
			}
		}
	})

	t.Run("legD no resize operation documented (TOP-2)", func(t *testing.T) {
		resizeRE := regexp.MustCompile(`(?i)resize|alter|grow|shrink|repartition`)
		for _, row := range typeRows {
			if resizeRE.MatchString(row[1]) {
				t.Errorf("documented type %s matches a resize-family name — TOP-2 forbids a resize operation", row[1])
			}
		}
		if !strings.Contains(doc, "no resize") {
			t.Errorf("docs/PROTOCOL.md lacks the fixed-partition statement (expected the phrase %q)", "no resize")
		}
	})
}

// declaredTypes parses EVERY non-test .go file in the package dir — never
// just frame.go — and returns each constant declared with BOTH the Type
// name prefix AND explicit type byte (never one filter alone: a future
// non-message byte const must not sweep in), value → identifier.
func declaredTypes(t *testing.T) map[uint64]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package dir: %v", err)
	}
	out := make(map[uint64]string)
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				ident, ok := vs.Type.(*ast.Ident)
				if !ok || ident.Name != "byte" {
					continue
				}
				for i, n := range vs.Names {
					if !strings.HasPrefix(n.Name, "Type") {
						continue
					}
					lit, ok := vs.Values[i].(*ast.BasicLit)
					if !ok {
						t.Fatalf("constant %s is not a plain literal", n.Name)
					}
					v, err := strconv.ParseUint(lit.Value, 10, 8)
					if err != nil {
						t.Fatalf("constant %s value %q: %v", n.Name, lit.Value, err)
					}
					if prev, dup := out[v]; dup {
						t.Fatalf("type value %d declared twice (%s in an earlier file, %s in %s)", v, prev, n.Name, name)
					}
					out[v] = n.Name
				}
			}
		}
	}
	return out
}

// parseRegistryTable extracts the marker-bounded block and parses it under
// exactly the D-SL6-2 grammar. Any illegal line is a hard failure naming
// the line — strict-parse-or-fail, because a lenient skip is silent drift.
func parseRegistryTable(t *testing.T, doc, name string, header []string) [][]string {
	t.Helper()
	begin := "<!-- registry:" + name + ":begin -->"
	end := "<!-- registry:" + name + ":end -->"
	bi := strings.Index(doc, begin)
	if bi < 0 {
		t.Fatalf("docs/PROTOCOL.md is missing the %s marker — the %s registry table is absent", begin, name)
	}
	rest := doc[bi+len(begin):]
	ei := strings.Index(rest, end)
	if ei < 0 {
		t.Fatalf("docs/PROTOCOL.md is missing the %s marker after its begin marker", end)
	}
	block := rest[:ei]

	sepRE := regexp.MustCompile(`^:?-+:?$`)
	var rows [][]string
	var haveHeader, haveSep bool
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
			// blank — legal
		case strings.HasPrefix(trimmed, "<!--") && strings.HasSuffix(trimmed, "-->"):
			// the in-situ grammar HTML comment (D-SL6-2) — a legal class
		case strings.HasPrefix(trimmed, "|"):
			cells := splitCells(trimmed)
			if len(cells) == 0 {
				t.Fatalf("registry:%s block has a table row with no cells: %q", name, line)
			}
			allSep := true
			for _, c := range cells {
				if !sepRE.MatchString(c) {
					allSep = false
					break
				}
			}
			if allSep {
				if haveSep {
					t.Fatalf("registry:%s block has a second separator row: %q", name, line)
				}
				haveSep = true
				continue
			}
			if _, err := strconv.ParseUint(cells[0], 10, 64); err == nil {
				if len(cells) != len(header) {
					t.Fatalf("registry:%s data row has %d cells, want %d: %q", name, len(cells), len(header), line)
				}
				rows = append(rows, cells)
				continue
			}
			// Not a separator, first cell not a base-10 integer: the one
			// legal reading left is the header row.
			if haveHeader {
				t.Fatalf("registry:%s block has a second header row, or a data row whose first cell is not a base-10 integer: %q", name, line)
			}
			if len(cells) != len(header) {
				t.Fatalf("registry:%s header row has %d cells, want %d: %q", name, len(cells), len(header), line)
			}
			for i, want := range header {
				if cells[i] != want {
					t.Fatalf("registry:%s header column %d is %q, want %q", name, i+1, cells[i], want)
				}
			}
			haveHeader = true
		default:
			t.Fatalf("illegal line inside the registry:%s block (legal classes: blank, the grammar comment, one header, one separator, integer-keyed data rows): %q", name, line)
		}
	}
	if !haveHeader {
		t.Fatalf("registry:%s block has no header row", name)
	}
	if !haveSep {
		t.Fatalf("registry:%s block has no separator row", name)
	}
	return rows
}

// splitCells splits a table row on UNESCAPED pipes only: `\|` is a literal
// pipe inside a cell (split first, then unescape). The leading and trailing
// pipes' empty fields are trimmed, then every cell is space-trimmed.
func splitCells(line string) []string {
	var cells []string
	var cur strings.Builder
	escaped := false
	for _, r := range line {
		switch {
		case escaped:
			if r != '|' {
				cur.WriteByte('\\')
			}
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '|':
			cells = append(cells, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	if escaped {
		cur.WriteByte('\\')
	}
	cells = append(cells, cur.String())
	if len(cells) > 0 && strings.TrimSpace(cells[0]) == "" {
		cells = cells[1:]
	}
	if len(cells) > 0 && strings.TrimSpace(cells[len(cells)-1]) == "" {
		cells = cells[:len(cells)-1]
	}
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	return cells
}

// upperSnake converts a stripped camel-case identifier to UPPER_SNAKE:
// MsgTooLarge → MSG_TOO_LARGE. Derived in code, never hand-written — a
// hand-maintained name map would be a second registry (ledger row 3).
func upperSnake(camel string) string {
	var b strings.Builder
	for i, r := range camel {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}
