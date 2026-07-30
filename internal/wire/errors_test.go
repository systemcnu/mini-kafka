// D-SL4-1 leg 1: the registry audit that cannot go stale — AllCodes() is
// checked against errors.go's own const block (go/parser over this package's
// source), so a new Code constant that skips AllCodes() fails here; a density
// check over AllCodes-derived bounds structurally cannot catch that.
package wire

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// declaredCodes parses errors.go and returns every constant declared with
// explicit type Code, value → name.
func declaredCodes(t *testing.T) map[Code]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "errors.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing errors.go: %v", err)
	}
	out := make(map[Code]string)
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			ident, ok := vs.Type.(*ast.Ident)
			if !ok || ident.Name != "Code" {
				continue
			}
			for i, name := range vs.Names {
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("constant %s is not a plain literal", name.Name)
				}
				v, err := strconv.ParseUint(lit.Value, 10, 16)
				if err != nil {
					t.Fatalf("constant %s value %q: %v", name.Name, lit.Value, err)
				}
				out[Code(v)] = name.Name
			}
		}
	}
	return out
}

func TestAllCodesCoversEveryDeclaredConstant(t *testing.T) {
	declared := declaredCodes(t)
	if len(declared) == 0 {
		t.Fatal("parsed no Code constants from errors.go")
	}
	all := AllCodes()
	if len(all) == 0 {
		t.Fatal("AllCodes() is empty")
	}
	listed := make(map[Code]bool, len(all))
	for _, c := range all {
		if listed[c] {
			t.Errorf("AllCodes() lists code %d twice", c)
		}
		listed[c] = true
	}
	for v, name := range declared {
		if !listed[v] {
			t.Errorf("declared constant %s = %d is missing from AllCodes()", name, v)
		}
	}
	for _, c := range all {
		if _, ok := declared[c]; !ok {
			t.Errorf("AllCodes() lists %d, which no declared constant carries", c)
		}
	}
}
