package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
)

// loggerMethods are the slog calls whose arguments after the message are key-value pairs.
var loggerMethods = map[string]int{
	"Debug": 1, "Info": 1, "Warn": 1, "Error": 1,
	"DebugContext": 2, "InfoContext": 2, "WarnContext": 2, "ErrorContext": 2,
	"Log": 3, "With": 0,
	// Helpers that pass their trailing key-value pairs on to a logger.
	"refuseArrivingCall": 6, "failedRead": 2, "hangUp": 3, "refuse": 3,
}

// attrConstructors are the log/slog functions whose first argument is an attribute key.
var attrConstructors = map[string]bool{
	"String": true, "Int": true, "Int64": true, "Uint64": true, "Float64": true,
	"Bool": true, "Time": true, "Duration": true, "Any": true, "Group": true,
}

// The router names the Stream app app_id on every line (AI-3909): a line naming it customer
// is one ClickHouse files under the old key only while its fallback lasts.
func (s *LogHandlerSuite) TestNoLineNamesTheAppCustomer() {
	root := filepath.Join("..", "..")
	files := token.NewFileSet()
	var found []string
	err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return err
		}
		parsed, err := parser.ParseFile(files, name, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			keys := func(args []ast.Expr, first int) {
				for i := first; i < len(args); i += 2 {
					if literal, ok := args[i].(*ast.BasicLit); ok && literal.Kind == token.STRING {
						if key, _ := strconv.Unquote(literal.Value); key == "customer" {
							found = append(found, files.Position(literal.Pos()).String())
						}
					}
				}
			}
			switch node := node.(type) {
			case *ast.CompositeLit:
				// The fields a line is built from: []any{"app_id", ...}.
				if array, ok := node.Type.(*ast.ArrayType); ok {
					if element, ok := array.Elt.(*ast.Ident); ok && element.Name == "any" {
						keys(node.Elts, 0)
					}
				}
			case *ast.CallExpr:
				switch fun := node.Fun.(type) {
				case *ast.SelectorExpr:
					if first, ok := loggerMethods[fun.Sel.Name]; ok {
						keys(node.Args, first)
					}
					// slog.String("app_id", v): the key is the first argument.
					if pkg, ok := fun.X.(*ast.Ident); ok && pkg.Name == "slog" && attrConstructors[fun.Sel.Name] && len(node.Args) > 0 {
						keys(node.Args[:1], 0)
					}
				case *ast.Ident:
					if fun.Name == "append" {
						keys(node.Args, 1)
					}
				}
			}
			return true
		})
		return nil
	})
	s.Require().NoError(err)
	s.Empty(found, "a line names the app as customer: name it app_id")
}
