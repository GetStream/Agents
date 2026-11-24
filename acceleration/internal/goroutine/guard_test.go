package goroutine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
)

// calls are the packages that start goroutines for a call. They are the agent, its
// connection to the Stream call, the floor control and the harness. They are also the
// speech and model providers, their routers, and the edge of the audio socket.
var calls = []string{
	"internal/agent",
	"internal/audiofloor",
	"internal/harness",
	"internal/llm",
	"internal/llmrouter",
	"internal/socketedge",
	"internal/stt",
	"internal/sttrouter",
	"internal/sts",
	"internal/stsrouter",
	"internal/tts",
	"internal/ttsrouter",
}

// testOnly are the packages in calls that only tests import, each with its reason. Do not
// add packages to this list.
var testOnly = map[string]string{
	"internal/llm/llmtest":  "model responses that tests write",
	"internal/stt/sttsuite": "the suite that the integration test of each speech-to-text provider runs",
}

func (s *GoroutineSuite) TestEveryGoroutineOfACallStartsThroughGo() {
	root := filepath.Join("..", "..")
	files := token.NewFileSet()
	var bare []string
	for _, pkg := range calls {
		err := filepath.WalkDir(filepath.Join(root, pkg), func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				relative, err := filepath.Rel(root, name)
				if err != nil {
					return err
				}
				if _, skipped := testOnly[filepath.ToSlash(relative)]; skipped || entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(files, name, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				if statement, ok := node.(*ast.GoStmt); ok {
					bare = append(bare, files.Position(statement.Pos()).String())
				}
				return true
			})
			return nil
		})
		s.Require().NoError(err)
	}
	slices.Sort(bare)
	s.Empty(bare, "a panic in one of these goroutines stops the router and all its calls. Use goroutine.Go to start them")
}
