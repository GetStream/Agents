# internal/connectors/sources/http

The `core.ToolSource` for a connector whose manifest lists its own tools, registered as `http` (`Kind`, `core.SourceKindHTTP`), the name a manifest's `sources[].kind` gives it. Each tool in `sources[].http_tools` (`core.HTTPTool`, validated in `core/http_tools.go`) is one HTTP request to the source's endpoint. Callers, through `core.Registry.ToolSources` (`cmd/router/main.go`): `POST /v1/agents/connections/{id}/validate` (`Discover`, `internal/api/connection_tools.go`) and the session (`Open`, `internal/session/connector_tools.go`), whose dispatcher runs `Toolset.Call` and bounds it itself. No stored connector has kind `http` until the API takes one (AI-3676).

## Flow

```
Discover(ctx, binding)                   sends nothing: the manifest is the list
  source       manifest sources[kind=http], its endpoint -> Endpoints[role]
  ToolSpec     name, description,
               input schema  {type: object, properties: {<parameter>: {type, description}},
                              required: [<required parameters, by name>]}
               SchemaDigest = sha256(name, description, input schema, method, path, read_only)

Open(ctx, binding, grants)
  alias        binding.Name, never holding "__"
  client       a copy of binding.HTTP whose Timeout is at least 35 s (callTimeout)
  keep         granted by name AND digest unchanged
  offer        alias + "__" + name

Toolset.Call(call)
  unknown name             -> error, nothing sent
  arguments vs schema      -> error, nothing sent
  URL          endpoint (one trailing / dropped) + path, joined as text
  {name}       the argument, url.PathEscape'd: one segment
  others       GET, HEAD, DELETE -> query;  POST, PUT, PATCH -> JSON object body
               (an empty object when every parameter is in the path)
               only declared parameters; numbers as the model wrote them
  headers      sources[].headers, then the connection's transport adds the credential
  no answer    not sent, refused by egress, or cut short -> *core.UnreachedError: one sentence
               "http: <tool>: the API could not be reached, or its answer did not arrive whole";
               the cause behind Unwrap and on the log line (core.LoggedMessage)
  answer       read up to 4 MiB, cut back to a whole rune; not UTF-8 -> "not text" sentence
  2xx          text, cut at 32 KiB + marker (core.CutResult); empty -> "The API answered <status>."
  otherwise    *core.ToolError "The API answered <status>[: <body>]", cut the same way
```

## Rules

- **Only through `ResolvedBinding.HTTP`.** The source never builds a client: an opened toolset sends with a copy of the binding's client that differs only in its timeout, so the credential, the 401 renewal (`core/AGENTS.md`, «Transport»), the egress checks and the redirect policy all run. Check: `go test -run 'TestSourceSuite/(TestAGETSends|TestAnAnswerThatIsNotA2xx)' ./internal/connectors/sources/http`.
- **Where a parameter goes is decided (AI-3669, «Decided»).** `{name}` in the path, escaped as one segment; the others in the query of a GET, a HEAD or a DELETE, and in a JSON body of a POST, a PUT or a PATCH. Only the parameters the tool declares are sent, so an argument the model adds goes nowhere. Check: `go test -run 'TestSourceSuite/(TestAGETSends|TestAPOSTSends|TestEachMethod|TestAnArgumentTheTool)' ./internal/connectors/sources/http`.
- **The endpoint and the path are joined as text.** Never `url.ResolveReference`, which drops the endpoint's own path for a path starting with `/`; `core.Manifest.Validate` refuses a path starting with `//` or holding `\`. Check: `go test -run 'TestSourceSuite/TestTheEndpointsOwnPath' ./internal/connectors/sources/http`.
- **A tool is offered only when its grant names it and pins the digest it has now.** The digest covers method, path and read_only beside what the model is shown, so a revision that turns a POST into a read-only GET is a new digest, and a connection pinned to one revision never runs a tool under a grant made for the other (AI-3674, from the AI-3673 review). Check: `go test -run 'TestHTTPSourceContract/(TestOnlyTheGranted|TestAnUngranted|TestAToolWhoseSchema)|TestSourceSuite/(TestAGrantForAnotherRevision|TestTheDigestCovers)' ./internal/connectors/sources/http`.
- **Arguments are checked before anything is sent**, against the schema built from the declared parameters (JSON Schema 2020-12). Check: `go test -run 'TestHTTPSourceContract/TestArguments' ./internal/connectors/sources/http`.
- **An answer that is not a 2xx is a `*core.ToolError`** naming the status, with the API's words when it said any. Check: `go test -run 'TestHTTPSourceContract/TestAnErrorWithNoText|TestSourceSuite/(TestAnAnswerThatIsNotA2xx|TestAToolErrorOverTheCap)' ./internal/connectors/sources/http`.
- **A call that got no whole answer reads one sentence.** The model never reads the request's URL, an argument, or what the egress policy or the resolver said, which would tell a private host from one that does not resolve (AI-3674, S1.1). Check: `go test -run 'TestSourceSuite/(TestACallThatReachesNoAPI|TestABodyCutShort)' ./internal/connectors/sources/http`.
- **Bounded.** One body is read up to 4 MiB (`maxResponseBytes`, the mcp source's cap); one result or tool error is at most `core.MaxResultBytes` (32 KiB), ending with `core.TruncatedMarker`. An opened toolset's client waits at least `callTimeout` (35 s), past the longest `timeout_ms` a binding may ask for (30 s), and `Binding.Timeout` bounds a call when set. Check: `go test -run 'TestHTTPSourceContract/TestAResultOverTheCap|TestSourceSuite/(TestABodyIsRead|TestAnOpenedToolsetWaits|TestTheBindingsTimeout)' ./internal/connectors/sources/http`.
- **Every hardcoded value says where it comes from**, beside it: `Separator`, `defaultCallTimeout`, `maxResponseBytes`, `nonText`, `bodyMethods`.

Not here yet (AI-3675, call-time safety): refusing an undeclared argument before sending (today it is dropped), refusing a `.` or `..` path value, refusing an override name at `Open`, redirect rules beyond egress's, and redacting arguments from log lines. A manifest that sets `sources[].tools` (the `needs_scopes` rules) on an http source is refused (`checkHTTPSource`), as the source reads none: no tool is left out for a scope and validate reports none.

## Tests

From `acceleration/`: `go test -race ./internal/connectors/sources/http`. `TestHTTPSourceContract` runs `core/contracttest.SourceContract`; `SourceSuite` the rest. The provider is a loopback TLS server that takes one bearer token, records each request and answers by path, reached through `core.Transports` with the `bearer` scheme. No mocks (`.claude/skills/go-testing/SKILL.md`).
