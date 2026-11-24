# internal/connectors/sources/http

The `core.ToolSource` for a connector whose manifest lists its own tools, registered as `http` (`Kind`, `core.SourceKindHTTP`), the name a manifest's `sources[].kind` gives it. Each tool in `sources[].http_tools` (`core.HTTPTool`, validated in `core/http_tools.go`) is one HTTP request to the source's endpoint. Callers, through `core.Registry.ToolSources` (`cmd/router/main.go`): `POST /v1/agents/connections/{id}/validate` (`Discover`, `internal/api/connection_tools.go`) and the session (`Open`, `internal/session/connector_tools.go`), whose dispatcher runs `Toolset.Call` and bounds it itself. No stored connector has kind `http` until the API takes one (AI-3676): an import refuses one too (`importableDefinition`, `internal/store/datamove.go`) until then.

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
  source       core.CheckHTTPSource again: a rule of validation broken -> error, nothing offered
  client       a copy of binding.HTTP whose Timeout is at least 35 s (callTimeout);
               redirects followed only for GET and HEAD, within binding.HTTP's own policy
  keep         granted by name AND digest unchanged
  offer        alias + "__" + name

Toolset.Call(call)
  unknown name             -> error, nothing sent
  an undeclared argument   -> "an argument is not one of the tool's parameters", nothing sent
  arguments vs schema      -> error, nothing sent
  URL          endpoint (one trailing / dropped) + path, joined as text
  {name}       the argument, url.PathEscape'd: one segment; a segment holding one that comes
               out "", "." or ".." -> "a path parameter is empty, . or ..", nothing sent
  others       GET, HEAD, DELETE -> query;  POST, PUT, PATCH -> JSON object body
               (an empty object when every parameter is in the path)
               only declared parameters; numbers as the model wrote them
  headers      sources[].headers, then the connection's transport adds the credential
  no answer    not sent, refused by egress, or cut short -> *core.UnreachedError: one sentence
               "http: <tool>: the API could not be reached, or its answer did not arrive whole";
               the cause behind Unwrap and on the log line (core.LoggedMessage)
  answer       read up to 4 MiB, cut back to a whole rune; not UTF-8 -> "not text" sentence
  write's 301, 302, 303
               "The API answered <code> <text>; the request may have run.", a result
  2xx          text, the credential taken out (core.Exchange.RedactCredential), cut at 32 KiB +
               marker (core.CutResult); empty -> "The API answered <status>."
  otherwise    *core.ToolError "The API answered <status>[: <body>]", redacted with the call's
               core.Exchange (credential, path, query), then cut the same way; a write's 307
               or 308 is one
```

## Rules

- **Only through `ResolvedBinding.HTTP`.** The source never builds a client: an opened toolset sends with a copy of the binding's client that differs only in its timeout, so the credential, the 401 renewal (`core/AGENTS.md`, «Transport»), the egress checks and the redirect policy all run. Check: `go test -run 'TestSourceSuite/(TestAGETSends|TestAnAnswerThatIsNotA2xx)' ./internal/connectors/sources/http`.
- **Where a parameter goes is decided (AI-3669, «Decided»).** `{name}` in the path, escaped as one segment; the others in the query of a GET, a HEAD or a DELETE, and in a JSON body of a POST, a PUT or a PATCH. Check: `go test -run 'TestSourceSuite/(TestAGETSends|TestAPOSTSends|TestEachMethod)' ./internal/connectors/sources/http`.
- **An argument the tool does not declare is refused before sending** (AC-6 of AI-3669), with one sentence, so `_method` or `X-HTTP-Method-Override` from the model never goes anywhere and the model does not take the call for one that went with it. Check: `go test -run 'TestSourceSuite/TestAnArgumentTheTool|TestAcceptanceSuite/TestAnOverride' ./internal/connectors/sources/http`.
- **A path value cannot name another resource** (AI-3674, S1.3). The path is judged as it is built: a segment holding a `{name}` that comes out empty (`DELETE /items/` for an empty id: the collection), `.` or `..` (resolved away by RFC 3986 section 5.2.4, also when `{a}{b}` makes `..` of `.` and `.`) is refused before sending. A value with a `/` stays one segment, `../admin` sent as `..%2Fadmin` (AC-6), and an escaped value keeps its `%`, `..%2F` sent as `..%252F`: an API that decodes `%2F` into a `/` before it routes reads another path, which is the API's own reading. Check: `go test -run 'TestSourceSuite/TestAPathValue' ./internal/connectors/sources/http`.
- **Open checks the source again** (`core.CheckHTTPSource`, the rules `Manifest.Validate` holds a stored one to): a tool parameter or a source header named as a method override, or a read_only tool that is not a GET or a HEAD, fails `Open`, so a manifest stored past validation offers nothing. Check: `go test -run 'TestSourceSuite/TestOpenRefuses' ./internal/connectors/sources/http`.
- **Only a read follows a redirect** (AI-3674, R1.6). A GET or a HEAD follows one within the egress client's policy (the same origin and method, so the credential never reaches another origin); any other method gets the 3xx as its answer: a 307 or a 308 as a tool error, and a 301, a 302 or a 303, after which the write may have run (RFC 9110 sections 15.4.2 to 15.4.4), as the result "The API answered <code> <text>; the request may have run.", so the model does not call it again as if it failed (AI-3675, R1.2). A write's method and path are what the manifest fixes, and net/http sends a POST, PUT or PATCH body again on a 307 or 308, so a followed one would run the write, maybe twice, at a path the manifest does not name. A client with no policy of its own stops at net/http's 10. Check: `go test -run 'TestSourceSuite/(TestOnlyARead|TestARedirectToAnother|TestAClientWithNoRedirect)' ./internal/connectors/sources/http`.
- **What the API answered is redacted before the model reads it** (AI-3674, S1.2). A tool error goes through the call's `core.Exchange.Redact` (`core.Observed`: the dispatcher's Exchange, or one of the call's own), which takes out the credential's strings and the request's path and query, before the 32 KiB cut, so a credential the cut falls in leaves no start of itself. A 2xx result goes through `core.Exchange.RedactCredential`, which takes out the credential's secret values only: its path and query stay, since an answer names its resource by them, and so does api_key's header name (`core.SecretValuer`). Check: `go test -run 'TestSourceSuite/TestA(ToolError(Echoing|IsRedacted)|2xxEchoing)|TestAcceptanceSuite/TestA(403|200)' ./internal/connectors/sources/http`.
- **No argument or credential on a log line.** The session's dispatcher logs `core.LoggedMessage` of the call's error with the Exchange's `Redact`: the URL a client error names is cut to its scheme and host, and a tool error is redacted. Check: `go test -run 'TestAcceptanceSuite/TestALogLine' ./internal/connectors/sources/http`.
- **The endpoint and the path are joined as text.** Never `url.ResolveReference`, which drops the endpoint's own path for a path starting with `/`; `core.Manifest.Validate` refuses a path starting with `//` or holding `\`. Check: `go test -run 'TestSourceSuite/TestTheEndpointsOwnPath' ./internal/connectors/sources/http`.
- **A tool is offered only when its grant names it and pins the digest it has now.** The digest covers method, path and read_only beside what the model is shown, so a revision that turns a POST into a read-only GET is a new digest, and a connection pinned to one revision never runs a tool under a grant made for the other (AI-3674, from the AI-3673 review). Check: `go test -run 'TestHTTPSourceContract/(TestOnlyTheGranted|TestAnUngranted|TestAToolWhoseSchema)|TestSourceSuite/(TestAGrantForAnotherRevision|TestTheDigestCovers)' ./internal/connectors/sources/http`.
- **Arguments are checked before anything is sent**, against the schema built from the declared parameters (JSON Schema 2020-12). Check: `go test -run 'TestHTTPSourceContract/TestArguments' ./internal/connectors/sources/http`.
- **An answer that is not a 2xx is a `*core.ToolError`** naming the status, with the API's words when it said any. Check: `go test -run 'TestHTTPSourceContract/TestAnErrorWithNoText|TestSourceSuite/(TestAnAnswerThatIsNotA2xx|TestAToolErrorOverTheCap)' ./internal/connectors/sources/http`.
- **A call that got no whole answer reads one sentence.** The model never reads the request's URL, an argument, or what the egress policy or the resolver said, which would tell a private host from one that does not resolve (AI-3674, S1.1). Check: `go test -run 'TestSourceSuite/(TestACallThatReachesNoAPI|TestABodyCutShort)' ./internal/connectors/sources/http`.
- **Bounded.** One body is read up to 4 MiB (`maxResponseBytes`, the mcp source's cap); one result or tool error is at most `core.MaxResultBytes` (32 KiB), ending with `core.TruncatedMarker`. An opened toolset's client waits at least `callTimeout` (35 s), past the longest `timeout_ms` a binding may ask for (30 s), and `Binding.Timeout` bounds a call when set. Check: `go test -run 'TestHTTPSourceContract/TestAResultOverTheCap|TestSourceSuite/(TestABodyIsRead|TestAnOpenedToolsetWaits|TestTheBindingsTimeout)' ./internal/connectors/sources/http`.
- **Every hardcoded value says where it comes from**, beside it: `Separator`, `defaultCallTimeout`, `maxResponseBytes`, `nonText`, `bodyMethods`.

A grant reads only at its digest: `internal/api` judges an http tool read_only (`grantSendsOut`, for a config saved beside a sensitive connector and for a connection's tools listing) only when the grant, or the connection's listed tool, pins the latest revision's read-only digest, since a session opens a tool only at its pinned digest, which covers method and read_only (AI-3675, from AI-3674's Q1). A manifest that sets `sources[].tools` (the `needs_scopes` rules) on an http source is refused (`checkHTTPSource`), as the source reads none: no tool is left out for a scope and validate reports none.

## Tests

From `acceleration/`: `go test -race ./internal/connectors/sources/http`. `TestHTTPSourceContract` runs `core/contracttest.SourceContract`; `SourceSuite` the rest. The provider is a loopback TLS server that takes one bearer token, records each request and answers by path, reached through `core.Transports` with the `bearer` scheme. `AcceptanceSuite` is AC-6 and AC-9 of AI-3669 against `fakeprovider`'s HTTP API, with the `api_key` scheme. No mocks (`.claude/skills/go-testing/SKILL.md`).
