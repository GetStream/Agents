package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// maxResponseBytes is the most of one response body a call reads: the mcp source's cap
// (mcp.maxResponseBytes, 4 MiB), which AI-3669 reuses («MCP's size caps are reused»), so an API
// that answers without end cannot exhaust the router's memory. The model reads at most
// core.MaxResultBytes of it. A call reads one byte past it, to tell a body the cap cut from one
// that ends at the cap.
const maxResponseBytes = 4 << 20

// nonText is what the model reads for a body that is not UTF-8 text, such as an image: the mcp
// source's wording for content that is not text (mcp.nonText).
const nonText = "The tool returned content that is not text, which the agent cannot read"

// placeholder is a {name} in a tool's path, as core.Manifest.Validate reads one (placeholder in
// core/manifest.go).
var placeholder = regexp.MustCompile(`\{([^{}]*)\}`)

// bodyMethods are the methods whose parameters go in a JSON body (AI-3669, «Decided»); those of
// GET, HEAD and DELETE go in the query.
var bodyMethods = []string{http.MethodPost, http.MethodPut, http.MethodPatch}

// The sentences the model reads of a call refused before anything was sent, each with the
// tool's name, so it can call again with what the tool takes.
const (
	// undeclaredArgument is an argument the tool does not declare, such as an _method or an
	// X-HTTP-Method-Override the model adds (AC-6 of AI-3669): it is refused, not dropped, so
	// the model does not take a call that went without it for one that went with it.
	undeclaredArgument = "http: %s: an argument is not one of the tool's parameters, so nothing was sent"
	// otherResource is a path value that would make the path name another resource (builtPath).
	otherResource = "http: %s: a path parameter is empty, . or .., which names another resource than the tool's, " +
		"so nothing was sent"
)

// Toolset is the granted tools of one http source, opened for one session. It is safe for
// concurrent use: its tools are fixed when Open returns, and each call is its own request.
type Toolset struct {
	client *http.Client
	// endpoint is the source's base URL, which each tool's path is appended to.
	endpoint string
	// headers are the manifest's sources[].headers, set on every request.
	headers map[string]string
	// timeout bounds one call; zero leaves it to the caller's context.
	timeout time.Duration
	tools   []llm.Tool
	// targets are the offered tools by the name the model calls them.
	targets map[string]target
}

var _ core.Toolset = (*Toolset)(nil)

// target is one offered tool: its definition and its input schema.
type target struct {
	tool      core.HTTPTool
	validator *jsonschema.Schema
}

// Tools are the tools the model is offered.
func (t *Toolset) Tools() []llm.Tool {
	return t.tools
}

// Instructions is empty: an API says nothing about its tools when a session opens.
func (*Toolset) Instructions() string {
	return ""
}

// Call runs one offered tool: one request, whose answer the model reads as text. A name this
// toolset did not offer, or arguments its input schema refuses, never reach the API. An answer
// that is not a 2xx is a *core.ToolError naming its status, and an empty 2xx is its status. A
// result past core.MaxResultBytes is cut and ends with core.TruncatedMarker.
func (t *Toolset) Call(ctx context.Context, call llm.ToolCall) (core.Result, error) {
	offered, found := t.targets[call.Name]
	if !found {
		return core.Result{}, stack.Wrap(fmt.Errorf("http: %q is not a tool this connection offers", call.Name))
	}
	raw := strings.TrimSpace(call.Arguments)
	if raw == "" {
		raw = "{}"
	}
	// Numbers stay json.Number, so one goes into a query or a body as the model wrote it.
	arguments, err := jsonschema.UnmarshalJSON(strings.NewReader(raw))
	if err != nil {
		return core.Result{}, stack.Wrap(fmt.Errorf("http: %s: the arguments are not JSON: %w", call.Name, err))
	}
	if fields, ok := arguments.(map[string]any); ok {
		for name := range fields {
			if _, declared := offered.tool.Parameters[name]; !declared {
				return core.Result{}, stack.Wrap(fmt.Errorf(undeclaredArgument, call.Name))
			}
		}
	}
	if err := offered.validator.Validate(arguments); err != nil {
		return core.Result{}, stack.Wrap(fmt.Errorf("http: %s: the arguments do not match the tool's input schema", call.Name))
	}
	// The call's Exchange, the dispatcher's, or one of its own for any other caller: what the
	// API answered is redacted with it before the model reads it.
	ctx, exchange := core.Observed(ctx)
	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}
	// The schema takes only an object, so arguments is one.
	request, err := t.request(ctx, offered.tool, arguments.(map[string]any))
	if errors.Is(err, errOtherResource) {
		return core.Result{}, stack.Wrap(fmt.Errorf(otherResource, call.Name))
	}
	if err != nil {
		return core.Result{}, err
	}
	response, err := t.client.Do(request)
	if err != nil {
		return core.Result{}, stack.Wrap(unreached(call.Name, err))
	}
	defer func() { _ = response.Body.Close() }()
	read, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return core.Result{}, stack.Wrap(unreached(call.Name, fmt.Errorf("read the answer: %w", err)))
	}
	// A body past the cap is cut at it, back to the start of the rune the cut falls in, so text
	// cut inside a rune is still text.
	body := core.CutAtRune(string(read), maxResponseBytes)
	text := nonText
	if utf8.ValidString(body) {
		text = body
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message := fmt.Sprintf("The API answered %s", response.Status)
		if len(body) > 0 {
			message += ": " + text
		}
		// An API may echo the request's credential or path in its error (AI-3674, S1.2): they
		// are taken out before the cut, which could leave the start of one.
		return core.Result{}, stack.Wrap(&core.ToolError{Message: core.CutResult(exchange.Redact(message))})
	}
	// An empty answer, such as a 204's or any HEAD's, is its status, so the model can tell a
	// call that was done from one that said nothing.
	if len(body) == 0 {
		return core.Result{Parts: llm.TextParts(fmt.Sprintf("The API answered %s.", response.Status))}, nil
	}
	return core.Result{Parts: llm.TextParts(core.CutResult(text))}, nil
}

// errOtherResource is a path value that would make the path name another resource (builtPath).
var errOtherResource = errors.New("a path value names another resource")

// unreached is a call to name that got no whole answer, for cause: one sentence for every such
// failure, so the model reads no URL, no argument and nothing of the egress policy
// (core.UnreachedError).
func unreached(name string, cause error) error {
	return &core.UnreachedError{Message: fmt.Sprintf("http: %s: the API could not be reached, or its answer did not arrive whole", name), Err: cause}
}

// Close does nothing: the connection's client and its pool are core.Transports'.
func (*Toolset) Close() {}

// request is the tool's request with arguments (AI-3669, «Decided»): each {name} in the path is
// its argument, escaped as one segment (builtPath); the other parameters go in the query of a GET, a HEAD or
// a DELETE, and in a JSON object, the body, of a POST, a PUT or a PATCH. Only the parameters the
// tool declares are sent. The URL is the endpoint and the path joined as text, never resolved as
// a reference, which would drop the endpoint's own path.
func (t *Toolset) request(ctx context.Context, tool core.HTTPTool, arguments map[string]any) (*http.Request, error) {
	path, inPath, err := builtPath(tool.Path, arguments)
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	fields := map[string]any{}
	for _, name := range slices.Sorted(maps.Keys(tool.Parameters)) {
		value, given := arguments[name]
		switch {
		case !given || inPath[name]:
		case slices.Contains(bodyMethods, tool.Method):
			fields[name] = value
		default:
			query.Set(name, fmt.Sprint(value))
		}
	}
	target := strings.TrimSuffix(t.endpoint, "/") + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var body io.Reader
	if slices.Contains(bodyMethods, tool.Method) {
		encoded, err := json.Marshal(fields)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("http: %s: encode the body: %w", tool.Name, err))
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, tool.Method, target, body)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("http: %s: build the request: %w", tool.Name, err))
	}
	for name, value := range t.headers {
		request.Header.Set(name, value)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return request, nil
}

// builtPath is template with each {name} its argument, escaped as one segment, and the names
// it took, or errOtherResource when a segment that holds a {name} comes out empty, . or ..: an
// empty one names the collection (DELETE /items/ for an empty id, AI-3674 S1.3), and a dot
// segment its parent, which a client or a server resolves away (RFC 3986 section 5.2.4), so the
// request would reach a resource the tool's path does not name. Each segment is judged as it is
// built, so {a}{b} with . and . is refused as the .. it makes. A value holding a / stays one
// segment, the / sent as %2F (AC-6 of AI-3669: ../admin is ..%2Fadmin), and an escaped one stays
// as the model wrote it, its % sent as %25 (..%2F is ..%252F): neither is a dot segment.
func builtPath(template string, arguments map[string]any) (string, map[string]bool, error) {
	inPath := map[string]bool{}
	segments := strings.Split(template, "/")
	for i, segment := range segments {
		if !placeholder.MatchString(segment) {
			continue
		}
		segments[i] = placeholder.ReplaceAllStringFunc(segment, func(match string) string {
			name := match[1 : len(match)-1]
			inPath[name] = true
			return url.PathEscape(fmt.Sprint(arguments[name]))
		})
		if segments[i] == "" || segments[i] == "." || segments[i] == ".." {
			return "", nil, errOtherResource
		}
	}
	return strings.Join(segments, "/"), inPath, nil
}

// offered is a tool as the model is shown it.
func offered(name, description string, schema map[string]any) llm.Tool {
	return llm.Tool{Name: name, Description: description, Parameters: schema}
}
