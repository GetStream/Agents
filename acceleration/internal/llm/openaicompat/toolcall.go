package openaicompat

import (
	"encoding/json"
	"strconv"
	"strings"
	"unicode"
)

// Gemma 4 writes a tool call as `<|tool_call>call:NAME{key:value,...}<tool_call|>`, with strings
// between `<|"|>` marks. vLLM's tool parser turns that into a real call, but not when the
// opening marker goes missing, as it can with an empty thought channel: the rest,
// `call:find_room{nights:3,...}<tool_call|>`, arrives as answer text. A call written as text
// is still a call, so it is made, not spoken.
//
// Without the marker the model does not always keep its own syntax either, and writes
// `call_tool: get_weather{}`, `call tool: open_case{address: "9 Fern Row"}` or
// `call:find_room(nights=3, arrival="18:45")`, so those are calls too.
const (
	toolCallOpen  = "<|tool_call>"
	toolCallClose = "<tool_call|>"
	gemmaQuote    = `<|"|>`
)

// toolCallPrefixes are what a call written as text starts with, ahead of the tool's name.
var toolCallPrefixes = []string{"call:", "call_tool:", "call tool:", "call "}

type toolCallState int

const (
	// toolCallScanningText reads ordinary answer text and looks for `<|tool_call>`, a prefix such as `call:`, or a tool
	// name followed by `{` or `(`.
	toolCallScanningText toolCallState = iota
	// toolCallAfterOpen expects a call immediately after `<|tool_call>`, or emits the orphan marker as text.
	toolCallAfterOpen
	// toolCallInCall reads a call body from its tool name to its matching closing bracket.
	toolCallInCall
	// toolCallClosing checks for an optional `<tool_call|>` immediately after a finished call.
	toolCallClosing
	// toolCallHeld holds calls written without a marker or `call:` prefix. They become calls if the answer ends, a
	// `<tool_call|>` follows or a marked call follows; any other words turn them back into text.
	toolCallHeld
)

// toolCallSplitter moves tool calls written as text out of the answer as it streams. Like the
// channel splitter, it holds back the end of what it has seen while that could still be the
// start of a marker or of a call.
type toolCallSplitter struct {
	// tools are the names offered on this request. A bare `call:` starts a call only when the
	// name after it is one of them, so "call: me back" is still spoken.
	tools   map[string]bool
	state   toolCallState
	pending string
	// body holds the text of the call being read, from its name on.
	body  strings.Builder
	depth int
	// quote is the mark that opened the string being read, `<|"|>`, `"` or `'`, or empty
	// outside one.
	quote string
	// parens is set for a call written `NAME(key=value)`, where a closing parenthesis ends
	// bare values and single quotes may begin anywhere in one.
	parens bool
	// loose is set while reading a call written without a marker or a `call:` prefix, such as `check_outage{}` or `call
	// check_outage{}`. It only becomes a call if the answer ends there or a closing marker or a marked call follows,
	// since it may be a tool name in a sentence.
	loose bool
	// raw is what came before the name of a loose call, such as spaces and `call `. We keep it so we can give the
	// original text back as words if prose follows.
	raw string
	// held are loose calls read that the answer has not yet gone on past, and heldText the
	// text they were written as. Nothing but space, closing markers or more calls after them
	// makes them calls; anything else makes them words, a tool named in a sentence.
	held     []string
	heldText string
}

func newToolCallSplitter(tools []string) *toolCallSplitter {
	names := make(map[string]bool, len(tools))
	for _, tool := range tools {
		names[tool] = true
	}
	return &toolCallSplitter{tools: names}
}

func (c *toolCallSplitter) startCall() {
	c.state = toolCallInCall
	c.depth = 0
	c.quote = ""
	c.parens = false
}

// Write takes the next piece of the answer and returns what of it is answer, and the text of
// each call it finished, from the tool's name to its closing brace.
func (c *toolCallSplitter) Write(delta string) (text string, calls []string) {
	c.pending += delta
	var answer strings.Builder
	for {
		switch c.state {
		case toolCallClosing:
			if strings.HasPrefix(c.pending, toolCallClose) {
				c.pending = c.pending[len(toolCallClose):]
				if len(c.held) > 0 {
					calls = append(calls, c.held...)
					c.held, c.heldText = nil, ""
				}
				c.state = toolCallScanningText
				continue
			}
			if strings.HasPrefix(toolCallClose, c.pending) {
				return answer.String(), calls
			}
			if len(c.held) > 0 {
				c.state = toolCallHeld
			} else {
				c.state = toolCallScanningText
			}
		case toolCallInCall:
			if !c.readCall() {
				return answer.String(), calls
			}
			if c.loose {
				c.held = append(c.held, c.body.String())
				c.heldText += c.raw + c.body.String()
			} else {
				calls = append(calls, c.body.String())
			}
			c.body.Reset()
			c.state = toolCallClosing
		case toolCallAfterOpen:
			match, skip := c.afterOpen()
			switch match {
			case nameMaybe:
				return answer.String(), calls
			case nameFound:
				if len(c.held) > 0 {
					calls = append(calls, c.held...)
					c.held, c.heldText = nil, ""
				}
				c.pending = c.pending[skip:]
				c.loose = false
				c.raw = ""
				c.startCall()
			default:
				answer.WriteString(c.heldText)
				answer.WriteString(toolCallOpen)
				c.held, c.heldText = nil, ""
				c.state = toolCallScanningText
			}
		case toolCallHeld:
			rest := strings.TrimLeftFunc(c.pending, unicode.IsSpace)
			space := c.pending[:len(c.pending)-len(rest)]
			start, skip, partial := c.nextCall()
			switch {
			case strings.HasPrefix(rest, toolCallClose):
				calls = append(calls, c.held...)
				c.held, c.heldText = nil, ""
				c.pending = rest[len(toolCallClose):]
				c.state = toolCallScanningText
			case start == len(space) && rest != "":
				prefix := c.pending[start : start+skip]
				c.pending = c.pending[start+skip:]
				if prefix == toolCallOpen {
					c.heldText += space
					c.state = toolCallAfterOpen
					continue
				}
				if skip == 0 || strings.TrimRight(prefix, " ") == "call" {
					c.loose = true
					c.raw = space + prefix
					c.startCall()
					continue
				}
				calls = append(calls, c.held...)
				c.held, c.heldText = nil, ""
				c.loose = false
				c.raw = prefix
				c.startCall()
			case rest == "" || strings.HasPrefix(toolCallClose, rest) || start < 0 && partial >= len(rest):
				return answer.String(), calls
			default:
				answer.WriteString(c.heldText)
				c.held, c.heldText = nil, ""
				c.state = toolCallScanningText
			}
		case toolCallScanningText:
			start, skip, partial := c.nextCall()
			if start < 0 {
				keep := len(c.pending) - partial
				answer.WriteString(c.pending[:keep])
				c.pending = c.pending[keep:]
				return answer.String(), calls
			}
			answer.WriteString(c.pending[:start])
			prefix := c.pending[start : start+skip]
			c.pending = c.pending[start+skip:]
			if prefix == toolCallOpen {
				c.state = toolCallAfterOpen
				continue
			}
			c.loose = skip == 0 || strings.TrimRight(prefix, " ") == "call"
			c.raw = prefix
			c.startCall()
		}
	}
}

// Flush returns what was held back once the reply has ended: an opening that never became a
// call is text, calls held until the answer ended are calls, and a call that never closed is
// returned unfinished, for the caller to drop.
func (c *toolCallSplitter) Flush() (text string, calls []string, unfinished string) {
	rest := c.pending
	c.pending = ""
	state := c.state
	c.state = toolCallScanningText
	switch state {
	case toolCallAfterOpen:
		text = c.heldText + toolCallOpen + rest
		c.held, c.heldText = nil, ""
		return text, nil, ""
	case toolCallInCall:
		calls = c.held
		c.held, c.heldText = nil, ""
		unfinished = c.body.String() + rest
		c.body.Reset()
		return "", calls, unfinished
	case toolCallClosing:
		calls = c.held
		c.held, c.heldText = nil, ""
		return "", calls, ""
	case toolCallHeld:
		// What is left could only have grown into another call or a closing marker. As
		// words, it is the answer going on past the calls held.
		if after := strings.TrimLeftFunc(rest, unicode.IsSpace); after != "" && !strings.HasPrefix(toolCallClose, after) {
			text = c.heldText + rest
			c.held, c.heldText = nil, ""
			return text, nil, ""
		}
		calls = c.held
		c.held, c.heldText = nil, ""
		return "", calls, ""
	default:
		return rest, nil, ""
	}
}

// afterOpen checks that a call starts right after `<|tool_call>`. That is either a call prefix and a tool name followed
// by `{` or `(`, or an offered tool name followed by `{` or `(`.
func (c *toolCallSplitter) afterOpen() (match nameMatch, skip int) {
	if c.pending == "" {
		return nameMaybe, 0
	}
	for _, prefix := range toolCallPrefixes {
		if strings.HasPrefix(c.pending, prefix) {
			rest := c.pending[len(prefix):]
			name := strings.TrimLeft(rest, " ")
			switch c.callName(name, true) {
			case nameFound:
				return nameFound, len(prefix) + len(rest) - len(name)
			case nameMaybe:
				return nameMaybe, 0
			}
		}
	}
	for _, prefix := range toolCallPrefixes {
		if strings.HasPrefix(prefix, c.pending) {
			return nameMaybe, 0
		}
	}
	for tool := range c.tools {
		if strings.HasPrefix(c.pending, tool) {
			if len(c.pending) == len(tool) {
				return nameMaybe, 0
			}
			if c.pending[len(tool)] == '{' || c.pending[len(tool)] == '(' {
				return nameFound, 0
			}
		} else if strings.HasPrefix(tool, c.pending) {
			return nameMaybe, 0
		}
	}
	return nameNone, 0
}

// nextCall finds where the next call starts in pending: the index, how much to skip there (the
// opening marker, or a prefix and the spaces after it), and otherwise how much of the end to
// hold back because it could still become one.
func (c *toolCallSplitter) nextCall() (start, skip, partial int) {
	start, skip, partial = c.nextMarkedCall()
	if bare := c.nextBareCall(); bare >= 0 && (start < 0 || bare < start) {
		return bare, 0, 0
	}
	if start < 0 {
		partial = max(partial, c.bareSuffix())
	}
	return start, skip, partial
}

// nextBareCall finds an offered tool's name written straight into the answer with its
// arguments, `get_weather{}` or `send_note(to="ana")`, with no marker or prefix ahead of it.
// A model that writes its calls as text sometimes drops the marker, and the call would
// otherwise be read out instead of made. The name has to start a word and be followed at
// once by its brace or parenthesis. Such a call is only made when nothing but space follows
// it (held): a tool named in a sentence, "you can use send_note(to=...) for that", is words.
func (c *toolCallSplitter) nextBareCall() int {
	at := -1
	for tool := range c.tools {
		for from := 0; ; {
			i := strings.Index(c.pending[from:], tool)
			if i < 0 {
				break
			}
			i += from
			end := i + len(tool)
			if (i == 0 || !isNameByte(c.pending[i-1])) && end < len(c.pending) &&
				(c.pending[end] == '{' || c.pending[end] == '(') {
				if at < 0 || i < at {
					at = i
				}
				break
			}
			from = i + 1
		}
	}
	return at
}

// bareSuffix is how much of the end of pending to hold back because it could still become a
// bare call: the last word, while it is the start of an offered name or the whole name with
// its bracket still to come.
func (c *toolCallSplitter) bareSuffix() int {
	word := len(c.pending)
	for word > 0 && isNameByte(c.pending[word-1]) {
		word--
	}
	tail := c.pending[word:]
	if tail == "" {
		return 0
	}
	for tool := range c.tools {
		if strings.HasPrefix(tool, tail) {
			return len(tail)
		}
	}
	return 0
}

func isNameByte(b byte) bool {
	return b == '_' || b == '-' || b == '.' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

// nextMarkedCall is nextCall for calls that start with the opening marker or a prefix.
func (c *toolCallSplitter) nextMarkedCall() (start, skip, partial int) {
	open := strings.Index(c.pending, toolCallOpen)
	for from := 0; ; {
		at, prefix := nextPrefix(c.pending, from)
		if at < 0 {
			break
		}
		if open >= 0 && open < at {
			break
		}
		rest := c.pending[at+prefix:]
		name := strings.TrimLeft(rest, " ")
		switch c.callName(name, false) {
		case nameFound:
			return at, prefix + len(rest) - len(name), 0
		case nameMaybe:
			return -1, 0, len(c.pending) - at
		}
		from = at + prefix
	}
	if open >= 0 {
		return open, len(toolCallOpen), 0
	}
	partial = partialSuffix(c.pending, toolCallOpen)
	for _, prefix := range toolCallPrefixes {
		partial = max(partial, partialSuffix(c.pending, prefix))
	}
	return -1, 0, partial
}

// nextPrefix finds the first call prefix in s at or after from, and its length.
func nextPrefix(s string, from int) (at, length int) {
	at = -1
	for _, prefix := range toolCallPrefixes {
		i := strings.Index(s[from:], prefix)
		if i >= 0 && (at < 0 || from+i < at) {
			at, length = from+i, len(prefix)
		}
	}
	return at, length
}

type nameMatch int

const (
	nameNone nameMatch = iota
	nameMaybe
	nameFound
)

// callName says whether rest starts with a callable name and its opening brace or
// parenthesis, could still grow into one, or cannot.
func (c *toolCallSplitter) callName(rest string, opened bool) nameMatch {
	brace := strings.IndexAny(rest, "{(")
	if brace < 0 {
		if !isName(rest) {
			return nameNone
		}
		if opened {
			return nameMaybe
		}
		for tool := range c.tools {
			if strings.HasPrefix(tool, rest) {
				return nameMaybe
			}
		}
		return nameNone
	}
	name := rest[:brace]
	if name != "" && isName(name) && (opened || c.tools[name]) {
		return nameFound
	}
	return nameNone
}

// readCall moves the call's text from pending into body up to its closing brace, and says
// whether it got there. A string mark or an escape split over two deltas is held back.
func (c *toolCallSplitter) readCall() bool {
	i := 0
	for i < len(c.pending) {
		rest := c.pending[i:]
		if c.quote == "" || c.quote == gemmaQuote {
			if strings.HasPrefix(rest, gemmaQuote) {
				c.quote = toggle(c.quote, gemmaQuote)
				c.body.WriteString(gemmaQuote)
				i += len(gemmaQuote)
				continue
			}
			if strings.HasPrefix(gemmaQuote, rest) {
				break
			}
		}
		ch := c.pending[i]
		if (c.quote == `"` || c.quote == "'") && ch == '\\' {
			if i+1 == len(c.pending) {
				break
			}
			c.body.WriteString(c.pending[i : i+2])
			i += 2
			continue
		}
		mark := ch == '"' || ch == '\'' &&
			(c.parens || c.quote == "'" || singleQuoteBoundary(c.body.String()))
		c.body.WriteByte(ch)
		i++
		if mark && (c.quote == "" || c.quote == string(ch)) {
			c.quote = toggle(c.quote, string(ch))
			continue
		}
		if c.quote != "" {
			continue
		}
		switch ch {
		case '{', '[', '(':
			c.parens = c.parens || c.depth == 0 && ch == '('
			c.depth++
		case '}', ']', ')':
			c.depth--
			if c.depth == 0 {
				c.pending = c.pending[i:]
				return true
			}
		}
	}
	c.pending = c.pending[i:]
	return false
}

func singleQuoteBoundary(body string) bool {
	body = strings.TrimRight(body, " \t\r\n")
	if body == "" {
		return true
	}
	switch body[len(body)-1] {
	case '{', '[', '(', ',', ':', '=':
		return true
	default:
		return false
	}
}

// toggle is the quote after mark: mark opens a string outside one and closes it inside.
func toggle(quote, mark string) string {
	if quote == "" {
		return mark
	}
	return ""
}

func isName(s string) bool {
	for _, r := range s {
		if r != '_' && r != '-' && r != '.' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// parseGemmaCall reads `NAME{key:value,...}`, or `NAME(key=value,...)`, into the tool's name
// and its arguments as JSON. It says false for text it cannot read whole.
func parseGemmaCall(text string) (name, arguments string, ok bool) {
	brace := strings.IndexAny(text, "{(")
	if brace <= 0 || !isName(text[:brace]) {
		return "", "", false
	}
	p := gemmaParser{text: text[brace:]}
	var value any
	if text[brace] == '(' {
		p.pos++
		p.parens = true
		value, ok = p.fields(')', '=')
	} else {
		value, ok = p.value()
	}
	if !ok {
		return "", "", false
	}
	p.space()
	if p.pos != len(p.text) {
		return "", "", false
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", "", false
	}
	return text[:brace], string(raw), true
}

// gemmaParser reads the argument syntax Gemma 4 writes: bare or quoted keys, strings between
// `<|"|>` marks, single quotes or double quotes, numbers, true, false and null, and nested
// objects and lists.
type gemmaParser struct {
	text string
	pos  int
	// parens is set for `NAME(key=value)` arguments, where a parenthesis ends a bare value.
	parens bool
}

func (p *gemmaParser) space() {
	for p.pos < len(p.text) && unicode.IsSpace(rune(p.text[p.pos])) {
		p.pos++
	}
}

func (p *gemmaParser) value() (any, bool) {
	p.space()
	if p.pos >= len(p.text) {
		return nil, false
	}
	switch {
	case strings.HasPrefix(p.text[p.pos:], gemmaQuote):
		return p.quoted()
	case p.text[p.pos] == '"':
		return p.jsonString()
	case p.text[p.pos] == '\'':
		return p.singleQuoted()
	case p.text[p.pos] == '{':
		p.pos++
		return p.fields('}', ':')
	case p.text[p.pos] == '[':
		return p.list()
	}
	ends := ",}]"
	if p.parens {
		ends += ")"
	}
	start := p.pos
	for p.pos < len(p.text) && !strings.ContainsRune(ends, rune(p.text[p.pos])) {
		p.pos++
	}
	token := strings.TrimSpace(p.text[start:p.pos])
	switch token {
	case "":
		return nil, false
	case "true":
		return true, true
	case "false":
		return false, true
	case "null":
		return nil, true
	}
	if _, err := strconv.ParseFloat(token, 64); err == nil {
		return json.Number(token), true
	}
	return token, true
}

func (p *gemmaParser) quoted() (string, bool) {
	p.pos += len(gemmaQuote)
	end := strings.Index(p.text[p.pos:], gemmaQuote)
	if end < 0 {
		return "", false
	}
	value := p.text[p.pos : p.pos+end]
	p.pos += end + len(gemmaQuote)
	return value, true
}

// jsonString reads a string between double quotes, with JSON's escapes.
func (p *gemmaParser) jsonString() (string, bool) {
	end := p.pos + 1
	for end < len(p.text) && p.text[end] != '"' {
		if p.text[end] == '\\' {
			end++
		}
		end++
	}
	if end >= len(p.text) {
		return "", false
	}
	var value string
	if err := json.Unmarshal([]byte(p.text[p.pos:end+1]), &value); err != nil {
		return "", false
	}
	p.pos = end + 1
	return value, true
}

// singleQuoted reads a string between single quotes, as Python writes one. Only an escaped
// quote or backslash loses its backslash.
func (p *gemmaParser) singleQuoted() (string, bool) {
	var value strings.Builder
	for end := p.pos + 1; end < len(p.text); end++ {
		switch p.text[end] {
		case '\\':
			end++
			if end == len(p.text) {
				return "", false
			}
			if p.text[end] != '\'' && p.text[end] != '\\' {
				value.WriteByte('\\')
			}
		case '\'':
			p.pos = end + 1
			return value.String(), true
		}
		value.WriteByte(p.text[end])
	}
	return "", false
}

// fields reads keys and values up to closing, each key followed by assign, once the opening
// bracket has been read.
func (p *gemmaParser) fields(closing, assign byte) (any, bool) {
	out := map[string]any{}
	for {
		p.space()
		if p.pos < len(p.text) && p.text[p.pos] == closing {
			p.pos++
			return out, true
		}
		key, ok := p.key(assign)
		if !ok {
			return nil, false
		}
		value, ok := p.value()
		if !ok {
			return nil, false
		}
		out[key] = value
		if !p.separator(closing) {
			return nil, false
		}
	}
}

func (p *gemmaParser) list() (any, bool) {
	p.pos++
	out := []any{}
	for {
		p.space()
		if p.pos < len(p.text) && p.text[p.pos] == ']' {
			p.pos++
			return out, true
		}
		value, ok := p.value()
		if !ok {
			return nil, false
		}
		out = append(out, value)
		if !p.separator(']') {
			return nil, false
		}
	}
}

// key reads an object key and the assign mark after it.
func (p *gemmaParser) key(assign byte) (string, bool) {
	var quoted func() (string, bool)
	switch {
	case strings.HasPrefix(p.text[p.pos:], gemmaQuote):
		quoted = p.quoted
	case strings.HasPrefix(p.text[p.pos:], `"`):
		quoted = p.jsonString
	case strings.HasPrefix(p.text[p.pos:], `'`):
		quoted = p.singleQuoted
	}
	if quoted != nil {
		value, ok := quoted()
		p.space()
		if !ok || p.pos >= len(p.text) || p.text[p.pos] != assign {
			return "", false
		}
		p.pos++
		return value, true
	}
	end := strings.IndexByte(p.text[p.pos:], assign)
	if end < 0 {
		return "", false
	}
	key := strings.TrimSpace(p.text[p.pos : p.pos+end])
	if key == "" || !isName(key) {
		return "", false
	}
	p.pos += end + 1
	return key, true
}

// separator reads the comma after a value, or the closing bracket, leaving the bracket for the
// caller's loop to take.
func (p *gemmaParser) separator(closing byte) bool {
	p.space()
	if p.pos >= len(p.text) {
		return false
	}
	switch p.text[p.pos] {
	case ',':
		p.pos++
		return true
	case closing:
		return true
	}
	return false
}
