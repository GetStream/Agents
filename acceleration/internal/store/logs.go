package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/getsentry/sentry-go"
)

// bearerCredential and credentialText take their unquoted value only when it is not a word
// (see redactUnlessWord): the prose of our own errors and a WWW-Authenticate challenge put a
// word or an auth-param name there, as in "a bearer token", "open secret: cipher: ..." or
// `Bearer error="insufficient_scope"` (RFC 6750 section 3).
var bearerCredential = regexp.MustCompile(`(?i)\bbearer\s+([^\s"\x27,;}]+)`)
var credentialText = regexp.MustCompile(`(?i)(?:api[_-]?key|access[_-]?token|refresh[_-]?token|(?:\b|_)id[_-]?token|auth[_-]?token|private[_-]?key|password|secret|authorization|cookie)["\x27]?\s*[=:]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|([^\s,;}]+))`)

// word is a value that reads as prose rather than a credential: letters, lower case after the
// first, with underscores, as an auth-param name has, or up to 8 capitals, as an error code
// (EOF, ERROR) has, and the punctuation that ends a clause or an auth-param name after it. A key
// or token has a digit, a mix of cases, another symbol or more length in it.
var word = regexp.MustCompile(`^(?:[A-Za-z][a-z_]*|[A-Z]{1,8})[=.:!?)]*$`)
var authorizationHeader = regexp.MustCompile(`(?im)\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*:\s*[^\r\n]+`)
var urlCredential = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@`)
var providerCredential = regexp.MustCompile(`\bsk-[a-zA-Z0-9_-]{8,}`)

// jsonToken is a "token" key of a JSON object (or a quoted key of a Python-style dict) with a
// quoted value, as a provider's answer echoed in an error has it: {"token":"..."}. The key is
// matched whole, so max_tokens, tokens, "token_count" and a number or null value stay; the
// _token names credentialText takes (access_token, id_token...) are not matched here. The key
// stays and the value becomes "[redacted]", so the JSON a log or a stored text holds stays valid.
var jsonToken = regexp.MustCompile(`(?i)(["\x27]token["\x27]\s*:\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')`)

// shapedCredential is a rule and the literals one of which a text holds if the rule can match
// it. The rule is run only then, so a line that passes mayHoldCredential for its "key" or "token" is
// not scanned once for every shape it does not hold.
type shapedCredential struct {
	rule     *regexp.Regexp
	literals []string
}

func newShaped(pattern string, literals ...string) shapedCredential {
	return shapedCredential{rule: regexp.MustCompile(pattern), literals: literals}
}

// shapeStart is the first group of a prefixed rule, see shapedCredentials.
const shapeStart = `(^|[^A-Za-z0-9_]|%[0-9A-Fa-f]{2}|\\[a-z]|\\u[0-9A-Fa-f]{4})`

// shapedCredentials are credentials with a fixed shape, which are taken out wherever they stand,
// with no keyword before them. A rule names one shape and is case sensitive, as the prefix
// of each is. A prefix starts after the start of the text, a character that is not a letter,
// digit or underscore, or an escape (%3D, \n, \u0026), which a \b would not see as a boundary
// when a letter follows it; the boundary is the rule's first group and is kept. The JWT and PEM
// rules have no boundary and no group. The lengths are floors below the documented ones, so a
// prefix with a short tail (a prose word, a test value) stays.
var shapedCredentials = []shapedCredential{
	// Slack tokens: xox, a letter for the type (b bot, p user, r refresh, e rotating refresh, and others)
	// and a dash (https://api.slack.com/authentication/token-types).
	newShaped(shapeStart+`xox[abposre]-[A-Za-z0-9-]{10,}`, "xox"),
	// Slack app-level token: xapp, a version digit and the rest.
	newShaped(shapeStart+`xapp-[A-Za-z0-9-]{10,}`, "xapp-"),
	// GitHub personal, OAuth, user-to-server, server-to-server and refresh tokens: gh plus a type
	// letter and an underscore, 36 characters after it
	// (https://github.blog/engineering/platform-security/behind-githubs-new-authentication-token-formats/).
	newShaped(shapeStart+`gh[pousr]_[A-Za-z0-9]{20,}`, "ghp_", "gho_", "ghu_", "ghs_", "ghr_"),
	// GitHub fine-grained personal access token.
	newShaped(shapeStart+`github_pat_[A-Za-z0-9_]{20,}`, "github_pat_"),
	// Google API key: AIza and 35 characters.
	newShaped(shapeStart+`AIza[0-9A-Za-z_-]{30,}`, "AIza"),
	// AWS access key id: AKIA and 16 characters
	// (https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html).
	newShaped(shapeStart+`AKIA[0-9A-Z]{16}`, "AKIA"),
	// AWS temporary access key id from STS: ASIA and 16 characters, as AKIA has.
	newShaped(shapeStart+`ASIA[0-9A-Z]{16}`, "ASIA"),
	// A JSON Web Token with no keyword before it: three base64url parts joined by dots, the first
	// the encoded header, which starts with eyJ as {" does (RFC 7515 section 7.1, RFC 7519), and
	// the payload, a JSON object, which starts with eyJ too, so a dotted name such as keyJar.example.com
	// stays. The signature of an unsecured token is empty.
	newShaped(`eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*`, "eyJ"),
	// A PEM private key block (RFC 7468 section 13): the BEGIN line, the Proc-Type and DEK-Info
	// headers of a legacy encrypted one (RFC 1421 section 4.6), the base64 body and escaped
	// newlines after it, and the END line when there is one. The body is read as the characters
	// it holds, not up to the END line, which a line cut short does not have and a lazy search for
	// it would read to the end of the text for.
	newShaped(`-----BEGIN [A-Z ]*PRIVATE KEY-----(?:(?:\s|\\[nr])*(?:Proc-Type|DEK-Info):[ \t]*[A-Za-z0-9,._-]+)*[A-Za-z0-9+/=\s\\]*(?:-----END [A-Z ]*PRIVATE KEY-----)?`, "-----BEGIN"),
}

// queryCredential is a key sent as a URL query parameter, which a Go http or websocket error
// prints with the URL. The names are the ones we send a key in: key for Gemini's Live API
// (internal/stt/gemini, internal/sts/gemini) and token for ipinfo (internal/locate);
// credentialText already takes api_key, access_token and client_secret. A parameter is
// matched whole after its ? or &, raw or percent-encoded (%3F, %26, %3D, RFC 3986 section
// 2.1), once or twice, or written as an HTML (&amp;) or JSON (\u0026) escape, or at the start
// of a quoted string, as a query is in a provider's JSON echo ("query":"key=..."), so keyterm,
// tokens, max_tokens or "the key=value pair" stay. The value runs to the next raw &, a backslash (the start of a JSON
// escape) or a quote, so an encoded one also takes the encoded parameters after it.
var queryCredential = regexp.MustCompile(`(?i)((?:[?&"]|&amp;|\\u0026|%(?:25)*3F|%(?:25)*26)(?:key|token)(?:=|\\u003d|%(?:25)*3D))[^&\s"\x27#\\]+`)

// encodedCredential is a credential name and an encoded equals sign (%3D, or %253D twice
// encoded) with the value after it, where no ? or & stands before the name: the text of an
// encoded query cut at the parameter ("key%3D<hex>"), or a parameter after another separator
// such as %22. The names are the ones credentialText takes, short: key, token, password and
// secret end api_key, access_token, client_secret and the like, and apikey is the one name no
// separator splits. The name starts after the start of the text or a character that is not a
// letter or digit (an underscore is one), so monkey%3D1 stays; the boundary is the first group
// and is kept (see shapeStart). The value is read as queryCredential reads it, whatever its
// length: a parameter named key is a key. A name with a plain = is the rule of credentialText
// and queryCredential.
var encodedCredential = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9]|%[0-9A-Fa-f]{2}|\\[a-z]|\\u[0-9A-Fa-f]{4})((?:apikey|key|token|password|secret)%(?:25)*3D)[^&\s"\x27#\\]+`)

// vendorKeyMessage is a provider's refusal that names the key it was sent, with no = or : to
// say so: "Invalid API key <32 hex>", "Incorrect API key provided: <key>", invalid_api_key, or
// the same for an API token. The key is the 16 characters or more of the key alphabet after the
// phrase, so a request id of 8 hex digits after "Invalid API key." stays. The phrase stays and
// the key is taken out, unless it is a word (see redactVendorKeyMessage).
var vendorKeyMessage = regexp.MustCompile(`(?i)\b((?:invalid|incorrect)[\s_-]+(?:x-)?api[\s_-]?(?:key|token)\b(?:\s+provided)?\s*[:=]?\s*)([A-Za-z0-9_-]{16,})`)

// encodedAuthorization is an authorization or cookie header (proxy-authorization and
// set-cookie end in those names) written into a URL: the name, an optional encoded quote
// (%22, %27) and an encoded colon or equals sign (%3A, %3D), then the value to the end of its
// query parameter. It takes no word boundary, which the hex digits of an encoded character
// before the name (%22, %3F) would hide.
var encodedAuthorization = regexp.MustCompile(`(?i)(?:authorization|cookie)(?:%(?:25)*2[27])?%(?:25)*3[AD][^\s"\x27&#\\]+`)

// encodedBearer is a bearer token whose space is encoded as %20, or as + the way a form
// encodes it (url.QueryEscape), with no word boundary for the same reason.
var encodedBearer = regexp.MustCompile(`(?i)bearer(?:%(?:25)*20|\+)+[^\s"\x27&#\\]+`)

// SafeLogText is RedactLogText cut to what the requests table keeps: past 8192 bytes, the first
// 2048 runes and a marker.
func SafeLogText(text string) string {
	text = RedactLogText(text)
	if len(text) > 8192 {
		text = string([]rune(text)[:min(len([]rune(text)), 2048)]) + "… [truncated]"
	}
	return text
}

// RedactLogText is text with every credential shape taken out, at whatever length. A caller
// that must send the whole text, such as a refusal naming what the caller sent, or a log line,
// which has no cap (RedactLogAttr), uses this; everything stored uses SafeLogText.
func RedactLogText(text string) string {
	if !mayHoldCredential(text) {
		return text
	}
	return redactCredentials(text)
}

// legacyWords are the words every rule in redactCredentials but the shaped ones needs, in any
// case: bearer (bearerCredential, encodedBearer), authorization and cookie
// (authorizationHeader, encodedAuthorization), key and token (queryCredential, jsonToken, and
// api_key, access_token or refresh_token in credentialText), password and secret
// (credentialText), the @ of urlCredential and the sk- of providerCredential. A PEM block has
// "key" in its BEGIN line. The rules for those are case sensitive; the check is not, which only
// lets more text through to them.
var legacyWords = []string{"bearer", "authorization", "cookie", "key", "token", "password", "secret", "@", "sk-"}

// shapeWords are the start of each shape in shapedCredentials. A text with only one of these
// (a base64 body with eyJ in it) runs the shaped rules and not the ones above, which scan the
// whole text.
var shapeWords = []string{"xox", "xapp-", "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_", "aiza", "akia", "asia", "eyj"}

// mayHoldCredential is false for a text no rule can match, which is most of what is logged,
// so it skips the regular expressions.
func mayHoldCredential(text string) bool {
	return holdsLegacyWord(text) || holdsWord(text, shapeWords)
}

// holdsLegacyWord is true for a text one of the rules other than the shaped ones can match. The
// rules fold case as Unicode does, where K is also the Kelvin sign and s the long s, so a text
// with either is checked in full.
func holdsLegacyWord(text string) bool {
	return strings.ContainsRune(text, '\u212a') || strings.ContainsRune(text, '\u017f') || holdsWord(text, legacyWords)
}

func holdsWord(text string, words []string) bool {
	for i := 0; i < len(text); i++ {
		c := text[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		for _, w := range words {
			if c == w[0] && len(text)-i >= len(w) && strings.EqualFold(text[i:i+len(w)], w) {
				return true
			}
		}
	}
	return false
}

// redactCredentials is RedactLogText with no check first.
func redactCredentials(text string) string {
	if holdsLegacyWord(text) {
		text = redactLegacyCredentials(text)
	}
	for _, shaped := range shapedCredentials {
		if shaped.mayMatch(text) {
			text = shaped.rule.ReplaceAllString(text, "${1}[redacted]")
		}
	}
	return text
}

func redactLegacyCredentials(text string) string {
	text = redactUnlessWord(bearerCredential, text)
	text = authorizationHeader.ReplaceAllString(text, "[redacted]")
	text = redactUnlessWord(credentialText, text)
	text = urlCredential.ReplaceAllString(text, "${1}[redacted]@")
	text = queryCredential.ReplaceAllString(text, "${1}[redacted]")
	text = encodedCredential.ReplaceAllString(text, "${1}${2}[redacted]")
	text = redactVendorKeyMessage(text)
	text = encodedAuthorization.ReplaceAllString(text, "[redacted]")
	text = encodedBearer.ReplaceAllString(text, "[redacted]")
	text = providerCredential.ReplaceAllString(text, "[redacted]")
	text = jsonToken.ReplaceAllString(text, `${1}"[redacted]"`)
	return text
}

// redactVendorKeyMessage takes the key out of each vendorKeyMessage match, but for a value that
// is a word, as redactUnlessWord does, such as authentication_failed after "invalid_api_key".
func redactVendorKeyMessage(text string) string {
	return vendorKeyMessage.ReplaceAllStringFunc(text, func(match string) string {
		parts := vendorKeyMessage.FindStringSubmatch(match)
		if word.MatchString(parts[2]) {
			return match
		}
		return parts[1] + "[redacted]"
	})
}

func (s shapedCredential) mayMatch(text string) bool {
	for _, literal := range s.literals {
		if strings.Contains(text, literal) {
			return true
		}
	}
	return false
}

// redactUnlessWord replaces each match of rule with [redacted], but for one whose unquoted
// value, the rule's first group, is a word. The search goes on from that word, which can be
// the name before a credential: "secret: password: <key>".
func redactUnlessWord(rule *regexp.Regexp, text string) string {
	var out strings.Builder
	last, from := 0, 0
	for from < len(text) {
		match := rule.FindStringSubmatchIndex(text[from:])
		if match == nil {
			break
		}
		for i := range match {
			if match[i] >= 0 {
				match[i] += from
			}
		}
		if match[2] >= 0 && word.MatchString(text[match[2]:match[3]]) {
			from = match[2]
			continue
		}
		out.WriteString(text[last:match[0]])
		out.WriteString("[redacted]")
		last, from = match[1], match[1]
	}
	if last == 0 {
		return text
	}
	out.WriteString(text[last:])
	return out.String()
}

// RedactLogAttr is a slog ReplaceAttr that puts every logged string, the message included, and
// every error or other value through RedactLogText, since a provider's error can echo the key
// it was sent. Phone numbers and caller ids in them are masked with logMasks (logmask.go) too.
// A value is read as %+v, which is how slog's text handler prints an error (with
// its stack, see internal/stack) or a map of details, but for a []byte, which it prints as a
// string. An attribute with nothing to redact is returned as is, so its line does not change.
func RedactLogAttr(_ []string, attr slog.Attr) slog.Attr {
	var text string
	switch attr.Value.Kind() {
	case slog.KindString:
		text = attr.Value.String()
	case slog.KindAny:
		if bytes, ok := attr.Value.Any().([]byte); ok {
			text = string(bytes)
		} else {
			text = fmt.Sprintf("%+v", attr.Value.Any())
		}
	default:
		return attr
	}
	return redactedAttr(attr, text)
}

// RedactJSONLogAttr is RedactLogAttr for slog's JSON handler, which prints an error as its
// Error() and any other value as json.Marshal does: those are the texts it redacts, since an
// error's %+v can hide the text Error() shows, or add a stack. A []byte is read as its string,
// not the base64 json.Marshal would print, which keeps the key in it.
func RedactJSONLogAttr(_ []string, attr slog.Attr) slog.Attr {
	if attr.Value.Kind() != slog.KindAny {
		return RedactLogAttr(nil, attr)
	}
	value := attr.Value.Any()
	_, marshals := value.(json.Marshaler)
	if err, ok := value.(error); ok && !marshals {
		return redactedAttr(attr, err.Error())
	}
	if bytes, ok := value.([]byte); ok {
		return redactedAttr(attr, string(bytes))
	}
	text, err := json.Marshal(value)
	if err != nil {
		return attr
	}
	return redactedAttr(attr, string(text))
}

// redactedAttr is attr with its value replaced by text redacted and masked (logMasks), if that
// changed anything.
func redactedAttr(attr slog.Attr, text string) slog.Attr {
	if redacted := maskLogText(attr.Key, RedactLogText(text)); redacted != text {
		return slog.String(attr.Key, redacted)
	}
	return attr
}

// redactOperatorText is text with credentials taken out and phone numbers masked, for what
// operators read. RedactLogText alone stays for text customers see.
func redactOperatorText(key, text string) string {
	return maskLogText(key, RedactLogText(text))
}

// RedactSentryEvent is a sentry BeforeSend that puts an event's message, its exceptions'
// values, its breadcrumbs' messages, its tags, every string in its contexts, its request's URL
// and query string and its transaction's name through RedactLogText: they are an error's text,
// which can echo the key a provider was sent, as a usage alert's context does
// (internal/alerts). Phone numbers and caller ids in them are masked with logMasks
// (logmask.go) too.
func RedactSentryEvent(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
	event.Message = redactOperatorText("", event.Message)
	for i := range event.Exception {
		event.Exception[i].Value = redactOperatorText("", event.Exception[i].Value)
	}
	for _, breadcrumb := range event.Breadcrumbs {
		breadcrumb.Message = redactOperatorText("", breadcrumb.Message)
	}
	for key, value := range event.Tags {
		event.Tags[key] = redactOperatorText(key, value)
	}
	event.Transaction = redactOperatorText("", event.Transaction)
	if event.Request != nil {
		event.Request.URL = redactOperatorText("", event.Request.URL)
		// queryCredential needs the separator that precedes a parameter.
		event.Request.QueryString = strings.TrimPrefix(redactOperatorText("", "?"+event.Request.QueryString), "?")
	}
	walk := &contextWalk{budget: maxContextNodes}
	for name, context := range event.Contexts {
		event.Contexts[name] = walk.context(context)
	}
	return event
}

// maxContextNodes is how many maps, slices and typed values a contextWalk takes apart in one
// event. A value that holds itself is a cycle that would otherwise overflow the stack and end
// the process, and one that holds itself twice has more paths than could ever be walked, where
// Sentry's own encoding of it only fails. 1,000 is some fifty times what any context holds
// (a usage alert's has a dozen values), and each node of a cycle redacts the strings beside it
// (about 0.1 ms each under -race), so a cycle ends in tens of milliseconds, not seconds. Past
// it a container is left as it is, so a string inside it is not reached and not redacted. Every
// string the walk does reach is redacted, and a string costs no budget, being a leaf.
const maxContextNodes = 1000

// contextWalk redacts the maps and slices of an event's contexts, spending one budget of nodes
// on the whole event. It keeps no record of what it has walked: a container reached by two
// paths is redacted on each, as far as the budget goes.
type contextWalk struct {
	budget int
}

// context is a copy of context with every string in it redacted. It is a copy, at every
// depth, because the event shares its contexts' values with the scope it came from, which the
// next event reads too.
func (w *contextWalk) context(context map[string]any) map[string]any {
	out := make(map[string]any, len(context))
	for key, value := range context {
		out[key] = w.value(key, value)
	}
	return out
}

// value is value with every string in it redacted, in maps and slices at any depth. A value of
// another type is read as the JSON Sentry sends of it and, when that JSON holds a credential,
// replaced by it decoded and redacted. Decoded, it holds only maps, slices, strings and
// numbers, booleans and nulls. A number is a leaf like a string: under a phone key it is
// masked as text, and it is never read as JSON again, as its JSON would match the same rule
// each time. A typed value is decoded only if a rule would change its JSON as a whole under the
// parent key, so a national number under a caller key inside a struct stays. None is sent today.
func (w *contextWalk) value(key string, value any) any {
	if text, ok := value.(string); ok {
		return redactOperatorText(key, text)
	}
	if number, ok := value.(json.Number); ok {
		// A string, as a json.Number holding the mask fails json.Marshal.
		if masked := redactOperatorText(key, string(number)); masked != string(number) {
			return masked
		}
		return value
	}
	if w.budget <= 0 {
		return value
	}
	w.budget--
	switch typed := value.(type) {
	case map[string]any:
		return w.context(typed)
	case []any:
		if len(typed) == 0 {
			return value
		}
		out := make([]any, len(typed))
		for i, inner := range typed {
			out[i] = w.value(key, inner)
		}
		return out
	}
	text, err := json.Marshal(value)
	if err != nil || redactOperatorText(key, string(text)) == string(text) {
		return value
	}
	decoder := json.NewDecoder(bytes.NewReader(text))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return value
	}
	return w.value(key, decoded)
}

func safeLogDetails(details map[string]any) map[string]any {
	out := map[string]any{}
	// Explicitly allow metadata; arbitrary tool arguments/results and request headers never enter storage.
	for _, key := range []string{"turn_id", "tool_call_id", "tool", "provider", "model", "duration_ms", "context", "kind", "error_message", "error_code", "probability", "held_ms", "sfu_edge", "sfu_host", "stream_sip_host", "vendor", "vendor_call_id", "sfus_used", "stream_session_id", "customer_trunk_host",
		"agent_current_sfu", "agent_location", "agent_distance_to_sfu_km", "caller_current_sfu", "caller_location", "caller_distance_to_sfu_km", "stream_media_addr", "customer_media_addr"} {
		if value, ok := details[key]; ok {
			out[key] = SafeLogText(fmt.Sprint(value))
		}
	}
	return out
}
