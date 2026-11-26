package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/getsentry/sentry-go"
)

// bearerCredential and credentialText take their unquoted value only when it is not a word
// (see redactUnlessWord): the prose of our own errors and a WWW-Authenticate challenge put a
// word or an auth-param name there, as in "a bearer token", "open secret: cipher: ..." or
// `Bearer error="insufficient_scope"` (RFC 6750 section 3).
var bearerCredential = regexp.MustCompile(`(?i)\bbearer\s+([^\s"\x27,;}]+)`)

// credentialNames are the names credentialText takes before a value, which jsonCredentialName takes
// as the end of a JSON object's key.
const credentialNames = `api[_-]?key|access[_-]?token|refresh[_-]?token|(?:\b|_)id[_-]?token|auth[_-]?token|private[_-]?key|password|secret|authorization|cookie`

var credentialText = regexp.MustCompile(`(?i)(?:` + credentialNames + `)["\x27]?\s*[=:]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|([^\s,;}]+))`)

// jsonCredentialName is a key of a JSON object whose value is a credential: a name of
// credentialNames at the end of it (client_secret, x-api-key). Its value is taken out whole,
// whatever it is (see redactJSONText).
var jsonCredentialName = regexp.MustCompile(`(?i)(?:` + credentialNames + `)$`)

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

// jsonTokenKey is a key of a JSON object, as decoded, that jsonToken takes in the text as written:
// "token", or one that ends in a quote and token (x'token, x"token), whose quote jsonToken reads as
// the start of the key.
var jsonTokenKey = regexp.MustCompile(`(?i)(?:^|["\x27])token$`)

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
// such as %22. The names are the ones credentialText takes, and as it takes them: api_key,
// access_token, password, secret and the others wherever they stand, in any case and with or
// without their separator (x_api_key, accessToken, clientSecret), and id_token after an
// underscore or a boundary. Key and token, which end other names, take a boundary before them:
// the start of the text, a character that is not a letter, digit or underscore, or an encoded
// character, once or twice encoded (%22, %2522), so monkey%3D1, max_token%3D256 and
// sort_key%3Ddate stay. The name and the boundary are kept. The value is read as
// queryCredential reads it, whatever its length: a parameter named key is a key. A name with a
// plain = is the rule of credentialText and queryCredential.
var encodedCredential = regexp.MustCompile(`(?i)((?:^|[^A-Za-z0-9_]|%(?:25)*[0-9A-Fa-f]{2}|\\[a-z]|\\u[0-9A-Fa-f]{4})(?:key|token|id[_-]?token)|_id[_-]?token|api[_-]?key|access[_-]?token|refresh[_-]?token|auth[_-]?token|private[_-]?key|password|secret)(%(?:25)*3D)[^&\s"\x27#\\]+`)

// vendorKeyMessage is a provider's refusal that names the key it was sent, with no = or : to
// say so: "Invalid API key <32 hex>", "Incorrect API key provided: <key>", invalid_api_key, or
// the same for an API token. The key is the 16 characters or more of the key alphabet, base64
// (+ /, and = only as the padding at its end) and base64url (- _) included, after the phrase,
// so a request id of 8 hex digits after "Invalid API key." stays, and so does a glued
// name=value after it (model=..., request_id=...). A quote before the key, or a quote escaped as a JSON body has it
// (\"), stays with the phrase. The phrase stays and the key is taken out, unless it is a word
// (see redactVendorKeyMessage).
var vendorKeyMessage = regexp.MustCompile(`(?i)\b((?:invalid|incorrect)[\s_-]+(?:x-)?api[\s_-]?(?:key|token)\b(?:\s+provided)?\s*[:=]?\s*(?:\\?["\x27])?)([A-Za-z0-9_+/-]{16,}={0,2})()`)

// vendorKeyBefore is the same refusal with the key before the verdict: "The API key '<key>' is
// invalid", with single quotes, double quotes (raw or JSON-escaped) or none. The key has the
// same 16 character floor, and the phrase around it stays.
var vendorKeyBefore = regexp.MustCompile(`(?i)\b(api[\s_-]?(?:key|token)\s+(?:\\?["\x27])?)([A-Za-z0-9_+/-]{16,}={0,2})((?:\\?["\x27])?\s+(?:is|was)\s+(?:invalid|incorrect)\b)`)

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
		text = string([]rune(text)[:min(len([]rune(text)), 2048)]) + truncatedMarker
	}
	return text
}

// truncatedMarker ends a text that was cut.
const truncatedMarker = "… [truncated]"

// maxTurnText is the most of one turn's text that is kept: what was asked, thought or answered.
// The session socket takes a message of 5 MiB and the text is stored twice, on the response and
// on its item, while a real turn is a few KiB: a ceiling far above that never cuts one.
const maxTurnText = 256 << 10

// turnText is what a conversation said as it is stored: RedactLogText, cut past maxTurnText at a
// rune boundary with the marker SafeLogText adds.
func turnText(text string) string {
	text = RedactLogText(text)
	if len(text) <= maxTurnText {
		return text
	}
	cut := maxTurnText
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + truncatedMarker
}

// RedactLogText is text with every credential shape taken out, at whatever length. A caller
// that must send the whole text, such as a refusal naming what the caller sent, or a log line,
// which has no cap (RedactLogAttr), uses this. What a conversation said, read back to the model,
// is stored with turnText; everything else stored uses SafeLogText.
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

// redactCredentials is RedactLogText with no check first. A text that is JSON is redacted
// as JSON (redactJSONText), so it is JSON still; any other text goes through the rules.
func redactCredentials(text string) string {
	if startsAsJSON(text) && json.Valid([]byte(text)) {
		if redacted, ok := redactJSONText(text, false); ok {
			return redacted
		}
	}
	return redactCredentialShapes(text)
}

// redactCredentialShapes is the rules over a text as it stands. The shaped rules run first, so a
// rule after them that takes only a part of a shaped token, as vendorKeyMessage takes a JWT
// up to its first dot, does not leave the rest of it in a shape no rule matches.
func redactCredentialShapes(text string) string {
	for _, shaped := range shapedCredentials {
		if shaped.mayMatch(text) {
			text = shaped.rule.ReplaceAllString(text, "${1}[redacted]")
		}
	}
	if holdsLegacyWord(text) {
		text = redactLegacyCredentials(text)
	}
	return text
}

func redactLegacyCredentials(text string) string {
	text = redactUnlessWord(bearerCredential, text)
	text = authorizationHeader.ReplaceAllString(text, "[redacted]")
	text = redactUnlessWord(credentialText, text)
	text = urlCredential.ReplaceAllString(text, "${1}[redacted]@")
	text = queryCredential.ReplaceAllString(text, "${1}[redacted]")
	// The encoded key goes before the vendor phrase: the key class of the phrase stops at the %
	// of %3D and would leave the value of an encoded key that follows it.
	text = encodedCredential.ReplaceAllString(text, "${1}${2}[redacted]")
	text = redactVendorKeyMessage(vendorKeyMessage, text)
	text = redactVendorKeyMessage(vendorKeyBefore, text)
	text = encodedAuthorization.ReplaceAllString(text, "[redacted]")
	text = encodedBearer.ReplaceAllString(text, "[redacted]")
	text = providerCredential.ReplaceAllString(text, "[redacted]")
	text = jsonToken.ReplaceAllString(text, `${1}"[redacted]"`)
	return text
}

// jsonFrame is the object or array redactJSONText is inside of. In an object, key is true when its
// next string is a key, and token when the value after a jsonTokenKey key is next.
type jsonFrame struct {
	object, key, token bool
}

// jsonRedacted is what the value of a credential becomes in JSON.
const jsonRedacted = `"[redacted]"`

// redactJSONText is a text that is valid JSON with its credentials taken out and its shape kept,
// so whatever reads it back as JSON still can. A key that ends in one of credentialNames
// (password, client_secret, x-api-key) keeps its name, and its value, whether a string, a number,
// an object or an array, becomes "[redacted]"; true, false and null hold no credential and stay,
// as a value that is a word does in credentialText. A "token" key (see jsonTokenKey) does the same
// for a string value, as jsonToken does. Every other string, key or value, goes through redactJSONString;
// nested is true for a text that is itself a string of a JSON text, whose strings are read by
// the rules only. It is false when the text cannot be read, and the caller uses the rules on it.
func redactJSONText(text string, nested bool) (string, bool) {
	type edit struct {
		start, end int
		text       string
	}
	var edits []edit
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var stack []jsonFrame
	prev := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false
		}
		end := int(decoder.InputOffset())
		var frame *jsonFrame
		if len(stack) > 0 {
			frame = &stack[len(stack)-1]
		}
		switch typed := token.(type) {
		case json.Delim:
			switch typed {
			case '{':
				stack = append(stack, jsonFrame{object: true, key: true})
			case '[':
				stack = append(stack, jsonFrame{})
			default:
				stack = stack[:len(stack)-1]
				if len(stack) > 0 {
					stack[len(stack)-1].key = stack[len(stack)-1].object
				}
			}
		case string:
			start := prev + strings.IndexByte(text[prev:end], '"')
			switch {
			case frame != nil && frame.key:
				frame.key, frame.token = false, jsonTokenKey.MatchString(typed)
				if edited, changed := redactJSONString(text[start:end], typed, nested); changed {
					edits = append(edits, edit{start, end, edited})
				}
				if jsonCredentialName.MatchString(typed) {
					valueStart := len(text) - len(strings.TrimLeft(text[end:], ": \t\r\n"))
					valueEnd, kept, err := skipJSONValue(decoder)
					if err != nil {
						return "", false
					}
					end = valueEnd
					if !kept {
						edits = append(edits, edit{valueStart, valueEnd, jsonRedacted})
					}
					frame.key = true
				}
			case frame != nil && frame.token:
				edits = append(edits, edit{start, end, jsonRedacted})
				frame.key = true
			default:
				if edited, changed := redactJSONString(text[start:end], typed, nested); changed {
					edits = append(edits, edit{start, end, edited})
				}
				if frame != nil {
					frame.key = frame.object
				}
			}
		default:
			if frame != nil {
				frame.key = frame.object
			}
		}
		prev = end
	}
	var out strings.Builder
	last := 0
	for _, e := range edits {
		out.WriteString(text[last:e.start])
		out.WriteString(e.text)
		last = e.end
	}
	out.WriteString(text[last:])
	return out.String(), true
}

// redactJSONString is a JSON string, as written in the text (literal, quotes included) and as
// decoded, with its credentials taken out, and whether that changed it. A string that holds JSON
// is redacted as JSON, unless the string is nested in such a JSON already: one level is read as
// JSON and a deeper one by the rules, so a string that holds itself over and over costs no more
// than two reads of it. Any other is read decoded and between its quotes, as a log line quotes it:
// a rule that wants a quote before a value (a query at the start of a string) finds it, a value
// stops at the closing one, and an escape such as \" is not in the way of a rule or cut by one
// (bearerCredential takes the backslash of \" after a token). What is left is written as JSON.
// A literal with no escape (and no byte the decoder replaced) is the decoded string between its
// quotes, so the rules read it once.
// The string keeps its escapes (\u0026) when the rules, run on it as written, leave the same text.
func redactJSONString(literal, decoded string, nested bool) (string, bool) {
	if !nested && startsAsJSON(decoded) && json.Valid([]byte(decoded)) {
		if redacted, ok := redactJSONText(decoded, true); ok {
			if redacted == decoded {
				return literal, false
			}
			return jsonString(redacted), true
		}
	}
	escaped := literal[1:len(literal)-1] != decoded
	read := literal
	if escaped {
		// Only the rules read this text; what is written is jsonString(redacted) or a literal
		// that decodes to redacted.
		var quoted strings.Builder
		quoted.Grow(len(decoded) + 2)
		quoted.WriteByte('"')
		quoted.WriteString(decoded)
		quoted.WriteByte('"')
		read = quoted.String()
	}
	read = redactCredentialShapes(read)
	redacted := strings.TrimSuffix(strings.TrimPrefix(read, `"`), `"`)
	if redacted == decoded {
		return literal, false
	}
	written := read
	if escaped {
		written = redactCredentialShapes(literal)
	}
	var kept string
	if json.Unmarshal([]byte(written), &kept) == nil && kept == redacted {
		return written, true
	}
	return jsonString(redacted), true
}

// startsAsJSON is true for a text that starts, after JSON's white space, as an object, an array or
// a string does, so it may be JSON.
func startsAsJSON(text string) bool {
	text = strings.TrimLeft(text, " \t\r\n")
	return text != "" && (text[0] == '{' || text[0] == '[' || text[0] == '"')
}

// skipJSONValue reads the value that comes next from decoder, an object and an array to their
// end, and says where it ends and whether it is one that stays: true, false or null.
func skipJSONValue(decoder *json.Decoder) (end int, kept bool, err error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, false, err
	}
	switch token.(type) {
	case json.Delim:
		for depth := 1; depth > 0; {
			inner, err := decoder.Token()
			if err != nil {
				return 0, false, err
			}
			if delim, ok := inner.(json.Delim); ok {
				if delim == '{' || delim == '[' {
					depth++
				} else {
					depth--
				}
			}
		}
	case bool, nil:
		kept = true
	}
	return int(decoder.InputOffset()), kept, nil
}

// jsonString is text as a JSON string, with no escape for <, > and &, which the text had none for.
func jsonString(text string) string {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(text) // a string always encodes
	return strings.TrimSuffix(out.String(), "\n")
}

// redactVendorKeyMessage takes the key out of each match of rule (vendorKeyMessage or
// vendorKeyBefore: the phrase before it, the key, the phrase after it), but for a value that
// is a word, as redactUnlessWord does, such as authentication_failed after "invalid_api_key".
func redactVendorKeyMessage(rule *regexp.Regexp, text string) string {
	return rule.ReplaceAllStringFunc(text, func(match string) string {
		parts := rule.FindStringSubmatch(match)
		if word.MatchString(parts[2]) {
			return match
		}
		return parts[1] + "[redacted]" + parts[3]
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
