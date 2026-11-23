package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

func TestSafeLogTextCredentials(t *testing.T) {
	for _, message := range []string{
		`{"authorization":"Bearer secret-value"}`,
		`{"password":"secret-value with spaces"}`,
		`password='secret-value with spaces'`,
		"Authorization: Basic secret-value",
		"https://user:secret-value@example.com/path",
		`{"nested":{"api_key":"secret-value"}}`,
	} {
		t.Run(message, func(t *testing.T) {
			if result := SafeLogText(message); strings.Contains(result, "secret-value") {
				t.Errorf("credential survived redaction: %s", result)
			}
		})
	}
	if result := SafeLogText("perplexity: PERPLEXITY_API_KEY is required"); result != "perplexity: PERPLEXITY_API_KEY is required" {
		t.Fatal("redaction removed the actionable error")
	}
	if result := SafeLogText(strings.Repeat("x", 10000)); len(result) > 8192 || !strings.Contains(result, "[truncated]") {
		t.Fatal("long messages must be bounded and marked")
	}
}

// A provider's key travels in a URL query or a URL-encoded header, and a Go http or websocket
// error prints that URL whole. The rows are the names we send a key in (key, token), the names
// credentialText took before this change (api_key, apikey, access_token, client_secret) and
// headers written into a URL.
func TestSafeLogTextQueryAndEncodedCredentials(t *testing.T) {
	secret := rand.Text()
	backslash := `\`
	for name, message := range map[string]string{
		"key after a json-escaped ampersand":         `{"url":"wss://live.example/ws?alt=sse` + backslash + `u0026key=` + secret + `"}`,
		"token after a json-escaped ampersand":       `{"url":"https://ipinfo.io/json?lang=en` + backslash + `u0026token=` + secret + `"}`,
		"key in a twice-encoded url":                 `redirect=https%253A%252F%252Flive.example%252Fws%253Fkey%253D` + secret,
		"key after a twice-encoded ampersand":        `redirect=https%253A%252F%252Flive.example%252Fws%253Fa%253D1%2526key%253D` + secret,
		"key after a json-escaped equals sign":       `{"url":"wss://live.example/ws?key` + backslash + `u003d` + secret + `"}`,
		"authorization behind a twice-encoded quote": `q=%257B%2522Authorization%2522%253A%2522Basic%2520` + secret + `%2522%257D`,
		"encoded authorization with an equals sign":  `h=authorization%3DBasic%20` + secret,
		"key after an html-escaped ampersand":        `<a href="https://live.example/ws?alt=sse&amp;key=` + secret + `">`,
		"twice-encoded bearer":                       `h=Bearer%2520` + secret,
		"twice-encoded authorization":                `h=Authorization%253A%2520Basic%2520` + secret,
		"key first (gemini stt and sts)":             `Get "wss://live.example/ws?key=` + secret + `": dial tcp: i/o timeout`,
		"key at the start of a json query":           `{"method":"GET","query":"key=` + secret + backslash + `u0026echo_status=404"}`,
		"token at the start of a quoted query":       `query="token=` + secret + `"`,
		"key after another parameter":                `wss://live.example/ws?alt=sse&key=` + secret + `&model=m`,
		"key in upper case":                          `wss://live.example/ws?KEY=` + secret,
		"key in an encoded url":                      `redirect=https%3A%2F%2Flive.example%2Fws%3Fkey%3D` + secret,
		"key after an encoded ampersand":             `redirect=https%3A%2F%2Flive.example%2Fws%3Fa%3D1%26key%3D` + secret,
		"token (ipinfo)":                             `Get "https://ipinfo.io/203.0.113.10/json?token=` + secret + `": EOF`,
		"token after another parameter":              `https://ipinfo.io/json?lang=en&token=` + secret,
		"api_key":                                    `https://api.example/v1?api_key=` + secret,
		"apikey":                                     `https://api.example/v1?apikey=` + secret,
		"access_token":                               `https://api.example/v1?access_token=` + secret,
		"client_secret":                              `https://auth.example/token?client_secret=` + secret,
		"encoded authorization bearer":               `h=Authorization%3A%20Bearer%20` + secret,
		"encoded authorization plus":                 `h=Authorization%3A+Bearer+` + secret,
		"encoded authorization basic":                `h=authorization%3A%20Basic%20` + secret,
		"encoded authorization json":                 `q=%7B%22Authorization%22%3A%22Basic%20` + secret + `%22%7D`,
		"encoded authorization quoted":               `q=%7B%27authorization%27%3A%27Basic%20` + secret + `%27%7D`,
		"encoded authorization param":                `redirect=https%3A%2F%2Fapi.example%2Fv1%3Fauthorization%3DBearer%2520` + secret,
		"encoded set-cookie":                         `h=Set-Cookie%3A%20session%3D` + secret,
		"encoded proxy-authorization":                `h=Proxy-Authorization%3A%20Basic%20` + secret,
		"encoded cookie":                             `h=Cookie%3A%20session%3D` + secret,
		"encoded bearer alone":                       `h=Bearer%20` + secret,
		"encoded bearer after a space":               `q=sent%20Bearer%20` + secret,
		"form-encoded bearer alone":                  `h=Bearer+` + secret,
	} {
		t.Run(name, func(t *testing.T) {
			if result := SafeLogText(message); strings.Contains(result, secret) {
				t.Errorf("credential survived redaction: %s", result)
			}
		})
	}
}

// The query rule names whole parameters: the parameters around a key stay, and parameters
// that only start with key or token, or prose that names them, are kept byte for byte.
func TestSafeLogTextKeepsTheRestOfTheURL(t *testing.T) {
	secret := rand.Text()
	backslash := `\`
	// The characters that end a redacted value: a space, #, ' and " each stop it, in the query
	// rule, the encoded authorization rule and the encoded bearer rule.
	for message, want := range map[string]string{
		`dial wss://live.example/ws?key=` + secret + ` failed: 401`:                  `dial wss://live.example/ws?key=[redacted] failed: 401`,
		`wss://live.example/ws?key=` + secret + `#top`:                               `wss://live.example/ws?key=[redacted]#top`,
		`url='wss://live.example/ws?key=` + secret + `' failed`:                      `url='wss://live.example/ws?key=[redacted]' failed`,
		`h=Authorization%3A%20Basic%20` + secret + ` failed: 401`:                    `h=[redacted] failed: 401`,
		`"h=Authorization%3A%20Basic%20` + secret + `" failed`:                       `"h=[redacted]" failed`,
		`h=Authorization%3A%20Basic%20` + secret + `#top`:                            `h=[redacted]#top`,
		`h=Bearer%20` + secret + ` failed: 401`:                                      `h=[redacted] failed: 401`,
		`h=Bearer%20` + secret + `#top`:                                              `h=[redacted]#top`,
		`{"url":"wss://live.example/ws?key=` + secret + backslash + `u0026model=m"}`: `{"url":"wss://live.example/ws?key=[redacted]` + backslash + `u0026model=m"}`,
		`h=Authorization%3A%20Basic%20` + secret + backslash + `u0026model=m`:        `h=[redacted]` + backslash + `u0026model=m`,
		`h=Bearer%20` + secret + backslash + `u0026model=m`:                          `h=[redacted]` + backslash + `u0026model=m`,
		`Get "wss://live.example/ws?alt=sse&key=` + secret + `&model=m": EOF`:        `Get "wss://live.example/ws?alt=sse&key=[redacted]&model=m": EOF`,
		`h=Authorization%3A%20Basic%20` + secret + `&model=m`:                        `h=[redacted]&model=m`,
		`h=Bearer%20` + secret + `&model=m`:                                          `h=[redacted]&model=m`,
		`{"query":"key=` + secret + backslash + `u0026echo_status=404"}`:             `{"query":"key=[redacted]` + backslash + `u0026echo_status=404"}`,
	} {
		if result := SafeLogText(message); result != want {
			t.Errorf("SafeLogText(%q) = %q, want %q", message, result, want)
		}
	}
	// The answers base 6b9627f99 gives for them, the same as the input; the bearer rule takes no
	// word after bearer, so the last one's "bearer of" stays too.
	for message, want := range map[string]string{
		`wss://stt.example/v1/listen?model=nova-3&keyterm=Stream&keyterms=Vision&keyterms_prompt=%5B%22Agents%22%5D&language_code=en`: `wss://stt.example/v1/listen?model=nova-3&keyterm=Stream&keyterms=Vision&keyterms_prompt=%5B%22Agents%22%5D&language_code=en`,
		`wss://stt.example/v1/listen?monkey=banana&keys=3&tokens=12&token_type_hint=access_token&max_tokens=256`:                      `wss://stt.example/v1/listen?monkey=banana&keys=3&tokens=12&token_type_hint=access_token&max_tokens=256`,
		`the key=value pair and token=12 in prose`:                                                        `the key=value pair and token=12 in prose`,
		`Get "https://api.example/v1/models?page=2&limit=50": dial tcp: lookup api.example: no such host`: `Get "https://api.example/v1/models?page=2&limit=50": dial tcp: lookup api.example: no such host`,
		`authorization%20failed for model gpt and the bearer of bad news`:                                 `authorization%20failed for model gpt and the bearer of bad news`,
	} {
		if result := SafeLogText(message); result != want {
			t.Errorf("SafeLogText(%q) = %q, want %q", message, result, want)
		}
	}
}

// A key in an encoded query that no ? or & stands before (a text cut at the parameter, a name
// after an encoded quote), and a provider's refusal that names the key with no = or : before it.
// Each row is the whole answer, so what stays around the key is checked too. The rows with no
// credential are the answers base 31fd2dd96 gives, the same as the input.
func TestSafeLogTextTakesAnEncodedKeyAndAVendorRefusalOut(t *testing.T) {
	raw := make([]byte, 16)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	secret := hex.EncodeToString(raw)
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ4In0." + secret
	for name, tc := range map[string]struct{ in, want string }{
		"jwt after the phrase":                   {"Invalid API token " + jwt, "Invalid API token [redacted]"},
		"jwt after the phrase and a colon":       {"Invalid API key: " + jwt, "Invalid API key: [redacted]"},
		"key":                                    {"key%3D" + secret, "key%3D[redacted]"},
		"api_key":                                {"api_key%3D" + secret, "api_key%3D[redacted]"},
		"token":                                  {"token%3D" + secret, "token%3D[redacted]"},
		"mixed case equals sign":                 {"key%3d" + secret, "key%3d[redacted]"},
		"upper case name":                        {"API_KEY%3d" + secret, "API_KEY%3d[redacted]"},
		"name with a prefix":                     {"x_api_key%3D" + secret, "x_api_key%3D[redacted]"},
		"apikey":                                 {"apikey%3D" + secret, "apikey%3D[redacted]"},
		"access_token":                           {"access_token%3D" + secret, "access_token%3D[redacted]"},
		"client_secret":                          {"client_secret%3D" + secret, "client_secret%3D[redacted]"},
		"password":                               {"password%3D" + secret, "password%3D[redacted]"},
		"camel case accessToken":                 {"accessToken%3D" + secret, "accessToken%3D[redacted]"},
		"camel case clientSecret":                {"clientSecret%3D" + secret, "clientSecret%3D[redacted]"},
		"camel case authToken":                   {"authToken%3D" + secret, "authToken%3D[redacted]"},
		"camel case refreshToken":                {"refreshToken%3D" + secret, "refreshToken%3D[redacted]"},
		"camel case idToken":                     {"idToken%3D" + secret, "idToken%3D[redacted]"},
		"id_token after a prefix":                {"x_id_token%3D" + secret, "x_id_token%3D[redacted]"},
		"camel case privateKey":                  {"privateKey%3D" + secret, "privateKey%3D[redacted]"},
		"after a twice-encoded quote":            {"q%3D%2522key%253D" + secret, "q%3D%2522key%253D[redacted]"},
		"after a twice-encoded space":            {"%2520key%253D" + secret, "%2520key%253D[redacted]"},
		"camel case after a twice-encoded ?":     {"%253FaccessToken%253D" + secret, "%253FaccessToken%253D[redacted]"},
		"camel case apiKey":                      {"apiKey%3D" + secret, "apiKey%3D[redacted]"},
		"twice-encoded equals sign":              {"key%253D" + secret, "key%253D[redacted]"},
		"in the middle of a sentence":            {"request failed: key%3D" + secret + " tail", "request failed: key%3D[redacted] tail"},
		"after an encoded quote":                 {"q=%22key%3D" + secret + "%22", "q=%22key%3D[redacted]"},
		"in the middle of a query":               {"wss://live.example/ws?a=1&key%3D" + secret + "&b=2", "wss://live.example/ws?a=1&key%3D[redacted]&b=2"},
		"in an encoded url":                      {"https%3A%2F%2Flive.example%2Fws%3Fa%3D1%26key%3D" + secret, "https%3A%2F%2Flive.example%2Fws%3Fa%3D1%26key%3D[redacted]"},
		"after a json-escaped newline":           {`{"m":"x\nkey%3D` + secret + `"}`, `{"m":"x\nkey%3D[redacted]"}`},
		"after a json-escaped quote":             {`{"m":"\u0022key%3D` + secret + `"}`, `{"m":"\u0022key%3D[redacted]"}`},
		"ends at a quote":                        {`"key%3D` + secret + `" failed`, `"key%3D[redacted]" failed`},
		"ends at a single quote":                 {`'key%3D` + secret + `' failed`, `'key%3D[redacted]' failed`},
		"ends at a fragment":                     {"key%3D" + secret + "#top", "key%3D[redacted]#top"},
		"ends at a json escape":                  {"key%3D" + secret + `\u0026model=m`, "key%3D[redacted]" + `\u0026model=m`},
		"invalid api key":                        {"Invalid API key " + secret, "Invalid API key [redacted]"},
		"invalid api key with a colon":           {"invalid api key: " + secret, "invalid api key: [redacted]"},
		"incorrect api key provided":             {"Incorrect API key provided: " + secret, "Incorrect API key provided: [redacted]"},
		"incorrect key from a vendor":            {"Incorrect API key provided: sk-" + secret, "Incorrect API key provided: [redacted]"},
		"invalid_api_key code":                   {"invalid_api_key " + secret, "invalid_api_key [redacted]"},
		"invalid x-api-key":                      {"Invalid x-api-key " + secret, "Invalid x-api-key [redacted]"},
		"invalid api token":                      {"Invalid API token " + secret, "Invalid API token [redacted]"},
		"key of 16 characters":                   {"Invalid API key " + secret[:16], "Invalid API key [redacted]"},
		"after a prefix and a colon":             {`provider: 401: {"message":"Invalid API key ` + secret + `"}`, `provider: 401: {"message":"Invalid API key [redacted]"}`},
		"text after the key":                     {"Invalid API key " + secret + " for model m", "Invalid API key [redacted] for model m"},
		"word then key":                          {"Invalid API key authentication_failed_error; Invalid API key " + secret, "Invalid API key authentication_failed_error; Invalid API key [redacted]"},
		"key then word":                          {"Invalid API key " + secret + "; Invalid API key authentication_failed_error", "Invalid API key [redacted]; Invalid API key authentication_failed_error"},
		"before the verdict, word then key":      {"API key authentication_failed_error is invalid; API key " + secret + " is invalid", "API key authentication_failed_error is invalid; API key [redacted] is invalid"},
		"key before the verdict, single quotes":  {"The API key '" + secret + "' is invalid", "The API key '[redacted]' is invalid"},
		"key before the verdict, double quotes":  {`The API key "` + secret + `" is invalid`, `The API key "[redacted]" is invalid`},
		"key before the verdict, no quotes":      {"The API key " + secret + " is invalid", "The API key [redacted] is invalid"},
		"api token before the verdict":           {"api token '" + secret + "' was incorrect", "api token '[redacted]' was incorrect"},
		"single-quoted key after a colon":        {"Invalid API key: '" + secret + "'", "Invalid API key: '[redacted]'"},
		"double-quoted key after the phrase":     {`Invalid API key "` + secret + `"`, `Invalid API key "[redacted]"`},
		"json-escaped quote after the phrase":    {`{"message":"Invalid API key: \"` + secret + `\""}`, `{"message":"Invalid API key: \"[redacted]\""}`},
		"json-escaped quotes before the verdict": {`{"detail":"The API key \"` + secret + `\" is invalid"}`, `{"detail":"The API key \"[redacted]\" is invalid"}`},
		"base64 key":                             {"Invalid API key Ab" + secret[:10] + "+/" + secret[10:] + "==", "Invalid API key [redacted]"},
		"base64 key before the verdict":          {"The API key '" + secret[:16] + "+/" + secret[16:] + "=' is invalid", "The API key '[redacted]' is invalid"},
		"key of 16 before the verdict":           {"API key '" + secret[:16] + "' is invalid for model m", "API key '[redacted]' is invalid for model m"},
		"key then an encoded key, in this order": {"Invalid API key " + secret[:16] + "-key%3D" + secret, "Invalid API key [redacted]%3D[redacted]"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, SafeLogText(tc.in))
			require.Equal(t, tc.want, SafeLogText(tc.want), "a second pass changes the text")
		})
	}
	for name, text := range map[string]string{
		"no key named":                       "Invalid API key provided",
		"no key named, other wording":        "Incorrect API key provided",
		"request id of 8 hex digits":         "Invalid API key " + secret[:8],
		"key of 15 characters":               "Invalid API key " + secret[:15],
		"request id after the sentence":      "Invalid API key. Request id " + secret[:8],
		"request id in parentheses":          "Invalid API key (request " + secret[:8] + ")",
		"a word after the phrase":            "invalid_api_key authentication_failed_error",
		"an id with no phrase":               "request id " + secret,
		"id of 15 before the verdict":        "API key '" + secret[:15] + "' is invalid",
		"id of 8 before the verdict":         "API key " + secret[:8] + " is invalid",
		"a word before the verdict":          "API key authentication_failed_error is invalid",
		"key before another verdict":         "API key " + secret + " is set",
		"phrase with no key":                 "API key not valid. Please pass a valid API key.",
		"already redacted vendor text":       "Invalid API key [redacted]",
		"already redacted query":             "key%3D[redacted]",
		"name with no value":                 "key%3D",
		"name inside a word":                 "monkey%3D" + secret,
		"key word glued to the phrase":       "Invalid API keystore" + secret,
		"plural name":                        "keys%3D" + secret,
		"tokens":                             "max_tokens%3D256 tokens%3D12",
		"a name ending in _token":            "max_token%3D256",
		"a name ending in _key":              "sort_key%3Ddate",
		"a name ending in id_token":          "valid_token%3D" + secret,
		"phrase glued to the word before it": "noninvalid api key " + secret,
		"phrase glued together":              "invalidapi key " + secret,
		"name glued to the word before it":   "rapi key " + secret + " is invalid",
		"name glued to the key":              "API key" + secret + " is invalid",
		"key glued to the verdict":           "API key " + secret + "is invalid",
		"name with a plain equals":           "the key=value pair",
		"a name=value after the phrase":      "Invalid API key provided: model=gpt-4o-mini-transcribe",
		"a request id after the colon":       "Invalid API key: request_id=req_0123456789abcdef",
	} {
		t.Run("keeps "+name, func(t *testing.T) {
			require.Equal(t, text, SafeLogText(text))
		})
	}
}

// Our own errors name a bearer token, a secret or a password in prose, and a WWW-Authenticate
// challenge starts with Bearer and an auth-param (RFC 6750 section 3): with no credential in
// them, they are kept byte for byte.
func TestRedactLogTextKeepsProseAndChallenges(t *testing.T) {
	for _, text := range []string{
		"bearer: the credential is not a bearer credential",
		"sandbox: the connection's scheme sends no bearer token",
		"auth: open secret: cipher: message authentication failed",
		"phone: open sip trunk password: cipher: message authentication failed",
		"store: rotate guest secret: context canceled",
		"store: set guest secret: EOF",
		"auth: open secret: ERROR #23505 duplicate key value",
		"apikey: the key is required",
		"needs_reauthorization: the provider no longer takes the credential",
		"usage: USAGE_INGEST_TOKEN is required: the bearer token producers and routers call with",
		"a Bearer Token. And a bearer or api_key connection",
		`Bearer error="insufficient_scope", scope="files:read files:write"`,
		`Bearer realm="", authorization_uri="https://x"`,
		`Bearer resource_metadata="https://resource.example.com/.well-known/oauth-protected-resource"`,
	} {
		if result := RedactLogText(text); result != text {
			t.Errorf("RedactLogText(%q) = %q, want it unchanged", text, result)
		}
	}
}

// A value that is not a word is a credential, after a word the rule passed over too.
func TestRedactLogTextTakesAValueThatIsNotAWord(t *testing.T) {
	secret := rand.Text()
	for text, want := range map[string]string{
		"Authorization: Bearer sk-test-" + secret:             "[redacted]",
		"sent Bearer " + secret + " and failed":               "sent [redacted] and failed",
		"sent bearer " + strings.ToLower(secret) + "0 failed": "sent [redacted] failed",
		"https://auth.example/token?client_secret=" + secret:  "https://auth.example/token?client_[redacted]",
		`{"password":"` + secret + `"}`:                       `{"[redacted]}`,
		`{"password":"word"}`:                                 `{"[redacted]}`,
		"password: hunter2 rejected":                          "[redacted] rejected",
		"secret: password: " + secret:                         "secret: [redacted]",
		"bearer of Bearer " + secret:                          "bearer of [redacted]",
		"Bearer private-token":                                "[redacted]",
		"Bearer ABCDEFGHI":                                    "[redacted]",
		`Bearer error="invalid_token", Bearer ` + secret:      `Bearer error="invalid_token", [redacted]`,
	} {
		if result := RedactLogText(text); result != want {
			t.Errorf("RedactLogText(%q) = %q, want %q", text, result, want)
		}
	}
}

// A connector's error can echo a token in a shape no keyword names: each row is a text with one,
// and the text redacted, so a rule that is removed, or a part of it that is cut, fails its row.
func TestRedactLogTextTakesAConnectorTokenShape(t *testing.T) {
	secret := rand.Text()
	jwt := "eyJhbGciOi-_JIUzI1NiJ9.eyJ" + secret[:12] + "-_." + secret[12:] + "_-"
	pem := pemLine("BEGIN", "RSA ") + "\nMIIEowIBAAKCAQEA+/" + secret + "\n" + secret + "==\n" + pemLine("END", "RSA ")
	rows := map[string]string{
		"slack bot token":                          "slack: invalid_auth for xoxb-123456789012-" + secret,
		"slack user token":                         "slack: invalid_auth for xoxp-123456789012-" + secret,
		"slack app token":                          "slack: invalid_auth for xoxa-123456789012-" + secret,
		"slack legacy token":                       "slack: invalid_auth for xoxo-123456789012-" + secret,
		"slack session token":                      "slack: invalid_auth for xoxs-123456789012-" + secret,
		"slack refresh token":                      "slack: invalid_auth for xoxr-123456789012-" + secret,
		"slack rotating refresh token":             "slack: invalid_auth for xoxe-1-" + secret,
		"slack app-level token":                    "slack: invalid_auth for xapp-1-A0123456789-123456789012-" + secret,
		"aws temporary access key id":              "aws: InvalidClientTokenId for ASIA" + secret[:16],
		"github personal access token":             "github: 401 for ghp_" + secret,
		"github oauth token":                       "github: 401 for gho_" + secret,
		"github user-to-server token":              "github: 401 for ghu_" + secret,
		"github server-to-server token":            "github: 401 for ghs_" + secret,
		"github refresh token":                     "github: 401 for ghr_" + secret,
		"github fine-grained token":                "github: 401 for github_pat_11A" + secret + "_" + secret,
		"token in a nested encoded query":          "https://a.example/?next=%3Faccess_token%3Dghp_" + secret,
		"google api key":                           "google: 403 for AIza" + secret + "-_abcdefghij",
		"google api key of 35 characters":          "google: 403 for AIza" + secret + secret[:9],
		"aws access key id":                        "aws: InvalidClientTokenId for AKIA" + secret[:16],
		"bare jwt":                                 "provider said " + jwt + " is expired",
		"bare jwt in quotes":                       `body="` + jwt + `"`,
		"jwt with an empty signature":              "provider said eyJhbGciOiJub25lIn0.eyJ" + secret[:12] + ". failed",
		"jwt after a colon":                        "got:" + jwt,
		"pem block":                                "parse: " + pem + ": no key found",
		"pem block with real newlines":             "parse: " + pemLine("BEGIN", "") + "\nMIIE" + secret + "\n" + pemLine("END", "") + "\nfailed",
		"encrypted pem block":                      "parse: " + pemLine("BEGIN", "ENCRYPTED ") + "\nMIIE" + secret + "\n" + pemLine("END", "ENCRYPTED ") + ": bad",
		"legacy encrypted pem block":               "parse: " + pemLine("BEGIN", "RSA ") + "\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,0A1B2C3D4E5F60718293A4B5C6D7E8F9\n\nMIIE" + secret + "\n" + pemLine("END", "RSA ") + ": bad",
		"legacy encrypted pem block escaped":       `parse: "` + pemLine("BEGIN", "RSA ") + `\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,0A1B2C3D4E5F60718293A4B5C6D7E8F9\n\nMIIE` + secret + `" failed`,
		"pem block cut before its end":             `parse: "` + pemLine("BEGIN", "") + `\nMIIE+/` + secret + `==" failed`,
		"id_token":                                 "oauth: bad id_token=" + secret + " from the provider",
		"id_token as json":                         `{"id_token":"` + secret + `"}`,
		"id-token header":                          "x-id-token: " + secret,
		"prefixed id_token":                        "oauth: client_id_token=" + secret + " refused",
		"auth_token":                               "request failed auth_token=" + secret + " refused",
		"auth_token as json":                       `{"auth_token":"` + secret + `"}`,
		"x-auth-token":                             "x-auth-token: " + secret,
		"oauth_token":                              "oauth_token=" + secret,
		"private_key as json":                      `{"type":"service_account","private_key":"` + pem + `","client_email":"a@b.example"}`,
		"private_key with a dash":                  "private-key=" + secret,
		"token as json":                            `{"ok":true,"token":"` + secret + `","scope":"read"}`,
		"token as json with spaces":                `{"ok":true,"token" : "` + secret + `"}`,
		"token as json in upper case":              `{"Token":"` + secret + `"}`,
		"token as json with an escaped quote":      `{"token":"` + secret + `\"` + secret + `","scope":"read"}`,
		"token as json with a word for its value":  `{"token":"expired"}`,
		"token as a dict with single quotes":       `{'token': '` + secret + `', 'scope': 'read'}`,
		"token as json after a bearer type":        `{"token_type":"bearer","token":"` + secret + `"}`,
		"token as json in a connector error":       "connector github: exchange failed: 200 OK " + `{"token":"` + secret + `"}`,
		"token as json after a nested object":      `{"data":{"token":"` + secret + `"}}`,
		"token as json after a number key":         `{"expires_in":3600,"token":"` + secret + `"}`,
		"token as json with a json null before it": `{"error":null,"token":"` + secret + `"}`,
	}
	want := map[string]string{
		"slack bot token":                          "slack: invalid_auth for [redacted]",
		"slack user token":                         "slack: invalid_auth for [redacted]",
		"slack app token":                          "slack: invalid_auth for [redacted]",
		"slack legacy token":                       "slack: invalid_auth for [redacted]",
		"slack session token":                      "slack: invalid_auth for [redacted]",
		"slack refresh token":                      "slack: invalid_auth for [redacted]",
		"slack rotating refresh token":             "slack: invalid_auth for [redacted]",
		"slack app-level token":                    "slack: invalid_auth for [redacted]",
		"aws temporary access key id":              "aws: InvalidClientTokenId for [redacted]",
		"github personal access token":             "github: 401 for [redacted]",
		"github oauth token":                       "github: 401 for [redacted]",
		"github user-to-server token":              "github: 401 for [redacted]",
		"github server-to-server token":            "github: 401 for [redacted]",
		"github refresh token":                     "github: 401 for [redacted]",
		"github fine-grained token":                "github: 401 for [redacted]",
		"token in a nested encoded query":          "https://a.example/?next=%3Faccess_token%3D[redacted]",
		"google api key":                           "google: 403 for [redacted]",
		"google api key of 35 characters":          "google: 403 for [redacted]",
		"aws access key id":                        "aws: InvalidClientTokenId for [redacted]",
		"bare jwt":                                 "provider said [redacted] is expired",
		"bare jwt in quotes":                       `body="[redacted]"`,
		"jwt with an empty signature":              "provider said [redacted] failed",
		"jwt after a colon":                        "got:[redacted]",
		"pem block":                                "parse: [redacted]: no key found",
		"pem block with real newlines":             "parse: [redacted]\nfailed",
		"encrypted pem block":                      "parse: [redacted]: bad",
		"legacy encrypted pem block":               "parse: [redacted]: bad",
		"legacy encrypted pem block escaped":       `parse: "[redacted]" failed`,
		"pem block cut before its end":             `parse: "[redacted]" failed`,
		"id_token":                                 "oauth: bad [redacted] from the provider",
		"id_token as json":                         `{"[redacted]}`,
		"id-token header":                          "x-[redacted]",
		"prefixed id_token":                        "oauth: client[redacted] refused",
		"auth_token":                               "request failed [redacted] refused",
		"auth_token as json":                       `{"[redacted]}`,
		"x-auth-token":                             "x-[redacted]",
		"oauth_token":                              "o[redacted]",
		"private_key as json":                      `{"type":"service_account","[redacted],"client_email":"a@b.example"}`,
		"private_key with a dash":                  "[redacted]",
		"token as json":                            `{"ok":true,"token":"[redacted]","scope":"read"}`,
		"token as json with spaces":                `{"ok":true,"token" : "[redacted]"}`,
		"token as json in upper case":              `{"Token":"[redacted]"}`,
		"token as json with an escaped quote":      `{"token":"[redacted]","scope":"read"}`,
		"token as json with a word for its value":  `{"token":"[redacted]"}`,
		"token as a dict with single quotes":       `{'token': "[redacted]", 'scope': 'read'}`,
		"token as json after a bearer type":        `{"token_type":"bearer","token":"[redacted]"}`,
		"token as json in a connector error":       "connector github: exchange failed: 200 OK " + `{"token":"[redacted]"}`,
		"token as json after a nested object":      `{"data":{"token":"[redacted]"}}`,
		"token as json after a number key":         `{"expires_in":3600,"token":"[redacted]"}`,
		"token as json with a json null before it": `{"error":null,"token":"[redacted]"}`,
	}
	require.Len(t, want, len(rows))
	for name, text := range rows {
		t.Run(name, func(t *testing.T) {
			got := RedactLogText(text)
			require.NotContains(t, got, secret)
			require.Equal(t, want[name], got)
		})
	}
}

// A provider's JSON answer keeps its shape when its token is taken out: the stored text is read
// back as JSON, so the key stays and its value is "[redacted]".
func TestRedactLogTextKeepsAJSONAnswerValidWhenItsTokenIsTakenOut(t *testing.T) {
	secret := rand.Text()
	for _, text := range []string{
		`{"ok":true,"token":"` + secret + `","scope":"read"}`,
		`{"token":"` + secret + `"}`,
		`{"data":{"token" : "` + secret + `\"` + secret + `"},"n":1}`,
		`{"error":null,"token":"expired","token_count":5}`,
	} {
		require.True(t, json.Valid([]byte(text)), text)
		got := RedactLogText(text)
		require.NotContains(t, got, secret)
		require.True(t, json.Valid([]byte(got)), got)
		require.Contains(t, got, `"[redacted]"`)
	}
}

// A token is taken out after whatever may stand before it in a logged line: the start of the text,
// a space, a quote, an ampersand, a JSON escape (\n, \t, \u0026) or a percent escape (%3D,
// %20, %22), which a word boundary would not see when a letter follows. One row per shape and
// boundary, so each rule's boundary is tested apart.
func TestRedactLogTextTakesAConnectorTokenShapeAfterAnyBoundary(t *testing.T) {
	secret := rand.Text()
	shapes := map[string]string{
		"slack":             "xoxb-123456789012-" + secret,
		"slack rotating":    "xoxe-1-" + secret,
		"slack app":         "xapp-1-A0123456789-" + secret,
		"github":            "ghp_" + secret,
		"github fine-grain": "github_pat_11A" + secret + "_" + secret,
		"google":            "AIza" + secret + "-_abcdefghij",
		"aws":               "AKIA" + secret[:16],
		"aws temporary":     "ASIA" + secret[:16],
	}
	boundaries := map[string]string{
		"start":                   "",
		"space":                   " ",
		"quote":                   `"`,
		"ampersand":               "&",
		"json newline":            `\n`,
		"json tab":                `\t`,
		"json unicode ampersand":  `\u0026`,
		"percent encoded equals":  "%3D",
		"percent encoded space":   "%20",
		"percent encoded quote":   "%22",
		"percent after a letter":  "a%3D",
		"json newline after word": "word\\n",
	}
	for shape, token := range shapes {
		for boundary, before := range boundaries {
			t.Run(shape+" after "+boundary, func(t *testing.T) {
				text := before + token + " end"
				got := RedactLogText(text)
				require.NotContains(t, got, secret)
				require.Equal(t, before+"[redacted] end", got)
			})
		}
	}
}

// What looks like a token shape and is not one stays byte for byte, in the rules' own words: a
// prefix with a short tail, prose that starts with gh or xox, a counter of tokens, a key name,
// base64 with no dots and a dotted text that does not start as a JWT does.
func TestRedactLogTextKeepsWhatOnlyLooksLikeATokenShape(t *testing.T) {
	for _, text := range []string{
		"max_tokens: 256 and tokens: 12",
		`{"max_tokens":256,"tokens":["a","b"],"token_count":5,"keyterm":"Stream"}`,
		`{"usage":{"input_tokens":123,"output_tokens":45},"token_type":"Bearer"}`,
		"input_tokens: 123 output_tokens=45 keyterm=Stream",
		`{"token":null}`,
		`{"token":12}`,
		`{"token":["a"]}`,
		`{"token":true}`,
		`{"x_token":"abc","tokens":"abc","my-token":"abc"}`,
		`{"note":"token: 'a1b2'"}`,
		"the token is missing",
		"token: expired",
		"token_type_hint=id_token&client=a",
		`{"token_type_hint":"id_token"}`,
		"valid_token=a1b2 and solid_token: 12",
		"id_token_hint=abc and idle_token=abc",
		"the ghost wrote gh pull and ghp and gho_ and ghp_short in ghrelin",
		"ghp_" + strings.Repeat("a", 19),
		"github_pat_short and github_pat_",
		"xoxo, xox, xoxb and xoxb-short and xoxz-1234567890123456 and box-xoxb",
		"xoxb-" + strings.Repeat("a", 9),
		"AIza and AIzaShort and AKIA and AKIA_REGION_NAME and AKIAshort and AKIA" + strings.Repeat("A", 15),
		"Asia and ASIA and ASIAshort and ASIA_PACIFIC and xapp and xapp- and xapp-short and ASIA" + strings.Repeat("A", 15),
		"inside identifiers: abcxapp-1-aaaaaaaaaaaaaaaa abcASIAAAAAAAAAAAAAAAAAAAA",
		"aiza akia are not shapes in lower case: aizaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa akiaaaaaaaaaaaaaaaaaaaaa",
		"inside identifiers: abcxoxb-12345678901234 abcghp_aaaaaaaaaaaaaaaaaaaaaaaaa abcgithub_pat_aaaaaaaaaaaaaaaaaaaaaa abcAIzaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa abcAKIAAAAAAAAAAAAAAAAAAAA",
		"eyJhbGciOiJIUzI1NiJ9",
		"eyJ.abc.def and eyJabc..def and eyJ.eyJabc.def",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0",
		"eyJ.",
		"dGhpcyBpcyBub3QgYSBqd3QgYXQgYWxsLCBqdXN0IHNvbWUgYmFzZTY0IHRleHQ=",
		"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
		"v1.2.3 and example.com.au and a.b.c and header.payload.signature",
		"open keyJar.example.com.au failed",
		"pkg.loadKeyJSON.func1.1(0x1)",
		"surveyJS.min.js.map",
		"MoneyJar.co.uk",
		"heyJude.mp3.bak",
		"-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----",
		"-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----",
		"the private key is missing and private_key: the key is required",
		"private_key_id: abc and private_keys: 3",
	} {
		if result := RedactLogText(text); result != text {
			t.Errorf("RedactLogText(%q) = %q, want it unchanged", text, result)
		}
	}
}

// The rules run on every log line, so a long line with a prefix or a dot in it over and over is
// read in linear time: a quadratic rule would take minutes here. The race detector slows a scan
// past the bound, so the bound is checked in a build without it, which is what CI runs.
func TestRedactLogTextReadsALongLineInTime(t *testing.T) {
	if raceDetector {
		t.Skip("a wall-clock bound does not hold under the race detector")
	}
	for name, text := range map[string]string{
		"jwt starts":     strings.Repeat("eyJ.", 100000),
		"jwt parts":      "eyJ" + strings.Repeat("a.", 100000),
		"pem begins":     strings.Repeat(pemLine("BEGIN", ""), 20000),
		"slack prefixes": strings.Repeat("xoxb-", 100000),
		"json tokens":    strings.Repeat(`"token"  :  `, 50000),
	} {
		t.Run(name, func(t *testing.T) {
			started := time.Now()
			RedactLogText(text)
			require.Less(t, time.Since(started), 5*time.Second)
		})
	}
}

// A text with a shape's start and no keyword (a base64 body with eyJ in it) runs only the shaped
// rules, so it is read much faster than the same text with a keyword, which runs all of them. The
// bound is a ratio of the two, so it holds on a slow machine. The gate-off mutant runs at 0.8x, so
// a 2x bound still fails it; the best of 7 runs keeps a loaded runner's one slow sample from failing it.
func TestRedactLogTextSkipsTheKeywordRulesForATextWithOnlyAShapeStart(t *testing.T) {
	if raceDetector {
		t.Skip("a wall-clock bound does not hold under the race detector")
	}
	body := strings.Repeat("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo", 30000)
	shapeOnly := "eyJ" + body
	withKeyword := "key " + body
	fastest := func(text string) time.Duration {
		best := time.Hour
		for range 7 {
			started := time.Now()
			RedactLogText(text)
			best = min(best, time.Since(started))
		}
		return best
	}
	require.Less(t, fastest(shapeOnly)*2, fastest(withKeyword))
}

// A connector's error is logged as the router logs it, whole, with the token it echoed taken out
// and the rest of the line, and the counters beside it, as it was.
func TestRedactLogAttrTakesAConnectorTokenOutOfAnError(t *testing.T) {
	secret := rand.Text()
	err := fmt.Errorf("connector slack: call chat.postMessage: 401: %w", errors.New(`{"ok":false,"error":"invalid_auth","token":"`+secret+`"} sent xoxb-1234567890-`+secret))
	line := logged(t, true, "connector call failed", "error", err, "input_tokens", 123, "max_tokens", 256)
	require.NotContains(t, line, secret)
	require.Contains(t, line, "invalid_auth")
	require.Contains(t, line, "input_tokens=123 max_tokens=256")
}

// mayHoldCredential lets through every text a rule redacts: one row per rule and per word it
// needs, in any case, with the Kelvin sign and the long s the rules fold to k and s.
func TestEveryTextARuleRedactsPassesTheCheck(t *testing.T) {
	secret := rand.Text()
	for _, text := range []string{
		"sent BEARER " + secret,
		"h=bearer%20" + secret,
		"Proxy-Authorization: Basic " + secret,
		"Set-Cookie: session=" + secret,
		"h=Authorization%3A%20Basic%20" + secret,
		"h=COOKIE%3D" + secret,
		"x_api_key=" + secret,
		"ApiKey: " + secret,
		"refresh-token=" + secret,
		"ACCESS_TOKEN=" + secret,
		"client_SECRET=" + secret,
		`{"Password":"` + secret + `"}`,
		"wss://live.example/ws?KEY=" + secret,
		"key%3d" + secret,
		"Invalid API key " + secret,
		"https://ipinfo.io/json?Token=" + secret,
		"https://user:" + secret + "@example.com",
		"used sk-" + secret,
		"used xoxb-1234567890-" + secret,
		"used ghp_" + secret,
		"used gho_" + secret,
		"used ghu_" + secret,
		"used ghs_" + secret,
		"used ghr_" + secret,
		"used github_pat_" + secret,
		"used AIza" + secret + "abcdefghij",
		"used AKIA" + secret[:16],
		"used ASIA" + secret[:16],
		"used xoxe-1-" + secret,
		"used xapp-1-A0123456789-" + secret,
		"used eyJhbGciOiJIUzI1NiJ9.eyJ" + secret + "." + secret,
		"used " + pemLine("BEGIN", "") + secret + pemLine("END", ""),
		"https://ipinfo.io/json?to\u212aen=" + secret,
		"pa\u017f\u017fword=" + secret,
	} {
		if redactCredentials(text) == text {
			t.Errorf("no rule redacts %q: the row tests nothing", text)
		}
		if !mayHoldCredential(text) {
			t.Errorf("the check skips %q, which a rule redacts", text)
		}
	}
	if mayHoldCredential(`llmrouter: post "https://api.example.com/v1/chat/completions": context deadline exceeded`) {
		t.Error("the check lets a line with none of the words through")
	}
}

// The size cap is SafeLogText's, not the redaction's: 8192 bytes are kept whole, 8193 are cut to
// 2048 runes and a marker, and RedactLogText never cuts.
func TestSafeLogTextCapsWhatItKeeps(t *testing.T) {
	secret := rand.Text()
	atCap, overCap := strings.Repeat("x", 8192), strings.Repeat("x", 8193)
	long := overCap + "?key=" + secret
	for name, got := range map[string][2]string{
		"8192 bytes are kept":      {SafeLogText(atCap), atCap},
		"8193 bytes are cut":       {SafeLogText(overCap), strings.Repeat("x", 2048) + "… [truncated]"},
		"runes are cut, not bytes": {SafeLogText(strings.Repeat("é", 5000)), strings.Repeat("é", 2048) + "… [truncated]"},
		"redact never cuts":        {RedactLogText(long), overCap + "?key=[redacted]"},
	} {
		if got[0] != got[1] {
			t.Errorf("%s: got %d bytes, want %d", name, len(got[0]), len(got[1]))
		}
	}
	if strings.Contains(SafeLogText(long), secret) {
		t.Error("the cap path kept the credential")
	}
}

func TestSafeLogDetailsKeepsTheSFUKeys(t *testing.T) {
	details := safeLogDetails(map[string]any{
		"sfu_edge": "sfu-a1",
		"sfu_host": "sfu-a1.example.com",
		"token":    "secret-value",
	})
	if len(details) != 2 || details["sfu_edge"] != "sfu-a1" || details["sfu_host"] != "sfu-a1.example.com" {
		t.Fatalf("details = %v, want only the two SFU keys", details)
	}
}

func TestSafeLogDetailsKeepsTheCallPlacedKeys(t *testing.T) {
	details := safeLogDetails(map[string]any{
		"stream_sip_host": "trunk-1.sip.example.com",
		"vendor":          "twilio",
		"vendor_call_id":  "twilio-call",
		"token":           "secret-value",
	})
	if len(details) != 3 || details["stream_sip_host"] != "trunk-1.sip.example.com" ||
		details["vendor"] != "twilio" || details["vendor_call_id"] != "twilio-call" {
		t.Fatalf("details = %v, want only the three call placed keys", details)
	}
}

func TestSafeLogDetailsKeepsTheBridgeMediaKeys(t *testing.T) {
	details := safeLogDetails(map[string]any{
		"stream_media_addr":   "203.0.113.10",
		"customer_media_addr": "198.51.100.7",
		"token":               "secret-value",
	})
	if len(details) != 2 || details["stream_media_addr"] != "203.0.113.10" || details["customer_media_addr"] != "198.51.100.7" {
		t.Fatalf("details = %v, want only the two media addresses", details)
	}
}

// logged is what a text handler with RedactLogAttr, or with no ReplaceAttr when redact is
// false, prints for one record at a pinned time, so two runs of it can be compared byte for
// byte.
func logged(t *testing.T, redact bool, message string, args ...any) string {
	t.Helper()
	var out bytes.Buffer
	options := &slog.HandlerOptions{}
	if redact {
		options.ReplaceAttr = RedactLogAttr
	}
	record := slog.NewRecord(time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC), slog.LevelError, message, 0)
	record.Add(args...)
	if err := slog.NewTextHandler(&out, options).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// echoedKey is a provider's 502 that echoes the Authorization header it was sent, and the key
// in it.
func echoedKey() (error, string) {
	key := rand.Text()
	return errors.New("502 Bad Gateway: upstream error: failed to connect to local service\n" +
		"request headers: Authorization: Bearer sk-test-" + key), key
}

func TestRedactLogAttrKeepsAnErrorsReasonAndNotTheKeyItEchoed(t *testing.T) {
	err, key := echoedKey()
	line := logged(t, true, "agent failure", "context", "llm", "error", fmt.Errorf("gemma: %w", err))
	if strings.Contains(line, key) {
		t.Fatalf("the key survived: %s", line)
	}
	want := `error="gemma: 502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]"`
	if !strings.Contains(line, want) || !strings.Contains(line, "context=llm") {
		t.Fatalf("the line lost the rest of the error: %s", line)
	}
}

// A string attribute and the message are what a line holds when its caller logged err.Error()
// or formatted the error into the message.
func TestRedactLogAttrTakesTheKeyOutOfAStringAndTheMessage(t *testing.T) {
	err, key := echoedKey()
	line := logged(t, true, "refused: "+err.Error(), "reason", err.Error())
	if strings.Contains(line, key) {
		t.Fatalf("the key survived: %s", line)
	}
	if strings.Count(line, `failed to connect to local service\nrequest headers: [redacted]`) != 2 {
		t.Fatalf("the line lost the rest of the error: %s", line)
	}
}

// A line names the app's public key as api_key (api.NewLogHandler), which must stay readable
// while the same key's secret, echoed in a text, does not.
func TestRedactLogAttrKeepsAPublicKeyAndMasksItsSecret(t *testing.T) {
	key := "vak_test_" + rand.Text()[:24]
	secret := "vas_test_" + rand.Text()
	line := logged(t, true, "served a request", "api_key", key, "reason", "api_key="+secret)
	if !strings.Contains(line, "api_key="+key+" ") {
		t.Fatalf("the public key was masked: %s", line)
	}
	if strings.Contains(line, secret) {
		t.Fatalf("the secret survived: %s", line)
	}
}

func TestRedactLogAttrTakesTheKeyOutOfAGroup(t *testing.T) {
	err, key := echoedKey()
	line := logged(t, true, "agent failure", slog.Group("turn", "error", err))
	if strings.Contains(line, key) || !strings.Contains(line, `turn.error="502 Bad Gateway`) {
		t.Fatalf("the group kept the key or lost the error: %s", line)
	}
}

// An alert logs its details as a map, which can hold an error's text.
func TestRedactLogAttrTakesTheKeyOutOfAMapOfDetails(t *testing.T) {
	err, key := echoedKey()
	line := logged(t, true, "usage alert", "extra", map[string]any{"listing": err.Error()})
	if strings.Contains(line, key) || !strings.Contains(line, "listing:502 Bad Gateway") {
		t.Fatalf("the details kept the key or lost the error: %s", line)
	}
}

// The text handler prints a []byte as a string, so the key in a provider's body logged as
// bytes is taken out of that string.
func TestRedactLogAttrTakesTheKeyOutOfBytes(t *testing.T) {
	err, key := echoedKey()
	line := logged(t, true, "provider answered", "body", []byte(err.Error()))
	if strings.Contains(line, key) || !strings.Contains(line, `body="502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]"`) {
		t.Fatalf("the bytes kept the key or lost the body: %s", line)
	}
}

// The text handler prints an error as %+v, which for a stack.Wrap error is the error and its
// stack; the redacted error keeps the stack.
func TestRedactLogAttrKeepsTheStackOfAnErrorItRedacts(t *testing.T) {
	err, key := echoedKey()
	line := logged(t, true, "agent failure", "error", stack.Wrap(err))
	if strings.Contains(line, key) || !strings.Contains(line, "TestRedactLogAttrKeepsTheStackOfAnErrorItRedacts") {
		t.Fatalf("the redacted error kept the key or lost its stack: %s", line)
	}
}

// A log line has no cap: a long error is logged whole, as it was before, with only its key
// taken out.
func TestRedactLogAttrKeepsALongErrorWhole(t *testing.T) {
	err, key := echoedKey()
	long := fmt.Errorf("%s: %w", strings.Repeat("upstream said no. ", 600), err)
	line := logged(t, true, "agent failure", "error", long)
	if strings.Contains(line, key) || strings.Count(line, "upstream said no.") != 600 ||
		!strings.Contains(line, "request headers: [redacted]") {
		t.Fatalf("the long error kept the key or lost its text: %d bytes", len(line))
	}
}

// The control is the line a text handler with no ReplaceAttr prints, which is the line the
// router printed before RedactLogAttr: with nothing to redact, it is the same byte for byte.
func TestRedactLogAttrLeavesALineWithNoKeyAsItWas(t *testing.T) {
	args := []any{
		"error", stack.Wrap(errors.New("502 Bad Gateway: upstream error: failed to connect to local service")),
		"reason", "perplexity: PERPLEXITY_API_KEY is required",
		"tokens", 42,
		"took", 1500 * time.Millisecond,
		"extra", map[string]any{"day": "2026-11-06", "rows": 3},
		"payload", []byte(`{"type":"ping"}`),
	}
	want := logged(t, false, "agent failure", args...)
	if got := logged(t, true, "agent failure", args...); got != want {
		t.Fatalf("the line changed:\n got %s\nwant %s", got, want)
	}
}

// loggedJSON is what a JSON handler with RedactJSONLogAttr, or with no ReplaceAttr when redact
// is false, prints for one record at a pinned time.
func loggedJSON(t *testing.T, redact bool, message string, args ...any) string {
	t.Helper()
	var out bytes.Buffer
	options := &slog.HandlerOptions{}
	if redact {
		options.ReplaceAttr = RedactJSONLogAttr
	}
	record := slog.NewRecord(time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC), slog.LevelError, message, 0)
	record.Add(args...)
	if err := slog.NewJSONHandler(&out, options).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// hiddenText is an error whose %v and %+v show less than its Error(), which is what the JSON
// handler prints.
type hiddenText struct{ text string }

func (e hiddenText) Error() string                 { return e.text }
func (e hiddenText) Format(f fmt.State, verb rune) { fmt.Fprint(f, "provider failed") }

// The JSON handler prints an error's Error(): the key is taken out of that, though %+v hides
// it, and a stack.Wrap error stays its Error(), with no stack added.
func TestRedactJSONLogAttrReadsAnErrorAsTheHandlerPrintsIt(t *testing.T) {
	err, key := echoedKey()
	line := loggedJSON(t, true, "agent failure", "hidden", hiddenText{err.Error()}, "wrapped", stack.Wrap(err),
		"body", []byte(err.Error()), "extra", map[string]any{"listing": err.Error()})
	if strings.Contains(line, key) {
		t.Fatalf("the key survived: %s", line)
	}
	reason := `502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]`
	for _, want := range []string{`"hidden":"` + reason + `"`, `"wrapped":"` + reason + `"`, `"body":"` + reason + `"`, `listing`} {
		if !strings.Contains(line, want) {
			t.Fatalf("the line lost %s: %s", want, line)
		}
	}
}

// A message and a string attribute are redacted too, whatever their key.
func TestRedactJSONLogAttrRedactsAMessageAndAStringAttribute(t *testing.T) {
	err, key := echoedKey()
	line := loggedJSON(t, true, err.Error(), "reason", err.Error())
	if strings.Contains(line, key) {
		t.Fatalf("the key survived: %s", line)
	}
	if !strings.Contains(line, `"reason":"502 Bad Gateway`) {
		t.Fatalf("the line lost the reason: %s", line)
	}
}

// marshalsOtherText is an error whose Error() is clean and whose JSON is not: the handler
// prints the JSON.
type marshalsOtherText struct{ text string }

func (e marshalsOtherText) Error() string { return "provider failed" }
func (e marshalsOtherText) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.text)
}

func TestRedactJSONLogAttrRedactsWhatAMarshalingErrorPrints(t *testing.T) {
	err, key := echoedKey()
	line := loggedJSON(t, true, "agent failure", "error", marshalsOtherText{err.Error()})
	if strings.Contains(line, key) {
		t.Fatalf("the key survived: %s", line)
	}
}

func TestRedactJSONLogAttrLeavesALineWithNoKeyAsItWas(t *testing.T) {
	args := []any{
		"error", stack.Wrap(errors.New("502 Bad Gateway: upstream error: failed to connect to local service")),
		"reason", "perplexity: PERPLEXITY_API_KEY is required",
		"tokens", 42,
		"extra", map[string]any{"day": "2026-11-06", "rows": 3},
		"payload", []byte(`{"type":"ping"}`),
	}
	want := loggedJSON(t, false, "agent failure", args...)
	if got := loggedJSON(t, true, "agent failure", args...); got != want {
		t.Fatalf("the line changed:\n got %s\nwant %s", got, want)
	}
}

// A Sentry event carries an error's text in its message, its exceptions and its breadcrumbs.
func TestRedactSentryEventKeepsTheReasonAndNotTheKey(t *testing.T) {
	err, key := echoedKey()
	event := RedactSentryEvent(&sentry.Event{
		Message:     "router stopped: " + err.Error(),
		Exception:   []sentry.Exception{{Type: "*errors.errorString", Value: err.Error()}},
		Breadcrumbs: []*sentry.Breadcrumb{{Message: err.Error()}},
	}, nil)
	reason := "502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]"
	if event.Message != "router stopped: "+reason || event.Exception[0].Value != reason || event.Breadcrumbs[0].Message != reason {
		t.Fatalf("the event kept the key or lost the reason: %+v", event)
	}
	if event.Exception[0].Type != "*errors.errorString" || strings.Contains(fmt.Sprint(event.Breadcrumbs[0]), key) {
		t.Fatalf("the event changed what it should keep: %+v", event)
	}
}

func TestRedactSentryEventTakesTheKeyOutOfTheRequest(t *testing.T) {
	event := RedactSentryEvent(&sentry.Event{
		Request:     &sentry.Request{URL: "https://h/x?token=sk-secret123", QueryString: "token=sk-secret123&a=1"},
		Transaction: "GET /x?key=sk-secret123",
	}, nil)
	require.NotContains(t, event.Request.URL, "sk-secret123")
	require.NotContains(t, event.Request.QueryString, "sk-secret123")
	require.NotContains(t, event.Transaction, "sk-secret123")
	require.Contains(t, event.Request.QueryString, "a=1")
}

func TestRedactSentryEventTakesAKeyAtTheStartOfTheQueryOut(t *testing.T) {
	event := RedactSentryEvent(&sentry.Event{Request: &sentry.Request{QueryString: "token=abc123secret&a=1"}}, nil)
	require.NotContains(t, event.Request.QueryString, "abc123secret")
	require.Contains(t, event.Request.QueryString, "a=1")
}

// A usage alert carries a provider's error in a context (internal/alerts), and a tag can name
// what failed: every string in them is redacted, at any depth and in a value of any type, and
// the reason before the key is kept.
func TestRedactSentryEventTakesTheKeyOutOfContextsAndTags(t *testing.T) {
	err, key := echoedKey()
	reason := "502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]"
	type reading struct {
		Error string
		Rows  int64
	}
	event := RedactSentryEvent(&sentry.Event{
		Tags: map[string]string{"item": err.Error()},
		Contexts: map[string]sentry.Context{"usage_alert": {
			"reading": err.Error(),
			"nested":  map[string]any{"error": err.Error()},
			"listing": []any{err.Error()},
			"typed":   map[string][]reading{"apps": {{Error: err.Error(), Rows: 1<<60 + 1}}},
		}},
	}, nil)
	if sent, _ := json.Marshal(event); strings.Contains(string(sent), key) {
		t.Fatalf("the event Sentry is sent keeps the key: %s", sent)
	}
	context := event.Contexts["usage_alert"]
	if event.Tags["item"] != reason || context["reading"] != reason {
		t.Fatalf("a tag or a context kept the key or lost the reason: %+v", event)
	}
	if context["nested"].(map[string]any)["error"] != reason || context["listing"].([]any)[0] != reason {
		t.Fatalf("a nested map or slice kept the key or lost the reason: %+v", context)
	}
	typed, _ := json.Marshal(context["typed"])
	if want := `{"apps":[{"Error":` + strconv.Quote(reason) + `,"Rows":1152921504606846977}]}`; string(typed) != want {
		t.Fatalf("a typed value kept the key or lost the reason:\n got %s\nwant %s", typed, want)
	}
}

// The scope an event came from shares its contexts' nested values with the next event, so
// redacting one event leaves them as they were.
func TestRedactSentryEventLeavesTheScopesContextsAlone(t *testing.T) {
	err, key := echoedKey()
	shared := map[string]any{"error": err.Error()}
	listed := []any{err.Error()}
	context := sentry.Context{"nested": shared, "listing": listed}
	RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"scope": context}}, nil)
	if !strings.Contains(shared["error"].(string), key) || !strings.Contains(listed[0].(string), key) {
		t.Fatalf("the scope's own context was changed: %+v %+v", shared, listed)
	}
	if context["nested"].(map[string]any)["error"] != err.Error() {
		t.Fatalf("the context the event shares with its scope was changed: %+v", context)
	}
}

// A context that holds itself would overflow the stack, which ends the process where Sentry's
// own encoding of it only fails: the walk stops at its budget, and the strings it met are
// redacted as before.
func TestRedactSentryEventReturnsFromAContextThatHoldsItself(t *testing.T) {
	err, key := echoedKey()
	reason := "502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]"
	loop := map[string]any{"error": err.Error()}
	loop["self"] = loop
	listing := []any{err.Error()}
	listing = append(listing, listing)
	listing[1] = listing

	// Two events: the budget is one event's, so the loop must not spend the listing's.
	event := RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"usage_alert": {"loop": loop}}}, nil)
	listed := RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"usage_alert": {"listing": listing}}}, nil)

	context := event.Contexts["usage_alert"]
	if got := context["loop"].(map[string]any)["error"]; got != reason {
		t.Fatalf("a context that holds itself kept the key or lost the reason: %v", got)
	}
	if got := listed.Contexts["usage_alert"]["listing"].([]any)[0]; got != reason {
		t.Fatalf("a slice that holds itself kept the key or lost the reason: %v", got)
	}
	if strings.Contains(fmt.Sprint(context["loop"].(map[string]any)["error"]), key) {
		t.Fatal("the key is kept")
	}
}

// A context that holds itself twice has about 2^64 paths: the walk spends a budget of nodes on
// the event and returns, with the strings it met redacted.
func TestRedactSentryEventReturnsFromAContextThatHoldsItselfTwice(t *testing.T) {
	err, _ := echoedKey()
	reason := "502 Bad Gateway: upstream error: failed to connect to local service\nrequest headers: [redacted]"
	loop := map[string]any{"error": err.Error()}
	loop["a"] = loop
	loop["b"] = loop

	done := make(chan *sentry.Event)
	go func() {
		done <- RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"usage_alert": {"loop": loop}}}, nil)
	}()
	select {
	case event := <-done:
		if got := event.Contexts["usage_alert"]["loop"].(map[string]any)["error"]; got != reason {
			t.Fatalf("a context that holds itself twice kept the key or lost the reason: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("a context that holds itself twice did not return")
	}
}

// A container two paths reach, one deep and one shallow, is redacted on both: no walk
// remembers it as it was first met.
func TestRedactSentryEventRedactsAContainerOnEveryPathThatReachesIt(t *testing.T) {
	err, key := echoedKey()
	for range 20 {
		shared := map[string]any{"error": err.Error()}
		var deep any = shared
		for range 70 {
			deep = map[string]any{"in": deep}
		}

		event := RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{
			"c": {"deep": deep, "shallow": shared, "listed": []any{shared}},
		}}, nil)

		sent, marshalErr := json.Marshal(event.Contexts)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if strings.Contains(string(sent), key) {
			t.Fatalf("a container reached by two paths kept the key on one: %s", sent)
		}
	}
}

// A value nested far deeper than any context is, but within the budget, is kept whole.
func TestRedactSentryEventKeepsAValueNestedDeep(t *testing.T) {
	var deep any = map[string]any{"note": "plain", "n": []any{"a", 1}}
	for range 80 {
		deep = map[string]any{"in": deep}
	}

	event := RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"c": {"deep": deep}}}, nil)

	var at any = event.Contexts["c"]["deep"]
	for range 80 {
		at = at.(map[string]any)["in"]
	}
	leaf := at.(map[string]any)
	if leaf["note"] != "plain" || len(leaf["n"].([]any)) != 2 {
		t.Fatalf("what lies deep was lost: %+v", leaf)
	}
}

// What lies past the budget is left as it was, not dropped.
func TestRedactSentryEventKeepsWhatLiesPastTheBudget(t *testing.T) {
	rows := make([]any, 3*maxContextNodes)
	for i := range rows {
		rows[i] = map[string]any{"n": i}
	}

	event := RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"c": {"rows": rows}}}, nil)

	got := event.Contexts["c"]["rows"].([]any)
	if len(got) != len(rows) {
		t.Fatalf("rows lost: %d of %d", len(got), len(rows))
	}
	for i, row := range got {
		if row == nil {
			t.Fatalf("row %d past the budget was dropped", i)
		}
	}
}

// What an event says with no key in it reaches Sentry as it did before: the same text and
// the same values, of the same types. A typed value is the very value, not a copy of it.
func TestRedactSentryEventLeavesAnEventWithNoKeyAsItWas(t *testing.T) {
	type reading struct{ Rows int }
	typed := &reading{Rows: 3}
	event := RedactSentryEvent(&sentry.Event{
		Tags: map[string]string{"usage_alert": "sandbox_mismatch", "source": "router"},
		Contexts: map[string]sentry.Context{"usage_alert": {
			"day": "2026-11-06", "rows": 3, "increase": 1.5, "over": nil, "rerun": "router usage sandboxes --day 2026-11-06",
			"nested": map[string]any{"value": int64(1) << 60}, "listing": []any{"apps", true}, "typed": typed,
		}},
	}, nil)
	want := map[string]any{
		"day": "2026-11-06", "rows": 3, "increase": 1.5, "over": nil, "rerun": "router usage sandboxes --day 2026-11-06",
		"nested": map[string]any{"value": int64(1) << 60}, "listing": []any{"apps", true}, "typed": typed,
	}
	context := event.Contexts["usage_alert"]
	if fmt.Sprintf("%#v", map[string]any(context)) != fmt.Sprintf("%#v", want) || context["typed"] != any(typed) {
		t.Fatalf("the context changed:\n got %#v\nwant %#v", context, want)
	}
	if event.Tags["usage_alert"] != "sandbox_mismatch" || event.Tags["source"] != "router" || len(event.Tags) != 2 {
		t.Fatalf("the tags changed: %+v", event.Tags)
	}
}

func TestRedactSentryEventMasksANumber(t *testing.T) {
	event := RedactSentryEvent(&sentry.Event{
		Message:     "call from +15550001101 failed",
		Exception:   []sentry.Exception{{Value: "dial +15550001101: busy"}},
		Breadcrumbs: []*sentry.Breadcrumb{{Message: "ringing +15550001101"}},
		Tags:        map[string]string{"line": "+15550001101", "caller": "5550001101"},
		Contexts: map[string]sentry.Context{"call": {
			"caller": "5550001101",
			"to":     []any{"5550001101"},
			// Its first number changes its JSON, so it is decoded, and its second is masked under
			// the key it was decoded under.
			"from":   []string{"+15550001101", "5550001101"},
			"nested": map[string]any{"error": "no answer from +15550001101"},
			"typed":  map[string][]string{"to": {"+15550001101"}},
		}},
	}, nil)
	sent, err := json.Marshal(event)
	require.NoError(t, err)
	require.NotContains(t, string(sent), "5550001101")
	require.Equal(t, "call from +1•••01 failed", event.Message)
	require.Equal(t, "dial +1•••01: busy", event.Exception[0].Value)
	require.Equal(t, "ringing +1•••01", event.Breadcrumbs[0].Message)
	require.Equal(t, "+1•••01", event.Tags["line"])
	context := event.Contexts["call"]
	require.Equal(t, "•••01", event.Tags["caller"])
	require.Equal(t, "•••01", context["caller"])
	require.Equal(t, []any{"•••01"}, context["to"])
	require.Equal(t, []any{"+1•••01", "•••01"}, context["from"])
	require.Equal(t, "no answer from +1•••01", context["nested"].(map[string]any)["error"])
}

func TestRedactSentryEventMasksANumberValueAndRedactsTheValuesBesideIt(t *testing.T) {
	// The order a map is walked in is random, so each event has a new chance to walk the
	// number before the credential.
	key := rand.Text()
	for range 20 {
		event := RedactSentryEvent(&sentry.Event{Contexts: map[string]sentry.Context{"call": {
			"caller": int64(15550001101),
			"nested": map[string]any{"error": map[string]any{"text": "Authorization: Bearer sk-test-" + key}},
		}}}, nil)
		sent, err := json.Marshal(event.Contexts)
		require.NoError(t, err)
		require.NotContains(t, string(sent), key)
		require.NotContains(t, string(sent), "15550001101")
		require.Equal(t, "•••01", event.Contexts["call"]["caller"])
	}
}

// BenchmarkLogLine is the cost of RedactLogAttr on a router line with no key in it, typical and
// with a 4 KB error, against the same handler with no ReplaceAttr, which is what the router
// logged with before it.
func BenchmarkLogLine(b *testing.B) {
	typical := errors.New(`llmrouter: post "https://api.example.com/v1/chat/completions": context deadline exceeded`)
	long := errors.New(strings.Repeat("upstream said the model is overloaded, retry later; ", 80))
	for _, run := range []struct {
		name    string
		replace func([]string, slog.Attr) slog.Attr
		err     error
	}{
		{"typical/plain", nil, typical},
		{"typical/redacted", RedactLogAttr, typical},
		{"long/plain", nil, long},
		{"long/redacted", RedactLogAttr, long},
	} {
		b.Run(run.name, func(b *testing.B) {
			logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{ReplaceAttr: run.replace}))
			b.ReportAllocs()
			for b.Loop() {
				logger.Info("served a request", "request_id", "01HZX3Q9V8M2", "model", "openai/gpt-4o-mini",
					"status", 200, "duration", 312*time.Millisecond, "error", run.err)
			}
		})
	}
}

// pemLine is a PEM boundary line, built in parts: a secret scanner reads a whole one in a source file
// as a private key. kind is "BEGIN" or "END" and label the words before PRIVATE KEY, as "RSA ".
func pemLine(kind, label string) string {
	return "-----" + kind + " " + label + "PRIVATE KEY-----"
}
