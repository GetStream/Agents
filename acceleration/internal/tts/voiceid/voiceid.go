// Package voiceid checks a voice id against the shape of the vendor that will put it in a
// URL. The deployment's vendor accounts hold every customer's clones and the ownership check
// (store.VoiceBindings.Elsewhere) compares ids as exact strings, so an id that could reach a
// different resource than the one it names, or the same voice under another spelling, is
// refused before either sees it.
package voiceid

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrInvalid marks an id that is not one of its vendor's voice ids.
var ErrInvalid = errors.New("voiceid: not a voice id")

// elevenLabs is what an ElevenLabs voice id may hold: letters, digits, '_' and '-', up to
// 128. ElevenLabs publishes no format: the path parameter is a string, "ID of the voice to
// be used" (https://elevenlabs.io/docs/api-reference/voices/get, read 2026-11-24), and every
// id seen is 20 letters and digits (21m00Tcm4TlvDq8ikWAM there; the ids in
// voices.elevenLabsVoiceID). Each of these characters is unreserved in a URL path (RFC 3986
// §2.3) and '.' is left out, so the id stays one segment and cannot be a dot-segment. The
// 128 is our own bound, as the ticket's remediation and api.limitKeyID have it.
var elevenLabs = regexp.MustCompile(`^[0-9A-Za-z_-]{1,128}$`)

// cartesia is a Cartesia voice id: a UUID, hyphenated, in its canonical lowercase form.
// Cartesia's docs type the id as a string (https://docs.cartesia.ai/api-reference/voices/get,
// read 2026-11-24) and every id in them, in its answers and in our fixtures is one such as
// db6b0ed5-d5d3-463d-ae85-518a07d3c2b4. A UUID is read without regard to case (RFC 9562
// §4), so Canonical lowers the id before this rule sees it: an upper case twin of another
// customer's id is then the very string the ownership check holds. An unhyphenated twin is
// refused.
var cartesia = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// shapes is the rule per provider name, as tts/elevenlabs and tts/cartesia name themselves
// (this package sits below them, so it cannot import their ProviderName).
var shapes = map[string]*regexp.Regexp{
	"elevenlabs": elevenLabs,
	"cartesia":   cartesia,
}

// Canonical is the one spelling of a provider's voice id, or an error when the id is not one
// of the provider's. Every use of an id (the ownership check, a URL, a request body) takes
// this spelling, so they all see the same string. A provider with no rule here puts its voice
// in a request body or an escaped query, so any id passes as it is.
func Canonical(provider, id string) (string, error) {
	shape, ok := shapes[provider]
	if !ok {
		return id, nil
	}
	canonical := id
	if provider == "cartesia" {
		canonical = strings.ToLower(id)
	}
	if !shape.MatchString(canonical) {
		return "", stack.Wrap(fmt.Errorf("%w of %s: %q", ErrInvalid, provider, id))
	}
	return canonical, nil
}

// Check refuses an id that is not a voice id of the provider.
func Check(provider, id string) error {
	_, err := Canonical(provider, id)
	return err
}

// ElevenLabs refuses an id that is not an ElevenLabs voice id.
func ElevenLabs(id string) error { return Check("elevenlabs", id) }

// Cartesia is the canonical spelling of a Cartesia voice id, or an error for an id that is
// not one.
func Cartesia(id string) (string, error) { return Canonical("cartesia", id) }
