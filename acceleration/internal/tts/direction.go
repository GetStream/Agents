package tts

import "strings"

// directionLimit bounds how much text may be held back waiting for a bracketed direction
// to close. A model that writes a lone opening bracket and then talks for a paragraph must
// not leave the reader watching a blank screen while the stripper waits for a closing
// bracket that is never coming.
const directionLimit = 64

// tagLimit bounds how much text may be held back waiting for a tag to close. It is larger
// than directionLimit because a link's address is in the tag.
const tagLimit = 1024

// htmlTags are the tags taken out. A tag of any other name is said as written: a voice
// acts SSML such as <break> and <spell>, and "List<String>" or "<your name>" is text.
var htmlTags = map[string]bool{
	"a": true, "b": true, "i": true, "u": true, "s": true, "em": true, "strong": true,
	"p": true, "br": true, "hr": true, "div": true, "span": true, "ul": true, "ol": true,
	"li": true, "h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true,
	"code": true, "pre": true, "blockquote": true, "table": true, "thead": true,
	"tbody": true, "tfoot": true, "tr": true, "td": true, "th": true, "caption": true,
	"img": true, "sup": true, "sub": true, "small": true, "mark": true, "del": true,
	"ins": true,
}

// blockBreaks is what a block tag leaves between the words on either side of it: a new
// line where it ends a line, so the sentence ends there too, and a space otherwise.
var blockBreaks = map[string]string{
	"br": "\n", "p": "\n", "li": "\n",
	"div": " ", "h1": " ", "h2": " ", "h3": " ", "h4": " ", "h5": " ", "h6": " ",
	"tr": " ", "td": " ", "th": " ", "ul": " ", "ol": " ", "table": " ", "thead": " ",
	"tbody": " ", "tfoot": " ", "caption": " ", "blockquote": " ", "pre": " ", "hr": " ",
}

// Directions takes bracketed performance directions such as [laughs] back out of a
// streaming reply, and HTML tags such as <b>, </b> and the nameless </> with them.
//
// A voice that can act them wants them, but a transcript never does, and a voice that
// cannot act them would read them out. No voice acts HTML: each would read the tag out as
// symbols. Text arrives a few characters at a time, so a direction or a tag can be split
// across deltas: everything that cannot yet be one is released immediately, because
// whoever is reading is waiting for it.
//
// It strips in one pass: a tag that only forms once another is taken out, as in <<b>b>,
// is said as it is.
type Directions struct {
	// Acted keeps the directions in, for a voice that acts them. Tags still go.
	Acted bool
	// pending is text held back because it might be a direction or a tag that has not
	// closed.
	pending strings.Builder
	// written reports whether anything has been released yet in this reply.
	written bool
	// spaced reports whether the last character released ended a word.
	spaced bool
	// swallow drops the space that followed a direction lifted from between two words,
	// which would otherwise leave two where there was one. It outlives the delta the
	// direction closed in, because the space after it may not have arrived yet.
	swallow bool
	// gap is what a block tag left between words, written before the next of them so
	// that nothing is left at the end of a reply.
	gap string
}

// Add takes a delta of a reply and returns it without the directions and tags that closed
// in it.
func (d *Directions) Add(text string) string {
	d.pending.WriteString(text)
	return d.scan(false)
}

// Unperformed returns complete text without its directions and tags: what a voice would
// say of it rather than act.
func Unperformed(text string) string {
	var d Directions
	return d.Add(text) + d.Flush()
}

// Flush releases what is left at the end of a reply. Text held back for a direction or a
// tag that never closed was only ever text, but a tag after an unclosed bracket still goes.
func (d *Directions) Flush() string {
	kept := d.scan(true)
	d.Reset()
	return kept
}

// Reset forgets a reply, whether it finished or was abandoned part-way through.
func (d *Directions) Reset() {
	d.pending.Reset()
	d.written = false
	d.spaced = false
	d.swallow = false
	d.gap = ""
}

// scan releases what is pending up to what might still become a direction or a tag. At the
// end of a reply nothing more is coming, so nothing is held.
func (d *Directions) scan(final bool) string {
	var kept strings.Builder
	for {
		buffered := d.pending.String()
		if buffered == "" {
			break
		}

		opens := "[<"
		if d.Acted {
			opens = "<"
		}
		at := strings.IndexAny(buffered, opens)
		if at < 0 {
			d.release(&kept, buffered)
			d.reset("")
			break
		}
		d.release(&kept, buffered[:at])

		held := buffered[at:]
		end, gap, state := tagEnd(held)
		limit := tagLimit
		if held[0] == '[' {
			end, gap, state = directionEnd(held)
			limit = directionLimit
		}
		// Nothing can be decided until the rest of it arrives. A bracket that has gone on
		// too long to be a direction, or cannot open a tag, is just something the model said.
		if state == markIncomplete && !final && len(held) <= limit {
			d.reset(held)
			return kept.String()
		}
		if state != markClosed {
			d.release(&kept, held[:1])
			d.reset(held[1:])
			continue
		}

		if d.written && !d.spaced && gap != "" {
			d.gap = gap
		}
		if !d.written || d.spaced || gap != "" {
			d.swallow = true
		}
		d.reset(held[end:])
	}
	return kept.String()
}

// markState is what could be made of the text starting at a bracket.
type markState int

const (
	// markIncomplete means it may yet become a direction or a tag, once more text arrives.
	markIncomplete markState = iota
	// markAbsent means it cannot become one.
	markAbsent
	// markClosed means one was read, ending where its end says.
	markClosed
)

// directionEnd is where the direction text opens with ends.
func directionEnd(text string) (int, string, markState) {
	at := strings.IndexByte(text, ']')
	if at < 0 {
		return 0, "", markIncomplete
	}
	return at + 1, "", markClosed
}

// tagEnd is where the HTML tag text opens with ends, and what it leaves between words: <b>,
// </b>, <br/>, <a href="x">, or the nameless <> and </> that open and close a fragment.
// Each attribute has a value, so "a<b and c>d" opens no tag.
func tagEnd(text string) (int, string, markState) {
	at := 1
	if at < len(text) && text[at] == '/' {
		at++
	}
	start := at
	for at < len(text) && (isLetter(text[at]) || isDigit(text[at])) {
		at++
	}
	name := strings.ToLower(text[start:at])
	if at == len(text) {
		if couldName(name) {
			return 0, "", markIncomplete
		}
		return 0, "", markAbsent
	}
	if name == "" {
		if text[at] == '>' {
			return at + 1, "", markClosed
		}
		return 0, "", markAbsent
	}
	if !htmlTags[name] {
		return 0, "", markAbsent
	}
	gap := blockBreaks[name]
	for {
		if at == len(text) {
			return 0, "", markIncomplete
		}
		switch c := text[at]; {
		case c == '>':
			return at + 1, gap, markClosed
		case c == '/':
			if at+1 == len(text) {
				return 0, "", markIncomplete
			}
			if text[at+1] != '>' {
				return 0, "", markAbsent
			}
			return at + 2, gap, markClosed
		case isSpace(c):
			at++
			continue
		}
		next, state := attributeEnd(text, at)
		if state != markClosed {
			return 0, "", state
		}
		at = next
	}
}

// attributeEnd is where the attribute starting at at ends: a name, an equals sign and a
// value, quoted or not. A quoted value may hold a closing bracket, as in title="a>b".
func attributeEnd(text string, at int) (int, markState) {
	start := at
	for at < len(text) && (isLetter(text[at]) || isDigit(text[at]) || strings.IndexByte("-_:.", text[at]) >= 0) {
		at++
	}
	if at == start {
		return 0, markAbsent
	}
	at = skipSpaces(text, at)
	if at == len(text) {
		return 0, markIncomplete
	}
	if text[at] != '=' {
		return 0, markAbsent
	}
	at = skipSpaces(text, at+1)
	if at == len(text) {
		return 0, markIncomplete
	}
	if quote := text[at]; quote == '"' || quote == '\'' {
		closing := strings.IndexByte(text[at+1:], quote)
		if closing < 0 {
			return 0, markIncomplete
		}
		return at + closing + 2, markClosed
	}
	start = at
	for at < len(text) && !isSpace(text[at]) && strings.IndexByte("<>\"'`=", text[at]) < 0 {
		at++
	}
	if at == len(text) {
		return 0, markIncomplete
	}
	return at, markClosed
}

// couldName reports whether more letters might still make name one of htmlTags.
func couldName(name string) bool {
	for tag := range htmlTags {
		if strings.HasPrefix(tag, name) {
			return true
		}
	}
	return false
}

func skipSpaces(text string, at int) int {
	for at < len(text) && isSpace(text[at]) {
		at++
	}
	return at
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func isLetter(c byte) bool { return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' }

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

func (d *Directions) release(kept *strings.Builder, text string) {
	if d.swallow {
		text = strings.TrimLeft(text, " ")
	}
	if text == "" {
		return
	}
	d.swallow = false
	kept.WriteString(d.gap)
	d.gap = ""
	kept.WriteString(text)
	d.written = true
	d.spaced = strings.HasSuffix(text, " ") || strings.HasSuffix(text, "\n")
}

func (d *Directions) reset(text string) {
	d.pending.Reset()
	d.pending.WriteString(text)
}
