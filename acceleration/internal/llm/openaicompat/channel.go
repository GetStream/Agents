package openaicompat

import "strings"

// Gemma 4 writes its thinking into the answer as a channel, `<|channel>thought\n…<channel|>`,
// and still writes an empty one with thinking off, mostly just before it calls a tool.
// vLLM's reasoning parser takes it out at the start of a reply but not after the model has
// already said something, and when it does take the tags it can leave the bare channel name
// behind. Either way a voice agent reads "thought" to the caller.
const (
	channelOpen  = "<|channel>"
	channelClose = "<channel|>"
	channelName  = "thought\n"
)

type channelState int

const (
	// channelLeading is the start of a reply: vLLM may have removed the channel tags and left a bare `thought` line
	// behind, so we drop it before scanning for a marker.
	channelLeading channelState = iota
	// channelText scans answer text for an opening `<|channel>` marker.
	channelText
	// channelHeader waits after `<|channel>` to strip a leading `thought\n` header across chunk boundaries.
	channelHeader
	// channelBody streams thinking text until `<channel|>`.
	channelBody
)

// channelSplitter moves Gemma's thought channel out of the answer as it streams. A marker
// can arrive split over several deltas, so the end of what it has seen is held back while
// it could still be the start of one.
type channelSplitter struct {
	state   channelState
	pending string
}

// Write takes the next piece of the answer and returns what of it is answer and what is
// thinking.
func (c *channelSplitter) Write(delta string) (text, thought string) {
	c.pending += delta
	var answer, thinking strings.Builder
	for {
		switch c.state {
		case channelLeading:
			name, undecided := bareChannelName(c.pending)
			if undecided {
				return answer.String(), thinking.String()
			}
			c.pending = c.pending[name:]
			c.state = channelText
		case channelText:
			start := strings.Index(c.pending, channelOpen)
			if start < 0 {
				keep := partialSuffix(c.pending, channelOpen)
				answer.WriteString(c.pending[:len(c.pending)-keep])
				c.pending = c.pending[len(c.pending)-keep:]
				return answer.String(), thinking.String()
			}
			answer.WriteString(c.pending[:start])
			c.pending = c.pending[start+len(channelOpen):]
			c.state = channelHeader
		case channelHeader:
			switch {
			case strings.HasPrefix(c.pending, channelName):
				c.pending = c.pending[len(channelName):]
				c.state = channelBody
			case strings.HasPrefix(channelName, c.pending):
				return answer.String(), thinking.String()
			default:
				c.state = channelBody
			}
		case channelBody:
			end := strings.Index(c.pending, channelClose)
			if end < 0 {
				keep := partialSuffix(c.pending, channelClose)
				thinking.WriteString(c.pending[:len(c.pending)-keep])
				c.pending = c.pending[len(c.pending)-keep:]
				return answer.String(), thinking.String()
			}
			thinking.WriteString(c.pending[:end])
			c.pending = c.pending[end+len(channelClose):]
			c.state = channelText
		}
	}
}

// Flush returns what was held back once the reply has ended: a marker that never finished
// is ordinary text, and a channel that never closed is still thinking.
func (c *channelSplitter) Flush() (text, thought string) {
	rest := c.pending
	c.pending = ""
	switch c.state {
	case channelLeading:
		c.state = channelText
		if name, _ := bareChannelName(rest + "\n"); name > 0 {
			return "", ""
		}
		return rest, ""
	case channelHeader, channelBody:
		c.state = channelText
		return "", rest
	default:
		return rest, ""
	}
}

// bareChannelName is how much of the start of s is a channel name vLLM left behind, with the
// whitespace around it, and whether more text could still make it one. After a tool result
// the name can come after a newline or a space, and before a space or a carriage return, as
// in "\nthought \r\n You're all set!": a reply that opens on the word and then says more,
// "thought about it", is not one.
func bareChannelName(s string) (n int, undecided bool) {
	const whitespace = " \t\r\n"
	word := strings.TrimSuffix(channelName, "\n")
	start := len(s) - len(strings.TrimLeft(s, whitespace))
	rest := s[start:]
	if len(rest) <= len(word) {
		return 0, strings.HasPrefix(word, rest)
	}
	if !strings.HasPrefix(rest, word) {
		return 0, false
	}
	after := rest[len(word):]
	// Gemma 4 also writes the name with a colon, "thought:[excited] You're all set!", and
	// nothing a reply opens with reads "thought:" to a caller.
	if strings.HasPrefix(after, ":") {
		n := start + len(word) + 1
		if gap := len(after[1:]) - len(strings.TrimLeft(after[1:], " \t\r")); strings.HasPrefix(after[1+gap:], "\n") {
			n += gap + 1
		}
		return n, false
	}
	spaces := len(after) - len(strings.TrimLeft(after, " \t\r"))
	switch {
	case spaces == len(after):
		return 0, true
	case after[spaces] == '\n':
		return start + len(word) + spaces + 1, false
	}
	return 0, false
}

// partialSuffix is the length of the longest end of s that is a proper start of marker.
func partialSuffix(s, marker string) int {
	for n := min(len(s), len(marker)-1); n > 0; n-- {
		if strings.HasSuffix(s, marker[:n]) {
			return n
		}
	}
	return 0
}
