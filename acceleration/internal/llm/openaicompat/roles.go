package openaicompat

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
)

// strictRoles shapes rendered messages for a chat template that takes one system message,
// first, and user and assistant turns that alternate (Options.StrictRoles). Later system
// messages are already the user's by then (SystemTurnsAsUser), and each call's results
// already follow it (ResultsFollowCalls).
//
// Mistral 7B v0.3's template, the strictest seen, counts only the user turns and the
// assistant turns without calls when it checks that they alternate, and wants a user turn
// first. So:
//
//   - a later system note is joined to the user turn before it rather than lifted into the
//     system prompt: the note stays where it was said, and the system prompt, the start of
//     every request's prefix, does not change from one turn to the next;
//   - a user turn that follows another with only calls and results between them is preceded
//     by an empty assistant turn, which is what happened: the model said nothing to the
//     results before the caller spoke again;
//   - what the model said just before its calls goes with them, as OpenAI itself sends a
//     reply that both speaks and calls, and two things it said in a row are one turn;
//   - what the model said before anybody else spoke is told to it in the system prompt, the
//     way the agent tells it a greeting it opened with.
func strictRoles(messages []openai.ChatCompletionMessageParamUnion) []openai.ChatCompletionMessageParamUnion {
	out := make([]openai.ChatCompletionMessageParamUnion, 0, len(messages)+1)
	for _, message := range messages {
		last := len(out) - 1
		switch {
		case message.OfSystem != nil && last >= 0 && out[last].OfSystem != nil:
			out[last] = openai.SystemMessage(joinText(systemText(out[last].OfSystem), systemText(message.OfSystem)))
		case message.OfUser != nil && last >= 0 && out[last].OfUser != nil:
			out[last] = joinUsers(out[last].OfUser, message.OfUser)
		case message.OfUser != nil && spoke(out) == roleUser:
			out = append(out, openai.AssistantMessage(""), message)
		case isAssistantText(message) && spoke(out) == "":
			out = opened(out, assistantText(message.OfAssistant))
		case isAssistantText(message) && last >= 0 && isAssistantText(out[last]):
			out[last] = openai.AssistantMessage(joinText(assistantText(out[last].OfAssistant), assistantText(message.OfAssistant)))
		case message.OfAssistant != nil && len(message.OfAssistant.ToolCalls) > 0:
			assistant := *message.OfAssistant
			said := ""
			if assistant.Content.OfString.Valid() {
				said = assistant.Content.OfString.Value
			}
			if last >= 0 && isAssistantText(out[last]) {
				said = joinText(assistantText(out[last].OfAssistant), said)
				out = out[:last]
			}
			// Always set, empty when nothing was said: OpenAI takes "" beside tool calls, and a
			// template that reads the content of every assistant turn finds one.
			if said != "" || !hasContent(&assistant) {
				assistant.Content = openai.ChatCompletionAssistantMessageParamContentUnion{OfString: param.NewOpt(said)}
			}
			out = append(out, openai.ChatCompletionMessageParamUnion{OfAssistant: &assistant})
		default:
			out = append(out, message)
		}
	}
	return out
}

const (
	roleUser      = "user"
	roleAssistant = "assistant"
)

// spoke is the role of the last turn a strict template counts when it checks that turns
// alternate: a user turn, or an assistant turn without calls. Empty when there is none yet.
func spoke(messages []openai.ChatCompletionMessageParamUnion) string {
	for i := len(messages) - 1; i >= 0; i-- {
		switch {
		case messages[i].OfUser != nil:
			return roleUser
		case isAssistantText(messages[i]):
			return roleAssistant
		}
	}
	return ""
}

// opened tells the model in the system prompt what it said before anybody else spoke.
func opened(messages []openai.ChatCompletionMessageParamUnion, said string) []openai.ChatCompletionMessageParamUnion {
	line := fmt.Sprintf("You opened the conversation by saying: %q", said)
	if len(messages) > 0 && messages[0].OfSystem != nil {
		messages[0] = openai.SystemMessage(joinText(systemText(messages[0].OfSystem), line))
		return messages
	}
	return append([]openai.ChatCompletionMessageParamUnion{openai.SystemMessage(line)}, messages...)
}

// isAssistantText reports whether a message is an assistant turn with no calls and with its
// content given as text.
func isAssistantText(message openai.ChatCompletionMessageParamUnion) bool {
	return message.OfAssistant != nil && len(message.OfAssistant.ToolCalls) == 0 &&
		len(message.OfAssistant.Content.OfArrayOfContentParts) == 0
}

// assistantText is an assistant turn's text.
func assistantText(message *openai.ChatCompletionAssistantMessageParam) string {
	if message.Content.OfString.Valid() {
		return message.Content.OfString.Value
	}
	return ""
}

// callIDLength is the length of the tool call ids Mistral's templates take, which also
// refuse any that is not alphanumeric. vLLM's Mistral parser makes ids this shape.
const callIDLength = 9

// shortCallIDs renames, in the request only, every tool call id that is not already
// callIDLength alphanumeric characters, in its call and in the results that answer it. The
// router's own ids (`call_…` for a call written as text, `…-tool-N` for one the provider
// sent unnamed) and another provider's, replayed after a fallback, are not that shape.
//
// The new id is a hash of the old, so the same history is sent the same way every turn and
// a cached prefix still matches. An id that is already the right shape keeps it, and one
// whose hash lands on an id already in the request is hashed again.
func shortCallIDs(messages []openai.ChatCompletionMessageParamUnion) {
	var ids []string
	for _, message := range messages {
		switch {
		case message.OfAssistant != nil:
			for _, call := range message.OfAssistant.ToolCalls {
				if call.OfFunction != nil {
					ids = append(ids, call.OfFunction.ID)
				}
			}
		case message.OfTool != nil:
			ids = append(ids, message.OfTool.ToolCallID)
		}
	}
	taken := map[string]bool{}
	for _, id := range ids {
		if shapedCallID(id) {
			taken[id] = true
		}
	}
	renamed := map[string]string{}
	for _, id := range ids {
		if shapedCallID(id) || renamed[id] != "" {
			continue
		}
		short := hashCallID(id, 0)
		for salt := 1; taken[short]; salt++ {
			short = hashCallID(id, salt)
		}
		taken[short] = true
		renamed[id] = short
	}
	if len(renamed) == 0 {
		return
	}
	for _, message := range messages {
		switch {
		case message.OfAssistant != nil:
			for _, call := range message.OfAssistant.ToolCalls {
				if call.OfFunction != nil && renamed[call.OfFunction.ID] != "" {
					call.OfFunction.ID = renamed[call.OfFunction.ID]
				}
			}
		case message.OfTool != nil && renamed[message.OfTool.ToolCallID] != "":
			message.OfTool.ToolCallID = renamed[message.OfTool.ToolCallID]
		}
	}
}

// shapedCallID reports whether an id is already one Mistral's templates take.
func shapedCallID(id string) bool {
	if len(id) != callIDLength {
		return false
	}
	for _, c := range id {
		if !strings.ContainsRune(callIDAlphabet, c) {
			return false
		}
	}
	return true
}

const callIDAlphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// hashCallID is callIDLength alphanumeric characters made from id and salt.
func hashCallID(id string, salt int) string {
	sum := sha256.Sum256([]byte(id + "\x00" + strconv.Itoa(salt)))
	n := binary.BigEndian.Uint64(sum[:8])
	short := make([]byte, callIDLength)
	for i := range short {
		short[i] = callIDAlphabet[n%uint64(len(callIDAlphabet))]
		n /= uint64(len(callIDAlphabet))
	}
	return string(short)
}

// joinUsers is two user turns in a row as one. Text joins text; a turn with images makes the
// joined turn a list of parts.
func joinUsers(first, second *openai.ChatCompletionUserMessageParam) openai.ChatCompletionMessageParamUnion {
	if first.Content.OfString.Valid() && second.Content.OfString.Valid() {
		return openai.UserMessage(joinText(first.Content.OfString.Value, second.Content.OfString.Value))
	}
	parts := append(userParts(first), userParts(second)...)
	return openai.UserMessage(parts)
}

// userParts is a user turn's content as a list of parts.
func userParts(message *openai.ChatCompletionUserMessageParam) []openai.ChatCompletionContentPartUnionParam {
	if message.Content.OfString.Valid() {
		if message.Content.OfString.Value == "" {
			return nil
		}
		return []openai.ChatCompletionContentPartUnionParam{openai.TextContentPart(message.Content.OfString.Value)}
	}
	return message.Content.OfArrayOfContentParts
}

// systemText is a system message's text, however its content was given.
func systemText(message *openai.ChatCompletionSystemMessageParam) string {
	if message.Content.OfString.Valid() {
		return message.Content.OfString.Value
	}
	texts := make([]string, 0, len(message.Content.OfArrayOfContentParts))
	for _, part := range message.Content.OfArrayOfContentParts {
		texts = append(texts, part.Text)
	}
	return strings.Join(texts, "\n\n")
}

// hasContent reports whether an assistant turn sends any content at all.
func hasContent(message *openai.ChatCompletionAssistantMessageParam) bool {
	return message.Content.OfString.Valid() || len(message.Content.OfArrayOfContentParts) > 0
}

// joinText puts two pieces of one turn together with a blank line between them, leaving out
// an empty one.
func joinText(first, second string) string {
	switch {
	case first == "":
		return second
	case second == "":
		return first
	}
	return first + "\n\n" + second
}
