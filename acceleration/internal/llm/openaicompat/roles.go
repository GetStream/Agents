package openaicompat

import (
	"strings"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
)

// strictRoles shapes rendered messages for a chat template that takes one system message,
// first, and user and assistant turns that alternate (Options.StrictRoles). Later system
// messages are already the user's by then (SystemTurnsAsUser).
//
// A later system note is joined to the user turn before it rather than lifted into the
// system prompt: the note stays where it was said, and the system prompt, the start of
// every request's prefix, does not change from one turn to the next.
func strictRoles(messages []openai.ChatCompletionMessageParamUnion) []openai.ChatCompletionMessageParamUnion {
	out := make([]openai.ChatCompletionMessageParamUnion, 0, len(messages))
	for _, message := range messages {
		last := len(out) - 1
		switch {
		case message.OfSystem != nil && last >= 0 && out[last].OfSystem != nil:
			out[last] = openai.SystemMessage(joinText(systemText(out[last].OfSystem), systemText(message.OfSystem)))
		case message.OfUser != nil && last >= 0 && out[last].OfUser != nil:
			out[last] = joinUsers(out[last].OfUser, message.OfUser)
		case message.OfAssistant != nil && len(message.OfAssistant.ToolCalls) > 0 && !hasContent(message.OfAssistant):
			assistant := *message.OfAssistant
			// Empty, not missing: OpenAI takes "" beside tool calls, and a template that reads
			// the content of every assistant turn finds one.
			assistant.Content = openai.ChatCompletionAssistantMessageParamContentUnion{OfString: param.NewOpt("")}
			out = append(out, openai.ChatCompletionMessageParamUnion{OfAssistant: &assistant})
		default:
			out = append(out, message)
		}
	}
	return out
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
