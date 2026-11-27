package openaicompat

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type SplitterSuite struct {
	suite.Suite
}

func TestSplitterSuite(t *testing.T) {
	suite.Run(t, new(SplitterSuite))
}

func (s *SplitterSuite) splitToolCalls(tools []string, chunks []string) (text string, calls []string, unfinished string) {
	splitter := newToolCallSplitter(tools)
	var out strings.Builder
	for _, chunk := range chunks {
		t, c := splitter.Write(chunk)
		out.WriteString(t)
		calls = append(calls, c...)
	}
	t, c, unfinished := splitter.Flush()
	out.WriteString(t)
	calls = append(calls, c...)
	return out.String(), calls, unfinished
}

func (s *SplitterSuite) splitChannel(chunks []string) (text, thought string) {
	splitter := &channelSplitter{}
	var out, thinking strings.Builder
	for _, chunk := range chunks {
		t, th := splitter.Write(chunk)
		out.WriteString(t)
		thinking.WriteString(th)
	}
	t, th := splitter.Flush()
	out.WriteString(t)
	thinking.WriteString(th)
	return out.String(), thinking.String()
}

func singleByteChunks(input string) []string {
	chunks := make([]string, len(input))
	for i := range len(input) {
		chunks[i] = input[i : i+1]
	}
	return chunks
}

func (s *SplitterSuite) TestOrphanToolCallMarkerResetsAndDoesNotAcceptUnregisteredCalls() {
	input := "<|tool_call>some prose. Later call: unknown_tool(x=1) and call: word"
	tools := []string{"get_weather"}

	// Streaming check: after an orphan marker, "call: word" must not buffer until Flush.
	splitter := newToolCallSplitter(tools)
	streamed, streamedCalls := splitter.Write(input)
	s.Equal(input, streamed)
	s.Empty(streamedCalls)
	flushed, flushedCalls, unfinished := splitter.Flush()
	s.Empty(flushed)
	s.Empty(flushedCalls)
	s.Empty(unfinished)

	// Multi-byte and single-byte chunk splits.
	for _, chunks := range [][]string{
		{"<|tool_call>", "some prose. ", "Later call: unknown_tool(x=1)", " and call: word"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(input, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestOrphanToolCallMarkerDoesNotBufferSubsequentCallPrefixUntilFlush() {
	input := "<|tool_call>some prose. Later call: word"
	tools := []string{"get_weather"}

	splitter := newToolCallSplitter(tools)
	streamed, streamedCalls := splitter.Write(input)
	s.Equal(input, streamed)
	s.Empty(streamedCalls)
	flushed, flushedCalls, unfinished := splitter.Flush()
	s.Empty(flushed)
	s.Empty(flushedCalls)
	s.Empty(unfinished)
}

func (s *SplitterSuite) TestOrphanToolCallMarkerFollowedByInvalidCallPrefixResets() {
	input := "<|tool_call>call: not a call. Then call: unknown_tool{a:1}"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"<|tool_call>call:", " not a call. ", "Then call: unknown_tool{a:1}"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(input, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestOrphanToolCallMarkerAtEOFIsEmittedAsText() {
	tools := []string{"get_weather"}

	for _, input := range []string{"hello <|tool_call>", "hello <|tool_call>call:foo"} {
		for _, chunks := range [][]string{
			{input},
			singleByteChunks(input),
		} {
			text, calls, unfinished := s.splitToolCalls(tools, chunks)
			s.Equal(input, text)
			s.Empty(calls)
			s.Empty(unfinished)
		}
	}
}

func (s *SplitterSuite) TestMarkedToolCallWithUnregisteredNameStillWorksAfterOpenMarker() {
	input := "<|tool_call>call:unknown_tool{a:1}<tool_call|> done"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"<|tool_call>", "call:unknown_tool{a:1}", "<tool_call|> done"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(" done", text)
		s.Equal([]string{"unknown_tool{a:1}"}, calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestMarkedBareToolCallAfterOpenMarkerIsNotLoose() {
	tools := []string{"get_weather", "find", "find_room"}

	for _, tc := range []struct {
		input string
		call  string
	}{
		{"<|tool_call>get_weather{} done", "get_weather{}"},
		{"<|tool_call>get_weather(city=\"NYC\") done", "get_weather(city=\"NYC\")"},
		{"<|tool_call>find_room{} done", "find_room{}"},
	} {
		for _, chunks := range [][]string{
			{tc.input},
			singleByteChunks(tc.input),
		} {
			text, calls, unfinished := s.splitToolCalls(tools, chunks)
			s.Equal(" done", text)
			s.Equal([]string{tc.call}, calls)
			s.Empty(unfinished)
		}
	}
}

func (s *SplitterSuite) TestDoubleOpenMarkerEmitsFirstAsTextAndParsesSecond() {
	input := "<|tool_call><|tool_call>call:unknown_tool{a:1}<tool_call|>"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"<|tool_call>", "<|tool_call>call:unknown_tool{a:1}<tool_call|>"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal("<|tool_call>", text)
		s.Equal([]string{"unknown_tool{a:1}"}, calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestAdjacentBareCallsFollowedByProseAreAllEmittedAsText() {
	input := "get_weather{} get_weather{} is a tool"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"get_weather{} ", "get_weather{} ", "is a tool"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(input, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestAdjacentCallSpaceCallsFollowedByProseAreAllEmittedAsText() {
	input := "call get_weather{} call get_weather{} are examples"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"call get_weather{} ", "call get_weather{} ", "are examples"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(input, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestAdjacentBareCallsAtEOFAreAllConfirmedAsCalls() {
	input := "One moment. get_weather{} check_stock{}"
	tools := []string{"get_weather", "check_stock"}

	for _, chunks := range [][]string{
		{input},
		{"One moment. get_weather{} ", "check_stock{}"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal("One moment. ", text)
		s.Equal([]string{"get_weather{}", "check_stock{}"}, calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestAdjacentBareCallsConfirmedByCloseMarkerThenProse() {
	tools := []string{"get_weather", "check_stock"}

	for _, input := range []string{
		"get_weather{} check_stock{}<tool_call|> Done.",
		"get_weather{} check_stock{} <tool_call|> Done.",
	} {
		for _, chunks := range [][]string{
			{input},
			singleByteChunks(input),
		} {
			text, calls, unfinished := s.splitToolCalls(tools, chunks)
			s.Equal(" Done.", text)
			s.Equal([]string{"get_weather{}", "check_stock{}"}, calls)
			s.Empty(unfinished)
		}
	}
}

func (s *SplitterSuite) TestBareCallFollowedByMarkedCallConfirmsBoth() {
	tools := []string{"get_weather", "check_stock"}

	for _, input := range []string{
		"get_weather{} call:check_stock{} Done.",
		"get_weather{} <|tool_call>call:check_stock{}<tool_call|> Done.",
	} {
		for _, chunks := range [][]string{
			{input},
			singleByteChunks(input),
		} {
			text, calls, unfinished := s.splitToolCalls(tools, chunks)
			s.Equal(" Done.", text)
			s.Equal([]string{"get_weather{}", "check_stock{}"}, calls)
			s.Empty(unfinished)
		}
	}
}

func (s *SplitterSuite) TestBareCallFollowedByOrphanOpenMarkerAndProseEmitsAllAsText() {
	tools := []string{"get_weather"}

	for _, input := range []string{
		"get_weather{} <|tool_call>some prose",
		"get_weather{} <|tool_call>",
		"get_weather{} <|tool_call>call:foo",
	} {
		for _, chunks := range [][]string{
			{input},
			singleByteChunks(input),
		} {
			text, calls, unfinished := s.splitToolCalls(tools, chunks)
			s.Equal(input, text)
			s.Empty(calls)
			s.Empty(unfinished)
		}
	}
}

func (s *SplitterSuite) TestAdjacentBareCallFollowedByUnfinishedCallConfirmsFirstAndReportsUnfinished() {
	input := "get_weather{} check_stock{a: 1"
	tools := []string{"get_weather", "check_stock"}

	for _, chunks := range [][]string{
		{input},
		{"get_weather{} ", "check_stock{a: 1"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Empty(text)
		s.Equal([]string{"get_weather{}"}, calls)
		s.Equal("check_stock{a: 1", unfinished)
	}
}

func (s *SplitterSuite) TestOpenMarkerFollowedByOfferedToolPrefixWithoutBracketEmitsText() {
	input := "<|tool_call>check_stock_v2{} ok"
	tools := []string{"check_stock"}

	for _, chunks := range [][]string{
		{input},
		{"<|tool_call>", "check_stock", "_v2{} ok"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(input, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestOpenMarkerWithSpacesAfterCallPrefixEmitsTrimmedBody() {
	input := "<|tool_call>call: get_weather{}<tool_call|>"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"<|tool_call>call:", " get_weather{}<tool_call|>"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Empty(text)
		s.Equal([]string{"get_weather{}"}, calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestOpenMarkerFollowedBySpaceTreatsMarkerAsOrphanText() {
	tools := []string{"get_weather"}

	offered := "<|tool_call> call:get_weather{}<tool_call|>"
	for _, chunks := range [][]string{
		{offered},
		{"<|tool_call>", " call:get_weather{}<tool_call|>"},
		singleByteChunks(offered),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal("<|tool_call> ", text)
		s.Equal([]string{"get_weather{}"}, calls)
		s.Empty(unfinished)
	}

	unoffered := "<|tool_call> call:unknown_tool{a:1}<tool_call|>"
	for _, chunks := range [][]string{
		{unoffered},
		{"<|tool_call>", " call:unknown_tool{a:1}<tool_call|>"},
		singleByteChunks(unoffered),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(unoffered, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestOpenMarkerFollowedByNewlineTreatsMarkerAsOrphanText() {
	tools := []string{"get_weather"}

	offered := "<|tool_call>\ncall:get_weather{}<tool_call|>"
	for _, chunks := range [][]string{
		{offered},
		{"<|tool_call>", "\ncall:get_weather{}<tool_call|>"},
		singleByteChunks(offered),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal("<|tool_call>\n", text)
		s.Equal([]string{"get_weather{}"}, calls)
		s.Empty(unfinished)
	}

	unoffered := "<|tool_call>\ncall:unknown_tool{a:1}<tool_call|>"
	for _, chunks := range [][]string{
		{unoffered},
		{"<|tool_call>", "\ncall:unknown_tool{a:1}<tool_call|>"},
		singleByteChunks(unoffered),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Equal(unoffered, text)
		s.Empty(calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestBareCallFollowedByPartialCloseMarkerAtEOFConfirmsCall() {
	input := "get_weather{} <tool_"
	tools := []string{"get_weather"}

	for _, chunks := range [][]string{
		{input},
		{"get_weather{} ", "<tool_"},
		singleByteChunks(input),
	} {
		text, calls, unfinished := s.splitToolCalls(tools, chunks)
		s.Empty(text)
		s.Equal([]string{"get_weather{}"}, calls)
		s.Empty(unfinished)
	}
}

func (s *SplitterSuite) TestChannelHeaderSplitAcrossAnyBoundaryStripsThoughtHeader() {
	input := "Hi <|channel>thought\nsecret plan<channel|>Done"

	// Every 2-chunk split boundary, including right after "<|channel>" and "<|channel>tho".
	for i := 1; i < len(input); i++ {
		text, thought := s.splitChannel([]string{input[:i], input[i:]})
		s.Equal("Hi Done", text, "split at %d", i)
		s.Equal("secret plan", thought, "split at %d", i)
	}

	// Single-byte chunks.
	text, thought := s.splitChannel(singleByteChunks(input))
	s.Equal("Hi Done", text)
	s.Equal("secret plan", thought)
}

func (s *SplitterSuite) TestChannelWithoutThoughtHeaderKeepsBodyInThinking() {
	for _, tc := range []struct {
		input   string
		thought string
	}{
		{"Hi <|channel>other\nplan<channel|>Done", "other\nplan"},
		{"Hi <|channel>thoughtful<channel|>Done", "thoughtful"},
	} {
		for _, chunks := range [][]string{
			{tc.input},
			singleByteChunks(tc.input),
		} {
			text, thought := s.splitChannel(chunks)
			s.Equal("Hi Done", text)
			s.Equal(tc.thought, thought)
		}
	}
}

func (s *SplitterSuite) TestChannelHeaderTruncatedAtEOFGoesToThinking() {
	for _, tc := range []struct {
		input   string
		thought string
	}{
		{"Hi <|channel>tho", "tho"},
		{"Hi <|channel>", ""},
	} {
		for _, chunks := range [][]string{
			{tc.input},
			singleByteChunks(tc.input),
		} {
			text, thought := s.splitChannel(chunks)
			s.Equal("Hi ", text)
			s.Equal(tc.thought, thought)
		}
	}
}
