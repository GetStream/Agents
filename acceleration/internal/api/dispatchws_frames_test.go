package api

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
)

// MessageFrameSuite covers the dispatch `message` frame as a worker reads it off the socket.
type MessageFrameSuite struct {
	suite.Suite
}

func TestMessageFrameSuite(t *testing.T) {
	suite.Run(t, new(MessageFrameSuite))
}

// written is the frame a worker is sent for one message, as the bytes on the wire.
func (s *MessageFrameSuite) written(message dispatch.Message) []byte {
	raw, err := json.Marshal(messageFrame(message))
	s.Require().NoError(err)
	return raw
}

// textOnly is a message with every field set but photos.
func textOnly() dispatch.Message {
	return dispatch.Message{
		WorkID: "work-1", ChannelType: "agent", ChannelID: "support-42", AgentID: "support-42",
		ConfigID: "config-1", SessionID: "session-1", CommandID: "request-1",
		Custom: map[string]string{"locale": "en-GB"}, Text: "the screen says error 4",
		MessageID: "message-1", UserID: "sam", UserName: "Sam",
		At: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
}

func (s *MessageFrameSuite) TestATextOnlyMessageIsWrittenAsItWasBeforePhotosWereCarried() {
	// Taken from messageFrame on c0d143595, before photos were carried: a worker that has
	// never heard of them must read the same bytes.
	s.Equal(`{"agent_id":"support-42","at":"2026-09-08T12:00:00Z","channel_id":"support-42",`+
		`"channel_type":"agent","config_id":"config-1","custom":{"locale":"en-GB"},`+
		`"message_id":"message-1","request_id":"request-1","session_id":"session-1",`+
		`"text":"the screen says error 4","type":"message","user_id":"sam","user_name":"Sam",`+
		`"work_id":"work-1"}`, string(s.written(textOnly())))
}

func (s *MessageFrameSuite) TestAnEmptyListOfPhotosIsLeftOutOfTheFrame() {
	message := textOnly()
	message.Images = []dispatch.Image{}

	s.Equal(string(s.written(textOnly())), string(s.written(message)))
}

func (s *MessageFrameSuite) TestThePhotosSentWithAMessageAreWrittenInOrder() {
	message := textOnly()
	message.Images = []dispatch.Image{
		{URL: "https://cdn.example/fault-code.jpg", MIME: "image/jpeg"},
		{URL: "https://cdn.example/control-panel.png"},
	}

	var fields map[string]json.RawMessage
	s.Require().NoError(json.Unmarshal(s.written(message), &fields))
	// The SDKs' tests read this same JSON (sdks/go/stream/dispatch_images_test.go): change
	// one and change the other.
	s.JSONEq(`[{"url":"https://cdn.example/fault-code.jpg","mime_type":"image/jpeg"},`+
		`{"url":"https://cdn.example/control-panel.png"}]`, string(fields["images"]))
	s.JSONEq(`"the screen says error 4"`, string(fields["text"]))
}

// LargeFrameSuite covers a frame a worker sends that is over the limit.
type LargeFrameSuite struct {
	suite.Suite
}

func TestLargeFrameSuite(t *testing.T) {
	suite.Run(t, new(LargeFrameSuite))
}

// named is what a frame of opening, 6 MiB of filler and closing names.
func (s *LargeFrameSuite) named(opening, filler, closing string) frameNamed {
	payload, large, err := workerFrameOf(strings.NewReader(opening + strings.Repeat(filler, 6<<20/len(filler)) + closing))
	s.Require().NoError(err)
	s.Nil(payload)
	s.Require().NotNil(large)
	return large.named()
}

func (s *LargeFrameSuite) TestANameIsFoundPastALongRunOfSpaces() {
	s.Equal(frameNamed{Type: "done", WorkID: "work-1"}, s.named(`{"type":"done",`, " \n\r\t", `"work_id":"work-1"}`))
}

func (s *LargeFrameSuite) TestANameIsFoundPastManyParts() {
	s.Equal(frameNamed{Type: "tool_result", ID: "call-1"},
		s.named(`{"type":"tool_result","output":[`, `{"type":"text","text":"0"},`, `{}],"id":"call-1"}`))
}

func (s *LargeFrameSuite) TestANameTooLongToBeOneIsNone() {
	s.Equal(frameNamed{Type: "tool_result"}, s.named(`{"type":"tool_result","id":"`, "a", `call-1"}`))
}

func (s *LargeFrameSuite) TestABackslashThatIsEscapedDoesNotEscapeTheEndOfItsString() {
	s.Equal(frameNamed{Type: "tool_result", ID: "call-1"},
		s.named(`{"output":"`, `\\`, `","type":"tool_result","id":"call-1"}`))
}

func (s *LargeFrameSuite) TestANameThatIsNoStringIsNoneAndTheRestAreFound() {
	s.Equal(frameNamed{Type: "tool_result", ToolCallID: "call-1"},
		s.named(`{"id":7,"output":"`, "a", `","type":"tool_result","tool_call_id":"call-1"}`))
}

func (s *LargeFrameSuite) TestWhatIsNotJSONNamesNothing() {
	s.Equal(frameNamed{}, s.named(`{"type":"done","work_id":"work-1","error":"`, "a", `"`))
}

// widest is a frame that says how much of itself was asked for at once.
type widest struct {
	io.Reader
	asked int
}

func (w *widest) Read(into []byte) (int, error) {
	w.asked = max(w.asked, len(into))
	return w.Reader.Read(into)
}

func (s *LargeFrameSuite) TestAFrameIsGatheredNoFurtherThanTheLimit() {
	frame := &widest{Reader: strings.NewReader(`{"error":"` + strings.Repeat("a", 64<<20) + `","type":"done"}`)}

	_, large, err := workerFrameOf(frame)

	s.Require().NoError(err)
	s.Equal("done", large.named().Type)
	s.LessOrEqual(frame.asked, 5<<20+1)
}

func (s *LargeFrameSuite) TestAFrameCutOffWithinTheLimitIsAFailedRead() {
	s.failsToRead(16)
}

func (s *LargeFrameSuite) TestAFrameCutOffPastTheLimitIsAFailedRead() {
	s.failsToRead(6 << 20)
}

func (s *LargeFrameSuite) failsToRead(after int) {
	cut := errors.New("cut off")

	_, _, err := workerFrameOf(io.MultiReader(strings.NewReader(strings.Repeat("a", after)), iotest.ErrReader(cut)))

	s.ErrorIs(err, cut)
}
