package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/channels"
)

type ChannelAnswerSuite struct{ suite.Suite }

func TestChannelAnswerSuite(t *testing.T) {
	suite.Run(t, new(ChannelAnswerSuite))
}

func (s *ChannelAnswerSuite) TestAChallengeIsEchoedAsPlainTextOnly() {
	recorder := httptest.NewRecorder()

	writeChannelAnswer(recorder, channels.Answer{Status: http.StatusOK, Text: "1158201444"})

	s.Equal(http.StatusOK, recorder.Code)
	s.Equal("1158201444", recorder.Body.String())
	s.Equal("text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	s.Equal("nosniff", recorder.Header().Get("X-Content-Type-Options"))
}
