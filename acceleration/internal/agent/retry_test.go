package agent

import (
	"fmt"
	"syscall"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// reset is a provider's stream dropped under it, which asked again may succeed.
var reset = fmt.Errorf("read tcp: %w", syscall.ECONNRESET)

// handedCount is how many responses the model has handed out, retries of one ID included.
func (s *AgentSuite) handedCount() int {
	s.model.mu.Lock()
	defer s.model.mu.Unlock()
	return len(s.model.handed)
}

func (s *AgentSuite) TestAReplyWhoseStreamDropsBeforeItBeganIsAskedAgain() {
	s.joinText()
	s.model.reply = nil
	turnID, err := s.agent.RespondTo(s.ctx, "what are your hours", nil, options.LLM{})
	s.Require().NoError(err)
	s.model.opened(turnID).Fail(reset, "read")

	s.eventually(func() bool { return s.handedCount() == 2 }, "the reply was never asked again")
	s.model.writes(turnID, "We open at nine.")
	s.model.finishes(turnID)
	s.eventually(func() bool { return countOf[Responded](s.reported()) == 1 }, "the retry never finished")

	turns := s.model.turns()
	s.Require().Len(turns, 2)
	s.Equal(turns[0].Input, turns[1].Input, "the retry asked something else")
	s.Equal([]string{"We open at nine."}, s.answered())
	s.Zero(countOf[Error](s.reported()), "a failure that was retried away was reported")
}

func (s *AgentSuite) TestAReplyWhoseStreamDropsPartWayIsNotAskedAgain() {
	s.joinText()
	s.model.reply = nil
	turnID, err := s.agent.RespondTo(s.ctx, "what are your hours", nil, options.LLM{})
	s.Require().NoError(err)
	s.model.writes(turnID, "We open ")
	s.eventually(func() bool { return countOf[ResponseDelta](s.reported()) > 0 }, "the reply never began")
	s.model.script(turnID).Fail(reset, "read")

	s.eventually(func() bool { return countOf[Error](s.reported()) == 1 }, "the failure was never reported")
	s.Equal(1, s.handedCount(), "a reply the caller had begun reading was asked again")
}

func (s *AgentSuite) TestAReplyThatFailsTwiceIsReportedFailed() {
	s.joinText()
	s.model.reply = nil
	turnID, err := s.agent.RespondTo(s.ctx, "what are your hours", nil, options.LLM{})
	s.Require().NoError(err)
	s.model.opened(turnID).Fail(reset, "read")
	s.eventually(func() bool { return s.handedCount() == 2 }, "the reply was never asked again")
	s.model.script(turnID).Fail(reset, "read")

	s.eventually(func() bool { return countOf[Error](s.reported()) == 1 }, "the second failure was never reported")
	s.Equal(2, s.handedCount(), "a reply was asked more than twice")
}

func (s *AgentSuite) TestAUnifiedReplyWhoseStreamDropsBeforeItBeganIsNotAskedAgain() {
	s.unified = true
	s.join(true)
	s.model.reply = nil
	participant := stt.Participant{ID: "alice"}
	s.speak(participant)

	s.says(participant, "hello")
	s.eventually(func() bool { return len(s.model.requests()) == 1 }, "the reply was never asked for")
	s.model.opened(s.model.requests()[0].ID).Fail(reset, "read")

	s.Never(func() bool { return s.handedCount() > 1 }, 200*time.Millisecond, 10*time.Millisecond,
		"a unified reply was asked again without a ruling on what it opens with")
}
