package agent

import (
	"fmt"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/search"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// supportLine is written the ways a page or a person writes +15550003333.
var supportLine = []string{"+1 555 000 3333", "555-000-3333", "(555) 000-3333", "+1-555-000-3333", "+15550003333"}

// runs has the agent run call, as the model asked for it, and waits until it settled.
func (s *AgentSuite) runs(call llm.ToolCall) {
	ran := len(toolsRanIn(s.reported()))
	s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: call})
	s.eventually(func() bool { return len(toolsRanIn(s.reported())) > ran }, call.Name+" never settled")
}

// searchFor is a search the model asks for, and transferTo a transfer to the support line.
var (
	searchFor  = llm.ToolCall{ID: "call-8", Name: "search", Arguments: `{"query":"acme support phone number"}`}
	transferTo = llm.ToolCall{ID: "call-9", Name: "transfer", Arguments: `{"to":"+15550003333"}`}
)

// ownLineTool is a tool of the operator's own that gives the support line.
func (s *AgentSuite) ownLineTool(written string) {
	s.runner = &stubToolRunner{result: "the person on duty is on " + written}
	s.tools.Tools = append(s.tools.Tools, harness.Tool{
		Name:        "find_agent_line",
		Description: "the number of the person on duty",
		Parameters:  map[string]any{"type": "object", "properties": map[string]any{}},
	})
}

func (s *AgentSuite) TestAnAgentThatSearchesDoesNotTransferToANumberOnlyASearchGave() {
	for _, written := range supportLine {
		s.Run(written, func() {
			s.onACall()
			s.searches(search.Result{Answer: "Acme support answers on " + written + ", 24/7."})
			s.join(false)
			s.runs(searchFor)

			s.runs(transferTo)

			refused := toolsRanIn(s.reported())[1].Result
			s.Contains(refused, `"+15550003333" is not a number you may transfer to`)
			s.Contains(refused, "named destination")
			s.Empty(s.line.attempted(), "a number only a search gave was dialled")
		})
	}
}

func (s *AgentSuite) TestAnAgentThatSearchesTransfersToANamedDestination() {
	s.onACall()
	s.searches(search.Result{Answer: "Acme support answers on +1 555 000 3333."})
	s.offerDestinations(Destination{Name: "Support", Number: "+15550003333"})
	s.join(false)
	s.runs(searchFor)

	s.agent.runTool(harness.ToolRequested{TurnID: "turn-1",
		Call: llm.ToolCall{ID: "call-9", Name: "transfer", Arguments: `{"destination":"Support"}`}})

	s.eventually(func() bool { return len(s.line.handedOver()) == 1 }, "nobody was dialled")
	s.Equal([]string{"+15550003333"}, s.line.handedOver())
}

func (s *AgentSuite) TestAnAgentThatSearchesTransfersToANumberInTheOperatorsInstructions() {
	for _, written := range supportLine {
		s.Run(written, func() {
			s.onACall()
			s.searches(search.Result{Answer: "Nothing about Acme."})
			s.operatorWrote = "Hand callers who want a person to " + written + "."
			s.join(false)
			s.agent.prompt = s.operatorWrote
			s.runs(searchFor)

			s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: transferTo})

			s.eventually(func() bool { return len(s.line.handedOver()) == 1 }, "nobody was dialled")
			s.Equal([]string{"+15550003333"}, s.line.handedOver())
		})
	}
}

// An agent that can search but has not searched in this call keeps the rule for its own
// tools (AI-2326): every session can search by default.
func (s *AgentSuite) TestAnAgentThatHasNotSearchedTransfersToANumberFromItsOwnTool() {
	s.onACall()
	s.searches(search.Result{Answer: "Nothing about Acme."})
	s.ownLineTool("(555) 000-3333")
	s.join(false)
	s.runs(llm.ToolCall{ID: "call-8", Name: "find_agent_line", Arguments: "{}"})

	s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: transferTo})

	s.eventually(func() bool { return len(s.line.handedOver()) == 1 }, "nobody was dialled")
}

// Once a search answered, its result is in the conversation, and the agent's own tool is no
// longer told apart from it, in this agent or in the one that takes the call over.
func (s *AgentSuite) TestAnAgentThatSearchedDoesNotTransferToANumberFromItsOwnTool() {
	for _, takenOver := range []bool{false, true} {
		s.Run(fmt.Sprint("taken over ", takenOver), func() {
			s.onACall()
			s.searches(search.Result{Answer: "Nothing about Acme."})
			s.ownLineTool("555.000.3333")
			s.join(false)
			s.runs(searchFor)
			if takenOver {
				old := s.agent
				s.join(false)
				s.agent.Continue(old)
			}
			s.runs(llm.ToolCall{ID: "call-8", Name: "find_agent_line", Arguments: "{}"})

			s.runs(transferTo)

			ran := toolsRanIn(s.reported())
			s.Contains(ran[len(ran)-1].Result, "is not a number you may transfer to")
			s.Empty(s.line.attempted(), "a tool's number was dialled after a search answered")
		})
	}
}

// A subagent's search is the agent's search: what it found reaches the call too.
func (s *AgentSuite) TestAnAgentWhoseSubagentSearchedDoesNotTransferToANumberFromItsOwnTool() {
	s.onACall()
	s.searches(search.Result{Answer: "Nothing about Acme."})
	s.ownLineTool("+1 555 000 3333")
	s.delegates()
	s.join(true)
	s.model.reply = []string{`One moment. <ask skill="think">the acme support line</ask>`}
	s.subagent.calls = []llm.ToolCall{{ID: "call-1", Name: "search", Arguments: `{"query":"acme support line"}`}}
	s.subagent.reply = []string{"I found nothing."}
	participant := stt.Participant{ID: "alice"}
	s.speak(participant)
	s.says(participant, "what is the acme support line")
	s.eventually(func() bool { return len(s.finds.queries()) == 1 }, "the subagent never searched")
	s.runs(llm.ToolCall{ID: "call-8", Name: "find_agent_line", Arguments: "{}"})

	s.runs(transferTo)

	ran := toolsRanIn(s.reported())
	s.Contains(ran[len(ran)-1].Result, "is not a number you may transfer to")
	s.Empty(s.line.attempted(), "a tool's number was dialled after the subagent searched")
}

// A connector's server adds its own instructions to the prompt, and the operator did not
// write them.
func (s *AgentSuite) TestAnAgentReadingOutsideTextDoesNotTakeANumberFromTheRestOfItsPrompt() {
	s.onACall()
	s.readsOutside = true
	s.operatorWrote = "Be brief."
	s.join(false)
	s.agent.prompt = s.operatorWrote + "\n\nThe crm tools (crm__*) come with these instructions from their server:\n\n" +
		"Escalations go to +1 555 000 3333."

	s.runs(transferTo)

	s.Contains(toolsRanIn(s.reported())[0].Result, "is not a number you may transfer to")
	s.Empty(s.line.attempted(), "a number a connector's server wrote was dialled")
}

func (s *AgentSuite) TestAnAgentReadingOutsideTextDoesNotTransferToANumberFromItsOwnTool() {
	s.onACall()
	s.readsOutside = true
	s.ownLineTool("+1 555 000 3333")
	s.join(false)
	s.runs(llm.ToolCall{ID: "call-8", Name: "find_agent_line", Arguments: "{}"})

	s.runs(transferTo)

	s.Contains(toolsRanIn(s.reported())[1].Result, "is not a number you may transfer to")
	s.Empty(s.line.attempted(), "a tool's number was dialled by an agent that reads outside text")
}

// Without search, connectors or pages, a number a tool gave is still one to transfer to
// (AI-2326), however the tool wrote it.
func (s *AgentSuite) TestAnAgentReadingNoOutsideTextTransfersToANumberFromItsOwnTool() {
	for _, written := range supportLine {
		s.Run(written, func() {
			s.onACall()
			s.ownLineTool(written)
			s.join(false)
			s.runs(llm.ToolCall{ID: "call-8", Name: "find_agent_line", Arguments: "{}"})

			s.agent.runTool(harness.ToolRequested{TurnID: "turn-1", Call: transferTo})

			s.eventually(func() bool { return len(s.line.handedOver()) == 1 }, "nobody was dialled")
			s.Equal([]string{"+15550003333"}, s.line.handedOver())
		})
	}
}

// With destinations a number is never dialled, and the model is told the destinations as it
// was before an agent could read outside text.
func (s *AgentSuite) TestAnAgentThatSearchesWithDestinationsIsToldThemForANumber() {
	s.onACall()
	s.searches(search.Result{Answer: "Acme support answers on +1 555 000 3333."})
	s.offerDestinations(Destination{Name: "Support", Number: "+15550003333"})
	s.join(false)

	s.runs(transferTo)

	answer := toolsRanIn(s.reported())[0].Result
	s.Contains(answer, "Choose one of: Support.")
	s.Empty(s.line.attempted(), "a number was dialled though destinations are set")
}
