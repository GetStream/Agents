package tts

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type DirectionsSuite struct {
	suite.Suite
}

func TestDirectionsSuite(t *testing.T) {
	suite.Run(t, new(DirectionsSuite))
}

// stripped is the reply as a reader would see it, written one delta at a time.
func (s *DirectionsSuite) stripped(deltas ...string) string {
	var directions Directions
	var kept strings.Builder
	for _, delta := range deltas {
		kept.WriteString(directions.Add(delta))
	}
	kept.WriteString(directions.Flush())
	return kept.String()
}

func (s *DirectionsSuite) TestADirectionIsTakenOutOfTheLine() {
	s.Equal("That is a good one.", s.stripped("[laughs] That is a good one."))
}

func (s *DirectionsSuite) TestADirectionLeavesNoGapWhereItWas() {
	// Two spaces where a word was lifted out is audible: a voice reading it pauses.
	s.Equal("Well, fine.", s.stripped("Well, [sighs] fine."))
}

func (s *DirectionsSuite) TestADirectionSplitAcrossDeltasIsStillWhole() {
	s.Equal("Well fine.", s.stripped("Well ", "[sig", "hs]", " fine."))
}

func (s *DirectionsSuite) TestTextBeforeADirectionIsReleasedWithoutWaiting() {
	// Whoever is reading is waiting for it, so only what might be a direction is held.
	var directions Directions

	s.Equal("Hello there. ", directions.Add("Hello there. [wh"))
}

func (s *DirectionsSuite) TestABracketThatNeverClosesWasOnlyEverText() {
	s.Equal("The total is [1", s.stripped("The total is [1"))
}

func (s *DirectionsSuite) TestABracketTooLongToBeADirectionStopsBeingTreatedAsOne() {
	// Holding text back for a closing bracket that is never coming would leave the reader
	// watching nothing while the model talks.
	rambling := "[" + strings.Repeat("a", directionLimit+10)

	s.Equal(rambling, s.stripped(rambling))
}

func (s *DirectionsSuite) TestSeveralDirectionsInOneLineAllGo() {
	s.Equal("Fine. Really.", s.stripped("[sighs] Fine. [pause] Really."))
}

func (s *DirectionsSuite) TestALineWithNoDirectionsIsUntouched() {
	s.Equal("Hello there. How are you?", s.stripped("Hello there. ", "How are you?"))
}

func (s *DirectionsSuite) TestAStrayClosingTagIsTakenOut() {
	// A voice reads "</>" out as symbols.
	s.Equal("Let me see what happened.", s.stripped("Let me see what happened</>."))
	s.Equal("Let me see what happened ", s.stripped("Let me see what happened </>"))
}

func (s *DirectionsSuite) TestATagIsTakenOutAndWhatItWrapsIsKept() {
	s.Equal("Tap Save now.", s.stripped("Tap <b>Save</b> now."))
	s.Equal("Hi there.", s.stripped("<>Hi there.</>"))
	s.Equal("Title. Press Enter.", s.stripped("<h1>Title.</h1> Press <code>Enter</code>."))
	s.Equal("Line one. Line two, here.", s.stripped(`Line one. <br/>Line two, <a href="https://x.io">here</a>.`))
}

func (s *DirectionsSuite) TestATagIsTakenOutWhateverItsCase() {
	s.Equal("Tap Save now.", s.stripped("Tap <B>Save</B> now."))
	s.Equal("Tap Save now.", s.stripped(`Tap <A HREF="x">Save</A> now.`))
}

func (s *DirectionsSuite) TestATagWhoseAttributesFollowATabOrALineIsTakenOut() {
	s.Equal("See here.", s.stripped("See <a\thref=\"x\">here</a>."))
	s.Equal("See here.", s.stripped("See <a\n  href='x'\n>here</a>."))
}

func (s *DirectionsSuite) TestAnAngleBracketThatIsNoHTMLTagIsSaid() {
	// Only the HTML a model writes is taken out: a type, a placeholder, a key or an
	// address in brackets is something it means to say.
	for _, said := range []string{
		"Use a List<String> here.",
		"It returns vector<int>.",
		"Sign it <your name>.",
		"Press <Enter> to go on.",
		"Write <first>.<last> as the address.",
		"Go to <https://x.io/a>.",
		"If a<b and c>d it holds.",
		"Wrap it in a <ui> element.",
		"Then <b5> and <bold>.",
		"Skip <!-- this --> and <!DOCTYPE html>.",
		"Then <é> and <bé>.",
		"Use <a href> or <b =x>.",
	} {
		s.Equal(said, s.stripped(said))
	}
}

func (s *DirectionsSuite) TestAVoicesOwnTagsAreLeftForIt() {
	// Some voices act SSML, and an operator may ask the model for it.
	said := `Wait <break time="500ms"/> then <spell>ABC</spell>, <emotion value="happy"/>, ` +
		`<phoneme alphabet="ipa" ph="t">t</phoneme>, <say-as interpret-as="date">1/2</say-as> ` +
		`and <prosody rate="slow">slowly</prosody>.`

	s.Equal(said, s.stripped(said))
}

func (s *DirectionsSuite) TestABlockTagBetweenWordsLeavesABoundary() {
	// Glued together, two lines would be read as one word and the sentence would not end.
	s.Equal("Hello.\nWorld", s.stripped("Hello.<br>World"))
	s.Equal("Milk\nEggs", s.stripped("<ul><li>Milk</li><li>Eggs</li></ul>"))
	s.Equal("Hi\nThere.", s.stripped("<p>Hi</p><p>There.</p>"))
	s.Equal("One Two", s.stripped("<div>One</div><div>Two</div>"))
	s.Equal("Title Body", s.stripped("<h2>Title</h2>Body"))
	s.Equal("a b", s.stripped("<table><tr><td>a</td><td>b</td></tr></table>"))
}

func (s *DirectionsSuite) TestATagLeavesNoGapWhereItWas() {
	s.Equal("Tap Save now.", s.stripped("Tap <b> Save</b> now."))
}

func (s *DirectionsSuite) TestATagWithAnUnquotedValueIsTakenOut() {
	s.Equal("See here.", s.stripped("See <a href=x>here</a>."))
}

func (s *DirectionsSuite) TestASlashNotClosingATagOpensNone() {
	s.Equal("Use <br/x> here.", s.stripped("Use <br/x> here."))
}

func (s *DirectionsSuite) TestTextThatCanOpenNoHTMLTagIsReleasedWithoutWaiting() {
	var directions Directions

	s.Equal("Press <Ent", directions.Add("Press <Ent"))
}

func (s *DirectionsSuite) TestABoundaryLeftAtTheEndOfAReplyIsNotCarriedIntoTheNext() {
	var directions Directions
	directions.Add("Hi<br>")

	directions.Reset()

	s.Equal("There.", directions.Add("There."))
}

func (s *DirectionsSuite) TestABlockTagAfterASpaceLeavesNoGap() {
	s.Equal("Tap it now.", s.stripped("Tap it <br/> now."))
}

func (s *DirectionsSuite) TestALongLinkStreamedAByteAtATimeIsTakenOutWhole() {
	// A link's address can be long, and a quoted value can hold a closing bracket.
	link := `See <a href="https://x.io/` + strings.Repeat("p", 300) + `" title="a>b">the guide</a>.`
	var bytes []string
	for at := range len(link) {
		bytes = append(bytes, link[at:at+1])
	}

	s.Equal("See the guide.", s.stripped(link))
	s.Equal("See the guide.", s.stripped(bytes...))
}

func (s *DirectionsSuite) TestATagSplitInsideItsAttributesIsStillWhole() {
	s.Equal("See here.", s.stripped(`See <a hr`, `ef="x`, `" title`, `='y'`, `>here</a>.`))
}

func (s *DirectionsSuite) TestATagSplitAcrossDeltasIsStillWhole() {
	s.Equal("Tap Save now.", s.stripped("Tap ", "<", "b", ">Save<", "/", "b> now."))
	s.Equal("Done.", s.stripped("Done", "<", "/", ">."))
}

func (s *DirectionsSuite) TestAnAngleBracketThatOpensNoTagIsSaid() {
	said := "Yes, 3 < 5 and 7 > 2, a <= b, x<-y, I <3 you, <1>, <_x>."
	s.Equal(said, s.stripped(said))
	s.Equal("Is 3 < 5?", s.stripped("Is 3 ", "<", " 5?"))
}

func (s *DirectionsSuite) TestAnAngleBracketThatNeverClosesWasOnlyEverText() {
	s.Equal("It costs <", s.stripped("It costs <"))
	s.Equal("It costs </", s.stripped("It costs </"))
	s.Equal("Hold <b", s.stripped("Hold <b"))
	s.Equal("Hold <b and", s.stripped("Hold <b and"))
}

func (s *DirectionsSuite) TestABracketCutOffByALineOrAnotherBracketOpensNoTag() {
	s.Equal("If a <b c\nthen d> e.", s.stripped("If a <b c\nthen d> e."))
	s.Equal("If x <b y z.", s.stripped("If x <b y <i>z</i>."))
	s.Equal("If a <b, c>d.", s.stripped("If a <b, c>d."))
}

func (s *DirectionsSuite) TestAnAngleBracketTooLongToBeATagStopsBeingTreatedAsOne() {
	// Held back for a closing bracket that is never coming, it would leave the caller
	// waiting on silence while the model talks.
	rambling := `<a href="` + strings.Repeat("a", tagLimit+10)
	var directions Directions

	s.Equal(rambling, directions.Add(rambling))
}

func (s *DirectionsSuite) TestAnUnclosedBracketDoesNotShieldATagFromAVoiceThatActsOrNot() {
	// Whatever the voice, what it is sent comes to the same words as what is written, and
	// how the reply was cut into deltas does not change either.
	for _, reply := range []string{
		"[ hello <b>x</b> there",
		"Wait [ <br>then",
		"[laughs <i>no</i>",
		"<b>[laughs]</b> fine.",
		"Hi<br>[laughs] there.",
		"[sighs] <li>one</li><li>two</li>",
		"3 < 5 [x <spell>A</spell>",
	} {
		plain := Unperformed(reply)
		s.NotContains(plain, "<b>", reply)
		s.NotContains(plain, "<i>", reply)
		s.NotContains(plain, "<br>", reply)
		s.NotContains(plain, "<li>", reply)
		for cut := range len(reply) + 1 {
			s.Equal(plain, s.stripped(reply[:cut], reply[cut:]), reply)
			acted := Directions{Acted: true}
			performed := acted.Add(reply[:cut]) + acted.Add(reply[cut:]) + acted.Flush()
			s.Equal(plain, Unperformed(performed), reply)
		}
	}
}

func (s *DirectionsSuite) TestAVoiceThatActsDirectionsKeepsThemButLosesTheTags() {
	directions := Directions{Acted: true}

	kept := directions.Add("[laughs] Tap <b>Save") + directions.Add("</b> [sighs] now.") + directions.Flush()

	s.Equal("[laughs] Tap Save [sighs] now.", kept)
}

func (s *DirectionsSuite) TestAVoiceThatActsDirectionsStillLosesTheTagsAfterAReset() {
	directions := Directions{Acted: true}
	directions.Add("Hello <b")

	directions.Reset()

	s.Equal("[laughs] Bye.", directions.Add("[laughs] <i>Bye.</i>"))
}

func (s *DirectionsSuite) TestAReplyThatWasAbandonedIsForgotten() {
	var directions Directions
	directions.Add("Hello [wh")

	directions.Reset()

	s.Equal("Goodbye.", directions.Add("Goodbye."))
}
