package sipbridge

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestCall(t *testing.T) (*Call, *fakeLeg, *fakeLeg) {
	customer, stream := &fakeLeg{}, &fakeLeg{}
	c := newCall(t.Context(), customer, stream, slog.New(slog.DiscardHandler))
	c.trunkAddr = testTrunkAddr
	c.establish()
	return c, customer, stream
}

func requireEnded(t *testing.T, c *Call) {
	t.Helper()
	select {
	case <-c.Done():
	default:
		t.Fatal("call has not ended")
	}
}

func TestByeFromCustomerEndsStreamLeg(t *testing.T) {
	c, customer, stream := newTestCall(t)

	c.onBye(t.Context(), customerSide)

	require.Equal(t, []string{"bye"}, stream.calls())
	require.Empty(t, customer.calls())
	requireEnded(t, c)
	require.NoError(t, c.Err())
}

func TestByeFromStreamEndsCustomerLeg(t *testing.T) {
	c, customer, stream := newTestCall(t)

	c.onBye(t.Context(), streamSide)

	require.Equal(t, []string{"bye"}, customer.calls())
	require.Empty(t, stream.calls())
	requireEnded(t, c)
}

func TestByeFromBothSidesAtOnceEndsCallOnce(t *testing.T) {
	c, customer, stream := newTestCall(t)

	var wg sync.WaitGroup
	for _, s := range []side{customerSide, streamSide} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.onBye(t.Context(), s)
		}()
	}
	wg.Wait()

	requireEnded(t, c)
	require.NoError(t, c.Err())
	// Exactly one bye should be sent across both legs
	totalByes := len(customer.calls()) + len(stream.calls())
	require.Equal(t, 1, totalByes, "expected exactly one bye sent, got %d", totalByes)
}

func TestAFailedByeStillEndsTheCall(t *testing.T) {
	c, _, stream := newTestCall(t)
	stream.byeErr = errors.New("connection reset")

	c.onBye(t.Context(), customerSide)

	requireEnded(t, c)
}

func TestReinviteFromStreamReachesTheCustomerAsItCameAndItsAnswerRebuilt(t *testing.T) {
	c, customer, _ := newTestCall(t)
	customer.reinviteAnswer = []byte("v=0\r\no=- 5 2 IN IP4 10.0.0.5\r\ns=-\r\nc=IN IP4 10.0.0.5\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\na=candidate:1 1 UDP 2130706431 10.0.0.5 30000 typ host\r\n")

	got, err := c.onReinvite(t.Context(), streamSide, streamAnswerSDP)

	require.NoError(t, err)
	require.Equal(t, []string{"reinvite"}, customer.calls())
	require.Equal(t, streamAnswerSDP, customer.body(0))
	require.Equal(t, Media{Addr: "1.0.0.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, got))
	require.NotContains(t, string(got), "10.0.0.5")
}

func TestReinviteFromStreamFailsWhenTheCustomerAnswerCannotBeRead(t *testing.T) {
	c, customer, _ := newTestCall(t)
	customer.reinviteAnswer = []byte("answer")

	_, err := c.onReinvite(t.Context(), streamSide, streamAnswerSDP)

	require.ErrorContains(t, err, "parse sdp")
}

func TestReinviteFromTheCustomerReachesStreamRebuilt(t *testing.T) {
	c, _, stream := newTestCall(t)
	stream.reinviteAnswer = streamAnswerSDP
	offer := "v=0\r\no=- 9 3 IN IP4 192.168.1.20\r\ns=-\r\nc=IN IP4 192.168.1.20\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\na=rtcp:30001 IN IP4 169.254.169.254\r\na=sendrecv\r\n" +
		"m=video 30002 RTP/AVP 96\r\na=rtpmap:96 VP8/90000\r\n"

	got, err := c.onReinvite(t.Context(), customerSide, []byte(offer))

	require.NoError(t, err)
	sent := string(stream.body(0))
	require.Equal(t, Media{Addr: "1.0.0.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, []byte(sent)))
	require.Contains(t, sent, "o=- 9 3 IN IP4 1.0.0.1\r\n")
	require.NotContains(t, sent, "169.254.169.254")
	require.NotContains(t, sent, "video")
	require.Regexp(t, `(?s)m=audio 20000 .*m=video 0 RTP/AVP 96\r\n`, string(got))
}

func TestReinviteFromTheCustomerThatCannotBeReadIsRefused(t *testing.T) {
	c, _, stream := newTestCall(t)

	_, err := c.onReinvite(t.Context(), customerSide, []byte("offer"))

	require.ErrorContains(t, err, "parse sdp")
	require.Empty(t, stream.calls())
}

func TestReinviteFromTheCustomerWithoutAnOfferAsksStreamForOne(t *testing.T) {
	c, _, stream := newTestCall(t)
	stream.reinviteAnswer = streamAnswerSDP

	got, err := c.onReinvite(t.Context(), customerSide, nil)

	require.NoError(t, err)
	require.Equal(t, streamAnswerSDP, got)
	require.Equal(t, []string{"reinvite"}, stream.calls())
	require.Empty(t, stream.body(0))
}

func TestInfoIsForwardedToTheOtherLeg(t *testing.T) {
	c, customer, _ := newTestCall(t)

	err := c.onInfo(t.Context(), streamSide, "application/dtmf-relay", []byte("Signal=5"))

	require.NoError(t, err)
	require.Equal(t, []string{"info:application/dtmf-relay"}, customer.calls())
	require.Equal(t, []byte("Signal=5"), customer.body(0))
}

func TestLostLegHangsUpTheOtherAndSaysWhy(t *testing.T) {
	c, customer, _ := newTestCall(t)

	c.lost(t.Context(), streamSide, errors.New("EOF"))

	require.Equal(t, []string{"bye"}, customer.calls())
	requireEnded(t, c)
	require.ErrorContains(t, c.Err(), "stream leg lost: EOF")
}

func TestHangupByesBothLegs(t *testing.T) {
	c, customer, stream := newTestCall(t)

	require.NoError(t, c.Hangup(t.Context()))

	require.Equal(t, []string{"bye"}, customer.calls())
	require.Equal(t, []string{"bye"}, stream.calls())
	requireEnded(t, c)
}

func TestHangupByesStreamWhileTheCustomerTrunkDoesNotAnswer(t *testing.T) {
	c, customer, stream := newTestCall(t)
	customer.byeBlocks = true
	ctx, cancel := context.WithCancel(t.Context())
	hungUp := make(chan error, 1)

	go func() { hungUp <- c.Hangup(ctx) }()

	require.Eventually(t, func() bool {
		return len(stream.calls()) == 1 && len(customer.calls()) == 1
	}, time.Second, time.Millisecond)
	require.Equal(t, []error{nil}, stream.byeContextErrs())
	select {
	case <-hungUp:
		t.Fatal("Hangup returned while the customer BYE was still waiting")
	default:
	}
	cancel()
	require.ErrorIs(t, <-hungUp, context.Canceled)
	requireEnded(t, c)
}

func TestHangupBoundsEachByeByItsOwnTimeout(t *testing.T) {
	c, customer, stream := newTestCall(t)
	var deadlines [2]time.Time
	customer.onBye = func(ctx context.Context) { deadlines[customerSide], _ = ctx.Deadline() }
	stream.onBye = func(ctx context.Context) { deadlines[streamSide], _ = ctx.Deadline() }
	start := time.Now()

	require.NoError(t, c.Hangup(t.Context()))

	for s, deadline := range deadlines {
		require.WithinDuration(t, start.Add(cleanupTimeout), deadline, time.Second, side(s))
	}
}

func TestNothingIsForwardedAfterTheCallEnded(t *testing.T) {
	c, customer, stream := newTestCall(t)
	c.onBye(t.Context(), customerSide)

	c.onBye(t.Context(), streamSide)

	require.Empty(t, customer.calls())
	require.Equal(t, []string{"bye"}, stream.calls())
}

func TestHangupAfterRemoteByeSendsNothing(t *testing.T) {
	c, customer, stream := newTestCall(t)

	c.onBye(t.Context(), customerSide)
	err := c.Hangup(t.Context())

	require.NoError(t, err)
	require.Equal(t, []string{"bye"}, stream.calls())
	require.Empty(t, customer.calls())
	requireEnded(t, c)
}

func TestReinviteAfterTheCallEndedIsRefused(t *testing.T) {
	c, customer, stream := newTestCall(t)
	c.onBye(t.Context(), customerSide)

	got, err := c.onReinvite(t.Context(), streamSide, []byte("offer"))

	require.ErrorIs(t, err, errCallEnded)
	require.Nil(t, got)
	// stream got the bye, customer should get nothing
	require.Equal(t, []string{"bye"}, stream.calls())
	require.Empty(t, customer.calls())
}

func TestInfoAfterTheCallEndedIsRefused(t *testing.T) {
	c, customer, stream := newTestCall(t)
	c.onBye(t.Context(), streamSide)

	err := c.onInfo(t.Context(), customerSide, "application/dtmf-relay", []byte("Signal=5"))

	require.ErrorIs(t, err, errCallEnded)
	// customer got the bye, stream should get nothing
	require.Equal(t, []string{"bye"}, customer.calls())
	require.Empty(t, stream.calls())
}

// setupCall is a call still being set up: Stream answered, but the customer leg is a real
// leg whose INVITE has no answer yet, so it has no dialog to send requests in.
func setupCall(t *testing.T) *Call {
	customer := &sipLeg{side: customerSide, log: slog.New(slog.DiscardHandler)}
	return newCall(t.Context(), customer, &fakeLeg{}, slog.New(slog.DiscardHandler))
}

func TestAReinviteDuringSetupIsAskedToWait(t *testing.T) {
	c := setupCall(t)
	forward := func(ctx context.Context, offer []byte) ([]byte, error) {
		return c.onReinvite(ctx, streamSide, offer)
	}

	res := answerReinvite(t.Context(), reinviteLeg(), reinviteFrom([]byte("v=0 hold\r\n")), forward)

	require.Equal(t, 491, res.StatusCode)
	require.Equal(t, "Request Pending", res.Reason)
}

func TestAnInfoDuringSetupIsRefused(t *testing.T) {
	c := setupCall(t)

	err := c.onInfo(t.Context(), streamSide, "application/dtmf-relay", []byte("Signal=5"))

	require.ErrorIs(t, err, errNotEstablished)
	code, reason := infoStatus(err)
	require.Equal(t, 500, code)
	require.Equal(t, "Server Internal Error", reason)
}

func TestAReinviteLogsTheAddressesBothWays(t *testing.T) {
	var buf bytes.Buffer
	customer := &fakeLeg{}
	stream := &fakeLeg{reinviteAnswer: sdpAt("198.51.100.20")}
	c := newCall(t.Context(), customer, stream, slog.New(slog.NewJSONHandler(&buf, nil)))
	c.trunkAddr = testTrunkAddr
	c.establish()

	_, err := c.onReinvite(t.Context(), customerSide, sdpAt("203.0.113.10"))

	require.NoError(t, err)
	logged := buf.String()
	require.Contains(t, logged, "203.0.113.10")
	require.Contains(t, logged, "198.51.100.20")
	require.Contains(t, logged, `"side":"customer"`)
	require.Contains(t, logged, `"side":"stream"`)
}

func TestReleaseEndsTheCallWithoutAByeOnEitherLeg(t *testing.T) {
	c, customer, stream := newTestCall(t)

	c.Release()

	requireEnded(t, c)
	require.ErrorIs(t, c.Err(), ErrReleased)
	require.Empty(t, customer.calls())
	require.Empty(t, stream.calls())
}

func TestAByeAfterReleaseIsNotPassedOn(t *testing.T) {
	c, _, stream := newTestCall(t)
	c.Release()

	c.onBye(t.Context(), customerSide)

	require.Empty(t, stream.calls())
}

func TestAPassedOnReinviteTellsOnChange(t *testing.T) {
	c, _, stream := newTestCall(t)
	stream.reinviteAnswer = sdpAt("198.51.100.20")
	changes := 0
	c.onChange = func() { changes++ }

	_, err := c.onReinvite(t.Context(), customerSide, sdpAt("203.0.113.10"))
	require.NoError(t, err)
	require.Equal(t, 1, changes)

	stream.reinviteErr = errors.New("refused")
	_, err = c.onReinvite(t.Context(), customerSide, sdpAt("203.0.113.11"))
	require.Error(t, err)
	require.Equal(t, 1, changes)
}

func TestACallStateHasBothLegsAndTheMedia(t *testing.T) {
	c, customer, stream := newTestCall(t)
	customer.state = LegState{CallID: "c"}
	stream.state = LegState{CallID: "s"}
	c.media = MediaAddrs{Customer: "203.0.113.10", Stream: "198.51.100.20"}

	got := c.State()

	require.Equal(t, "c", got.Customer.CallID)
	require.Equal(t, "s", got.Stream.CallID)
	require.Equal(t, MediaAddrs{Customer: "203.0.113.10", Stream: "198.51.100.20"}, got.Media)
}

func TestReinviteFromTheCustomerWithAFaxLineReachesStreamAudioOnly(t *testing.T) {
	c, _, stream := newTestCall(t)
	stream.reinviteAnswer = streamAnswerSDP
	offer := "v=0\r\no=- 9 3 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\n" + t38Line

	got, err := c.onReinvite(t.Context(), customerSide, []byte(offer))

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, stream.body(0)))
	require.NotContains(t, string(stream.body(0)), "image")
	require.Regexp(t, `(?s)m=audio 20000 .*m=image 0 udptl t38\r\n`, string(got))
}

func TestReinviteFromStreamAnsweredWithAFaxLineReachesStreamAudioOnly(t *testing.T) {
	c, customer, _ := newTestCall(t)
	customer.reinviteAnswer = []byte("v=0\r\no=- 5 2 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\n" + t38Line)

	got, err := c.onReinvite(t.Context(), streamSide, streamAnswerSDP)

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, got))
	require.NotContains(t, string(got), "image")
}
