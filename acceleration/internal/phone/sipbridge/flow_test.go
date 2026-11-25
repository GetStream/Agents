package sipbridge

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func flowConfig() Config {
	cfg := validConfig()
	cfg.Logger = slog.New(slog.DiscardHandler)
	cfg.Call.RingTimeout = 50 * time.Millisecond
	cfg.CustomerTrunk.Codecs = []string{"PCMA", "PCMU"}
	cfg.trunkAddr = testTrunkAddr
	return cfg
}

// testTrunkAddr stands for the address the guard checked for the customer trunk.
var testTrunkAddr = netip.MustParseAddr("1.0.0.1")

var (
	customerOfferSDP    = buildSDP(Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv}, 1, 1)
	streamAnswerSDP     = buildSDP(Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv}, 2, 1)
	streamAnswerPCMASDP = buildSDP(Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}, 3, 1)
	customerPCMA        = buildSDP(Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}, 4, 1)
	customerPCMU        = buildSDP(Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, 5, 1)
	errBusy             = errors.New("486 Busy Here")
)

func mediaOf(t *testing.T, body []byte) Media {
	t.Helper()
	m, err := ParseMedia(body)
	require.NoError(t, err)
	return m
}

// Flow A

func TestFlowAConnectsCustomerOfferToStream(t *testing.T) {
	customer := &fakeLeg{inviteAnswer: customerOfferSDP}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

	media, err := flowA(t.Context(), flowConfig(), customer, stream)
	require.NoError(t, err)

	require.Equal(t, MediaAddrs{Customer: "198.51.100.7", Stream: "203.0.113.10"}, media)

	require.Equal(t, []string{"invite", "ack"}, customer.calls())
	require.Nil(t, customer.body(0))
	require.Equal(t, streamAnswerSDP, customer.body(1))
	require.Equal(t, []string{"invite", "ack"}, stream.calls())
	// The customer's address is a documentation one, not public, so Stream is sent the trunk's.
	require.Equal(t,
		Media{Addr: "1.0.0.1", Port: 30000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv},
		mediaOf(t, stream.body(0)))
}

func TestFlowAStreamGetsTheCustomerOfferRebuilt(t *testing.T) {
	offer := "v=0\r\no=- 4242 7 IN IP4 10.0.0.5\r\ns=-\r\nc=IN IP4 10.0.0.5\r\nt=0 0\r\n" +
		"m=video 30002 RTP/AVP 96\r\na=rtpmap:96 VP8/90000\r\n" +
		"m=audio 30000 RTP/AVP 111 97 0 101\r\na=rtpmap:111 opus/48000/2\r\na=rtpmap:97 AMR/8000\r\na=rtpmap:0 PCMU/8000\r\n" +
		"a=rtpmap:101 telephone-event/8000\r\na=rtcp:30001 IN IP4 10.0.0.5\r\n" +
		"a=candidate:1 1 UDP 2130706431 10.0.0.5 30000 typ host\r\na=x-extra:1\r\na=sendrecv\r\n"
	customer := &fakeLeg{inviteAnswer: []byte(offer)}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)
	require.NoError(t, err)

	want := "v=0\r\no=- 4242 7 IN IP4 1.0.0.1\r\ns=sipbridge\r\nc=IN IP4 1.0.0.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 111 0 101\r\na=rtpmap:111 opus/48000/2\r\na=rtpmap:0 PCMU/8000\r\n" +
		"a=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16\r\na=ptime:20\r\na=sendrecv\r\n"
	require.Equal(t, want, string(stream.body(0)))
	// The customer offered two streams, so its answer has two m= lines: the video one rejected.
	ack := string(customer.body(1))
	require.Regexp(t, `(?s)m=video 0 RTP/AVP 96\r\n.*m=audio 20000 RTP/AVP 0 101\r\n`, ack)
	require.Equal(t, 2, strings.Count(ack, "m="))
}

func TestFlowARefusesAnOfferStreamCannotSpeak(t *testing.T) {
	offer := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 97 101\r\na=rtpmap:97 AMR/8000\r\na=rtpmap:101 telephone-event/8000\r\n"
	customer := &fakeLeg{inviteAnswer: []byte(offer)}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "no codec Stream accepts")
	require.Empty(t, stream.calls())
	require.Equal(t, []string{"invite", "ack", "bye"}, customer.calls())
	require.Equal(t, 0, mediaOf(t, customer.body(1)).Port)
}

func TestFlowACustomerNotAnsweringNeverRingsStream(t *testing.T) {
	customer := &fakeLeg{inviteErr: errBusy}
	stream := &fakeLeg{}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorIs(t, err, errBusy)
	require.Empty(t, stream.calls())
}

func TestFlowARingTimeoutCancelsCustomer(t *testing.T) {
	customer := &fakeLeg{inviteBlocks: true}
	stream := &fakeLeg{}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, []string{"invite"}, customer.calls())
	require.Empty(t, stream.calls())
}

func TestFlowAStreamRejectingHangsUpAnsweredCustomer(t *testing.T) {
	customer := &fakeLeg{inviteAnswer: customerOfferSDP}
	stream := &fakeLeg{inviteErr: errors.New("404 Not Found")}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "404 Not Found")
	require.Equal(t, []string{"invite", "ack", "bye"}, customer.calls())
	ack := mediaOf(t, customer.body(1))
	require.Equal(t, 0, ack.Port)
	require.Equal(t, []Codec{pcmu, dtmfCodec}, ack.Codecs)
}

func TestFlowAFailedStreamAckHangsUpBoth(t *testing.T) {
	customer := &fakeLeg{inviteAnswer: customerOfferSDP}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP, ackErr: errors.New("broken pipe")}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "broken pipe")
	require.Equal(t, []string{"invite", "ack", "bye"}, customer.calls())
	ack := mediaOf(t, customer.body(1))
	require.Equal(t, 0, ack.Port)
	require.Equal(t, []Codec{pcmu, dtmfCodec}, ack.Codecs)
	require.Equal(t, "bye", stream.last())
}

// Flow B

func TestFlowBSetsUpStreamFirstThenRetargets(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteAnswer: customerPCMA}

	media, err := flowB(t.Context(), flowConfig(), customer, stream)
	require.NoError(t, err)

	require.Equal(t, MediaAddrs{Customer: "198.51.100.7", Stream: "203.0.113.10"}, media)
	require.Equal(t, []string{"invite", "ack", "reinvite", "reinvite"}, stream.calls())
	require.Equal(t, []string{"invite", "ack"}, customer.calls())

	require.Equal(t,
		Media{Addr: "192.0.2.1", Port: 20000, Codecs: []Codec{pcma, pcmu, dtmfCodec}, Direction: DirSendRecv},
		mediaOf(t, stream.body(0)))
	require.Equal(t,
		Media{Addr: "192.0.2.1", Port: 20000, Codecs: []Codec{pcma, pcmu, dtmfCodec}, Direction: DirInactive},
		mediaOf(t, stream.body(2)))
	require.Equal(t,
		Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv},
		mediaOf(t, customer.body(0)))
	// The customer answered with a documentation address, not public, so Stream is sent the trunk's.
	require.Equal(t,
		Media{Addr: "1.0.0.1", Port: 30000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv},
		mediaOf(t, stream.body(3)))
}

func TestFlowBPointsStreamAtAPublicCustomerAddress(t *testing.T) {
	for _, addr := range []string{"1.1.1.1", "2606:4700:4700::1111"} {
		stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
		answer := buildSDP(Media{Addr: addr, Port: 30000, Codecs: []Codec{pcma}, Direction: DirSendRecv}, 4, 1)
		customer := &fakeLeg{inviteAnswer: answer}

		media, err := flowB(t.Context(), flowConfig(), customer, stream)
		require.NoError(t, err)

		require.Equal(t, addr, media.Customer)
		require.Equal(t, addr, mediaOf(t, stream.body(3)).Addr)
	}
}

func TestFlowBTakesStreamsAddressFromItsLastAnswer(t *testing.T) {
	moved := buildSDP(Media{Addr: "203.0.113.20", Port: 20002, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}, 6, 1)
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP, reinviteAnswer: moved}
	customer := &fakeLeg{inviteAnswer: customerPCMA}

	media, err := flowB(t.Context(), flowConfig(), customer, stream)
	require.NoError(t, err)

	require.Equal(t, "203.0.113.20", media.Stream)
}

func TestFlowBFirstStreamInviteUsesTheConfiguredPlaceholder(t *testing.T) {
	cfg := flowConfig()
	cfg.FlowB.PlaceholderAddr = "0.0.0.0"
	cfg.FlowB.PlaceholderPort = 4000
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteAnswer: customerPCMA}

	_, err := flowB(t.Context(), cfg, customer, stream)
	require.NoError(t, err)

	first := mediaOf(t, stream.body(0))
	require.Equal(t, "0.0.0.0", first.Addr)
	require.Equal(t, 4000, first.Port)
}

func TestFlowBOffersToStreamShareOneSessionAndBumpTheVersion(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteAnswer: customerPCMA}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)
	require.NoError(t, err)

	origin := regexp.MustCompile(`(?m)^o=- (\d+) (\d+) IN IP4 `)
	var ids, versions []string
	for _, i := range []int{0, 2, 3} {
		match := origin.FindSubmatch(stream.body(i))
		require.NotNil(t, match, "no o= line in %q", stream.body(i))
		ids = append(ids, string(match[1]))
		versions = append(versions, string(match[2]))
	}
	require.Equal(t, []string{ids[0], ids[0], ids[0]}, ids)
	require.Equal(t, []string{"1", "2", "3"}, versions)
}

func TestFlowBUnansweredCustomerLeavesNothingInStream(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteBlocks: true}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, "bye", stream.last())
	require.Equal(t, []string{"invite"}, customer.calls())
}

func TestFlowBCustomerBusyHangsUpStream(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteErr: errBusy}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorIs(t, err, errBusy)
	require.Equal(t, "bye", stream.last())
}

func TestFlowBCustomerAnsweringWithAnotherCodecHangsUpBoth(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteAnswer: customerPCMU}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "only PCMA was offered")
	require.Equal(t, "bye", customer.last())
	require.Equal(t, "bye", stream.last())
}

func TestFlowBStreamRejectingStopsBeforeRinging(t *testing.T) {
	stream := &fakeLeg{inviteErr: errors.New("403 Forbidden")}
	customer := &fakeLeg{}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "403 Forbidden")
	require.Equal(t, []string{"invite"}, stream.calls())
	require.Empty(t, customer.calls())
}

func TestFlowBHoldRefusedHangsUpStreamBeforeRinging(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP, reinviteErr: errors.New("400 Bad Request")}
	customer := &fakeLeg{}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "400 Bad Request")
	require.Equal(t, "bye", stream.last())
	require.Empty(t, customer.calls())
}

func TestFlowBStreamRejectingRetargetHangsUpBoth(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP, reinviteErrs: []error{nil, errors.New("488 Not Acceptable Here")}}
	customer := &fakeLeg{inviteAnswer: customerPCMA}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "488 Not Acceptable Here")
	require.Equal(t, "bye", customer.last())
	require.Equal(t, "bye", stream.last())
}

func TestFlowBFailedCustomerAckHangsUpBoth(t *testing.T) {
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteAnswer: customerPCMA, ackErr: errors.New("network error")}

	_, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "network error")
	require.Equal(t, "bye", customer.last())
	require.Equal(t, "bye", stream.last())
}

func TestFlowAFailedCustomerAckHangsUpBoth(t *testing.T) {
	customer := &fakeLeg{inviteAnswer: customerOfferSDP, ackErr: errors.New("network error")}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "network error")
	require.Equal(t, "bye", customer.last())
	require.Equal(t, "bye", stream.last())
}

func TestFlowBCancelWhileRingingStillHangsUpStream(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteBlocks: true}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	_, err := flowB(ctx, flowConfig(), customer, stream)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, "bye", stream.last())
	// The BYE went out with a live cleanup context (nil error)
	require.Len(t, stream.byeCtxErrs, 1)
	require.Nil(t, stream.byeCtxErrs[0])
}

func sdpAt(addr string) []byte {
	return buildSDP(Media{Addr: addr, Port: 40000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, 1, 1)
}

func TestMediaNamesTheSideAndTheAddress(t *testing.T) {
	got := media(customerSide, sdpAt("203.0.113.10"))

	require.Subset(t, got, []any{"side", "customer", "addr", "203.0.113.10", "port", 40000, "private", false})
}

func TestMediaFlagsAPrivateAddress(t *testing.T) {
	for _, addr := range []string{"10.0.0.5", "100.64.1.1"} {
		got := media(streamSide, sdpAt(addr))

		require.Subset(t, got, []any{"private", true}, addr)
	}
}

func TestMediaOfAnUnreadableBodySaysSo(t *testing.T) {
	got := media(streamSide, []byte("garbage"))

	require.Contains(t, got, "sdp_error")
	require.Contains(t, got, "stream")
}

const t38Line = "m=image 30002 udptl t38\r\na=T38FaxVersion:0\r\n"

func TestFlowAConnectsAnOfferWithAFaxLineAudioOnly(t *testing.T) {
	offer := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\n" + t38Line
	customer := &fakeLeg{inviteAnswer: []byte(offer)}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.NoError(t, err)
	sent := string(stream.body(0))
	require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, []byte(sent)))
	require.NotContains(t, sent, "image")
	// The fax line is rejected under its own media type, in the order it was offered.
	require.Regexp(t, `(?s)m=audio 20000 RTP/AVP 0 101\r\n.*m=image 0 udptl t38\r\n`, string(customer.body(1)))
}

func TestFlowARefusesAnOfferWithOnlyAFaxLine(t *testing.T) {
	offer := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" + t38Line
	customer := &fakeLeg{inviteAnswer: []byte(offer)}
	stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

	_, err := flowA(t.Context(), flowConfig(), customer, stream)

	require.ErrorContains(t, err, "no media of type audio")
	require.Empty(t, stream.calls())
}

func TestFlowBConnectsACustomerAnswerWithAFaxLineAudioOnly(t *testing.T) {
	answer := "v=0\r\no=- 4 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 8\r\n" + t38Line
	stream := &fakeLeg{inviteAnswer: streamAnswerPCMASDP}
	customer := &fakeLeg{inviteAnswer: []byte(answer)}

	media, err := flowB(t.Context(), flowConfig(), customer, stream)

	require.NoError(t, err)
	require.Equal(t, "1.1.1.1", media.Customer)
	require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv},
		mediaOf(t, stream.body(3)))
}

func TestFlowAConnectsAnOfferWhateverItsLineEndings(t *testing.T) {
	for name, offer := range lineEndings {
		t.Run(name, func(t *testing.T) {
			customer := &fakeLeg{inviteAnswer: []byte(offer)}
			stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

			var err error
			require.NotPanics(t, func() { _, err = flowA(t.Context(), flowConfig(), customer, stream) })

			require.NoError(t, err)
			require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, stream.body(0)))
			requireAudioThenRejectedVideo(t, customer.body(1))
		})
	}
}

// otherLines are m= lines next to the audio that the SDP parser refuses on its own, by type, port or
// protocol. A call must still connect with audio only.
var otherLines = map[string]string{
	"application over udp": "m=application 30004 udp wb\r\n",
	"image over udptl":     "m=image 30002 udptl t38\r\n",
	"video port range":     "m=video 30002/x RTP/AVP 96\r\n",
}

func TestFlowAConnectsAnOfferWithALineTheParserRefusesAudioOnly(t *testing.T) {
	for name, line := range otherLines {
		t.Run(name, func(t *testing.T) {
			offer := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\nm=audio 30000 RTP/AVP 0\r\n" + line
			customer := &fakeLeg{inviteAnswer: []byte(offer)}
			stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

			_, err := flowA(t.Context(), flowConfig(), customer, stream)

			require.NoError(t, err)
			require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, stream.body(0)))
			fields := strings.Fields(strings.TrimPrefix(line, "m="))
			rejected := "m=" + fields[0] + " 0 " + strings.Join(fields[2:], " ") + "\r\n"
			require.Regexp(t, `(?s)m=audio 20000 RTP/AVP 0 101\r\n.*`+regexp.QuoteMeta(rejected), string(customer.body(1)))
		})
	}
}

func TestFlowAPassesOnAnOpusOrG729OfferRebuilt(t *testing.T) {
	const tail = "a=ptime:20\r\na=sendrecv\r\n"
	for name, tc := range map[string]struct{ media, want string }{
		"opus": {
			media: "m=audio 30000 RTP/AVP 111 101\r\na=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\n" +
				"a=rtpmap:101 telephone-event/48000\r\n",
			want: "m=audio 30000 RTP/AVP 111 101\r\na=rtpmap:111 opus/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\n" +
				"a=rtpmap:101 telephone-event/48000\r\na=fmtp:101 0-16\r\n" + tail,
		},
		"g729": {
			media: "m=audio 30000 RTP/AVP 18 101\r\na=rtpmap:18 G729/8000\r\na=fmtp:18 annexb=no\r\na=rtpmap:101 telephone-event/8000\r\n",
			want: "m=audio 30000 RTP/AVP 18 101\r\na=rtpmap:18 G729/8000\r\na=fmtp:18 annexb=no\r\n" +
				"a=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16\r\n" + tail,
		},
		"g729 by its static payload type": {
			media: "m=audio 30000 RTP/AVP 18\r\n",
			want:  "m=audio 30000 RTP/AVP 18\r\na=rtpmap:18 G729/8000\r\n" + tail,
		},
		"g729 by its static payload type with fmtp": {
			media: "m=audio 30000 RTP/AVP 18\r\na=fmtp:18 annexb=no\r\n",
			want:  "m=audio 30000 RTP/AVP 18\r\na=rtpmap:18 G729/8000\r\na=fmtp:18 annexb=no\r\n" + tail,
		},
		"fmtp Stream is not given": {
			media: "m=audio 30000 RTP/AVP 18\r\na=rtpmap:18 G729/8000\r\na=fmtp:18 annexb=no\"<x>\r\n",
			want:  "m=audio 30000 RTP/AVP 18\r\na=rtpmap:18 G729/8000\r\n" + tail,
		},
	} {
		t.Run(name, func(t *testing.T) {
			offer := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" + tc.media
			customer := &fakeLeg{inviteAnswer: []byte(offer)}
			stream := &fakeLeg{inviteAnswer: streamAnswerSDP}

			_, err := flowA(t.Context(), flowConfig(), customer, stream)

			require.NoError(t, err)
			want := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=sipbridge\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\n" + tc.want
			require.Equal(t, want, string(stream.body(0)))
		})
	}
}
