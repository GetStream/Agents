package sipbridge

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/pion/sdp/v3"
	"github.com/stretchr/testify/require"
)

var (
	pcmu = staticCodecs["PCMU"]
	pcma = staticCodecs["PCMA"]
)

func TestBuildSDPWritesTheWholeBody(t *testing.T) {
	m := Media{Addr: "192.0.2.1", Port: 20000, Codecs: []Codec{pcmu, pcma, dtmfCodec}, Direction: DirSendRecv}

	got := string(buildSDP(m, 42, 1))

	want := "v=0\r\n" +
		"o=- 42 1 IN IP4 192.0.2.1\r\n" +
		"s=sipbridge\r\n" +
		"c=IN IP4 192.0.2.1\r\n" +
		"t=0 0\r\n" +
		"m=audio 20000 RTP/AVP 0 8 101\r\n" +
		"a=rtpmap:0 PCMU/8000\r\n" +
		"a=rtpmap:8 PCMA/8000\r\n" +
		"a=rtpmap:101 telephone-event/8000\r\n" +
		"a=fmtp:101 0-16\r\n" +
		"a=ptime:20\r\n" +
		"a=sendrecv\r\n"
	require.Equal(t, want, got)
}

func TestParseMediaReadsWhatBuildSDPWrote(t *testing.T) {
	m := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirInactive}

	got, err := ParseMedia(buildSDP(m, 7, 3))

	require.NoError(t, err)
	require.Equal(t, m, got)
}

func TestParseMediaPrefersTheMediaLevelAddress(t *testing.T) {
	body := "v=0\r\no=- 1 1 IN IP4 10.0.0.1\r\ns=-\r\nc=IN IP4 10.0.0.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\nc=IN IP4 198.51.100.7\r\n"

	got, err := ParseMedia([]byte(body))

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, got)
}

func TestParseMediaKnowsStaticCodecsWithoutRtpmap(t *testing.T) {
	body := "v=0\r\no=- 1 1 IN IP4 198.51.100.7\r\ns=-\r\nc=IN IP4 198.51.100.7\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 9 8\r\n"

	got, err := ParseMedia([]byte(body))

	require.NoError(t, err)
	require.Equal(t, []Codec{staticCodecs["G722"], pcma}, got.Codecs)
}

func TestParseMediaRefusesABodyWithoutAudio(t *testing.T) {
	_, err := ParseMedia([]byte("v=0\r\no=- 1 1 IN IP4 203.0.113.9\r\ns=-\r\nt=0 0\r\n"))

	require.ErrorContains(t, err, "no media")
}

func TestSDPSessionKeepsItsIDAndBumpsTheVersion(t *testing.T) {
	s := &sdpSession{id: 99}
	m := Media{Addr: "192.0.2.1", Port: 20000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}

	first, second := string(s.build(m)), string(s.build(m))

	require.Contains(t, first, "o=- 99 1 IN IP4 192.0.2.1\r\n")
	require.Contains(t, second, "o=- 99 2 IN IP4 192.0.2.1\r\n")
}

func TestPlaceholderOfferPointsNowhere(t *testing.T) {
	got := placeholderOffer("192.0.2.1", 20000, []Codec{pcmu, pcma})

	require.Equal(t, Media{Addr: "192.0.2.1", Port: 20000, Codecs: []Codec{pcmu, pcma, dtmfCodec}, Direction: DirSendRecv}, got)
}

func TestHoldOfferOnlyChangesDirection(t *testing.T) {
	offer := placeholderOffer("192.0.2.1", 20000, []Codec{pcmu})

	got := holdOffer(offer)

	want := offer
	want.Direction = DirInactive
	require.Equal(t, want, got)
	require.Equal(t, DirSendRecv, offer.Direction)
}

func TestCustomerOfferUsesStreamsAddressAndOnlyItsCodec(t *testing.T) {
	stream := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}

	got, err := customerOffer(stream)

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}, got)
}

func TestCustomerOfferDropsDTMFStreamDidNotAccept(t *testing.T) {
	stream := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}

	got, err := customerOffer(stream)

	require.NoError(t, err)
	require.Equal(t, []Codec{pcmu}, got.Codecs)
}

func TestCustomerOfferRefusesAnAnswerWithoutAudio(t *testing.T) {
	_, err := customerOffer(Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{dtmfCodec}})

	require.ErrorContains(t, err, "no audio codec")
}

func TestRetargetPointsStreamAtTheCustomer(t *testing.T) {
	offered := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}
	customer := Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcma}, Direction: DirSendRecv}

	got, err := retargetOffer(customer, offered)

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcma, dtmfCodec}, Direction: DirSendRecv}, got)
}

func TestRetargetRefusesAnotherCodec(t *testing.T) {
	offered := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma}, Direction: DirSendRecv}
	customer := Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}

	_, err := retargetOffer(customer, offered)

	require.ErrorContains(t, err, "PCMA")
}

func TestRetargetRefusesRejectedAudio(t *testing.T) {
	offered := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcma}, Direction: DirSendRecv}
	customer := Media{Addr: "198.51.100.7", Port: 0, Codecs: []Codec{pcma}, Direction: DirSendRecv}

	_, err := retargetOffer(customer, offered)

	require.ErrorContains(t, err, "rejected the audio")
}

func TestRejectAnswerClosesTheStream(t *testing.T) {
	offer := Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv}

	got := rejectAnswer(offer)

	require.Equal(t, Media{Addr: "192.0.2.1", Port: 0, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirInactive}, got)
}

func TestRejectAnswerStillNamesAFormatWhenTheOfferHadNone(t *testing.T) {
	got := rejectAnswer(Media{})

	require.Equal(t, []Codec{pcmu}, got.Codecs)
}

func TestParseMediaNormalisesCodecNames(t *testing.T) {
	body := "v=0\r\no=- 1 1 IN IP4 192.0.2.1\r\ns=-\r\nc=IN IP4 192.0.2.1\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0 101\r\n" +
		"a=rtpmap:0 pcmu/8000\r\n" +
		"a=rtpmap:101 Telephone-Event/8000\r\n"

	got, err := ParseMedia([]byte(body))

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "192.0.2.1", Port: 30000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv}, got)
}

func TestRetargetSucceedsWithLowercaseCodecInAnswer(t *testing.T) {
	offered := Media{Addr: "203.0.113.10", Port: 20000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv}
	// Customer answer parsed from SDP with lowercase "pcmu"
	customerSDP := "v=0\r\no=- 1 1 IN IP4 198.51.100.7\r\ns=-\r\nc=IN IP4 198.51.100.7\r\nt=0 0\r\n" +
		"m=audio 30000 RTP/AVP 0\r\n" +
		"a=rtpmap:0 pcmu/8000\r\n"
	customer, err := ParseMedia([]byte(customerSDP))
	require.NoError(t, err)

	got, err := retargetOffer(customer, offered)

	require.NoError(t, err)
	require.Equal(t, Media{Addr: "198.51.100.7", Port: 30000, Codecs: []Codec{pcmu, dtmfCodec}, Direction: DirSendRecv}, got)
}

func TestCheckCodecsAcceptsTheSupportedNamesAndRefusesOthers(t *testing.T) {
	require.NoError(t, CheckCodecs([]string{"PCMU", "PCMA", "G722"}))
	require.EqualError(t, CheckCodecs(nil), "at least one codec is required")
	require.EqualError(t, CheckCodecs([]string{"PCMU", "OPUS"}), `unsupported codec "OPUS", use PCMU, PCMA or G722`)
}

// offerAt is an offer from the customer with conn as its session-level c= line and media as the
// rest, PCMU unless media says otherwise.
func offerAt(conn, media string) []byte {
	body := "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\n" + conn + "t=0 0\r\n"
	if media == "" {
		media = "m=audio 30000 RTP/AVP 0\r\n"
	}
	return []byte(body + media)
}

func TestToStreamSendsStreamOnlyToAPublicAddress(t *testing.T) {
	trunk := netip.MustParseAddr("1.0.0.1")
	for name, tc := range map[string]struct {
		conn, media string
		addr, dir   string
	}{
		"public ipv4":                   {conn: "c=IN IP4 1.1.1.1\r\n", addr: "1.1.1.1", dir: DirSendRecv},
		"public ipv6":                   {conn: "c=IN IP6 2606:4700:4700::1111\r\n", addr: "2606:4700:4700::1111", dir: DirSendRecv},
		"public ipv4-mapped ipv6":       {conn: "c=IN IP6 ::ffff:1.1.1.1\r\n", addr: "1.1.1.1", dir: DirSendRecv},
		"loopback":                      {conn: "c=IN IP4 127.0.0.1\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"private 10/8":                  {conn: "c=IN IP4 10.1.2.3\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"private 192.168/16":            {conn: "c=IN IP4 192.168.1.20\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"link-local metadata":           {conn: "c=IN IP4 169.254.169.254\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"carrier-grade nat":             {conn: "c=IN IP4 100.64.0.1\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"ipv6 loopback":                 {conn: "c=IN IP6 ::1\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"ipv6 unique-local":             {conn: "c=IN IP6 fd00::1\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"private ipv4-mapped ipv6":      {conn: "c=IN IP6 ::ffff:10.0.0.1\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"hostname":                      {conn: "c=IN IP4 pbx.example.com\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"hold sendrecv":                 {conn: "c=IN IP4 0.0.0.0\r\n", addr: "1.0.0.1", dir: "sendonly"},
		"hold recvonly":                 {conn: "c=IN IP4 0.0.0.0\r\n", media: "m=audio 30000 RTP/AVP 0\r\na=recvonly\r\n", addr: "1.0.0.1", dir: DirInactive},
		"ipv6 hold sendrecv":            {conn: "c=IN IP6 ::\r\n", addr: "1.0.0.1", dir: "sendonly"},
		"hold sendonly":                 {conn: "c=IN IP4 0.0.0.0\r\n", media: "m=audio 30000 RTP/AVP 0\r\na=sendonly\r\n", addr: "1.0.0.1", dir: "sendonly"},
		"private address keeps its dir": {conn: "c=IN IP4 10.1.2.3\r\n", media: "m=audio 30000 RTP/AVP 0\r\na=recvonly\r\n", addr: "1.0.0.1", dir: "recvonly"},
		"media level beats session":     {conn: "c=IN IP4 10.1.2.3\r\n", media: "m=audio 30000 RTP/AVP 0\r\nc=IN IP4 1.1.1.1\r\n", addr: "1.1.1.1", dir: DirSendRecv},
		"private media level":           {conn: "c=IN IP4 1.1.1.1\r\n", media: "m=audio 30000 RTP/AVP 0\r\nc=IN IP4 10.1.2.3\r\n", addr: "1.0.0.1", dir: DirSendRecv},
		"session sendonly":              {conn: "c=IN IP4 1.1.1.1\r\n", media: "a=sendonly\r\nm=audio 30000 RTP/AVP 0\r\n", addr: "1.1.1.1", dir: "sendonly"},
		"session recvonly":              {conn: "c=IN IP4 1.1.1.1\r\n", media: "a=recvonly\r\nm=audio 30000 RTP/AVP 0\r\n", addr: "1.1.1.1", dir: "recvonly"},
		"session inactive":              {conn: "c=IN IP4 1.1.1.1\r\n", media: "a=inactive\r\nm=audio 30000 RTP/AVP 0\r\n", addr: "1.1.1.1", dir: DirInactive},
		"session hold sendonly":         {conn: "c=IN IP4 0.0.0.0\r\n", media: "a=sendonly\r\nm=audio 30000 RTP/AVP 0\r\n", addr: "1.0.0.1", dir: "sendonly"},
		"media direction beats session": {conn: "c=IN IP4 1.1.1.1\r\n", media: "a=inactive\r\nm=audio 30000 RTP/AVP 0\r\na=sendrecv\r\n", addr: "1.1.1.1", dir: DirSendRecv},
		"first audio of several":        {conn: "c=IN IP4 1.1.1.1\r\n", media: "m=video 30002 RTP/AVP 96\r\nc=IN IP4 10.9.9.9\r\nm=audio 30000 RTP/AVP 0\r\nm=audio 30004 RTP/AVP 8\r\n", addr: "1.1.1.1", dir: DirSendRecv},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := toStream(offerAt(tc.conn, tc.media), trunk)
			require.NoError(t, err)

			m := mediaOf(t, got)
			require.Equal(t, tc.addr, m.Addr)
			require.Equal(t, tc.dir, m.Direction)
			require.Equal(t, 30000, m.Port)
			require.Equal(t, []Codec{pcmu}, m.Codecs)
			require.Equal(t, 1, strings.Count(string(got), "m="))
		})
	}
}

func TestToStreamWritesAnIPv6AddressAsIP6(t *testing.T) {
	got, err := toStream(offerAt("c=IN IP6 2606:4700:4700::1111\r\n", ""), netip.MustParseAddr("1.0.0.1"))
	require.NoError(t, err)

	require.Contains(t, string(got), "\r\nc=IN IP6 2606:4700:4700::1111\r\n")
	require.Contains(t, string(got), "\r\no=- 1 1 IN IP6 2606:4700:4700::1111\r\n")
}

func TestToStreamRefusesWhatItCannotRebuild(t *testing.T) {
	trunk := netip.MustParseAddr("1.0.0.1")
	for name, tc := range map[string]struct {
		body []byte
		err  string
	}{
		"not sdp":         {body: []byte("offer"), err: "parse sdp"},
		"no c= line":      {body: offerAt("", ""), err: "no connection address"},
		"no audio":        {body: offerAt("c=IN IP4 1.1.1.1\r\n", "m=video 30002 RTP/AVP 96\r\n"), err: "no media"},
		"only dtmf":       {body: offerAt("c=IN IP4 1.1.1.1\r\n", "m=audio 30000 RTP/AVP 101\r\na=rtpmap:101 telephone-event/8000\r\n"), err: "no codec Stream accepts"},
		"only opus":       {body: offerAt("c=IN IP4 1.1.1.1\r\n", "m=audio 30000 RTP/AVP 111\r\na=rtpmap:111 opus/48000/2\r\n"), err: "no codec Stream accepts"},
		"unknown dynamic": {body: offerAt("c=IN IP4 1.1.1.1\r\n", "m=audio 30000 RTP/AVP 98\r\n"), err: "no codec Stream accepts"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := toStream(tc.body, trunk)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestToStreamKeepsTheCustomersPacketTime(t *testing.T) {
	for name, tc := range map[string]struct{ attrs, want string }{
		"none":            {attrs: "", want: "a=ptime:20\r\na=sendrecv\r\n"},
		"ptime":           {attrs: "a=ptime:30\r\n", want: "a=ptime:30\r\na=sendrecv\r\n"},
		"maxptime":        {attrs: "a=maxptime:40\r\n", want: "a=ptime:20\r\na=maxptime:40\r\na=sendrecv\r\n"},
		"both":            {attrs: "a=maxptime:60\r\na=ptime:30\r\n", want: "a=ptime:30\r\na=maxptime:60\r\na=sendrecv\r\n"},
		"not a number":    {attrs: "a=ptime:x30\r\na=maxptime:40 ms\r\n", want: "a=ptime:20\r\na=sendrecv\r\n"},
		"on another line": {attrs: "m=video 30002 RTP/AVP 96\r\na=ptime:30\r\n", want: "a=ptime:20\r\na=sendrecv\r\n"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := toStream(offerAt("c=IN IP4 1.1.1.1\r\n", "m=audio 30000 RTP/AVP 0\r\n"+tc.attrs), testTrunkAddr)
			require.NoError(t, err)
			require.True(t, strings.HasSuffix(string(got), "\r\n"+tc.want), string(got))
		})
	}
}

func TestToStreamReadsTheAudioCodecsFromTheAudioLineOnly(t *testing.T) {
	// The video line comes first and names payload type 96 for another codec.
	media := "m=video 30002 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\n" +
		"m=audio 30000 RTP/AVP 0 96\r\na=rtpmap:96 telephone-event/8000\r\n"

	got, err := toStream(offerAt("c=IN IP4 1.1.1.1\r\n", media), testTrunkAddr)
	require.NoError(t, err)

	require.Equal(t, []Codec{pcmu, {Name: dtmfCodec.Name, PayloadType: 96, ClockRate: 8000}}, mediaOf(t, got).Codecs)
}

func TestToStreamKeepsOnlyTheCodecsStreamAccepts(t *testing.T) {
	media := "m=audio 30000 RTP/AVP 111 9 8 0 101\r\na=rtpmap:111 opus/48000/2\r\na=rtpmap:101 telephone-event/8000\r\n"

	got, err := toStream(offerAt("c=IN IP4 1.1.1.1\r\n", media), netip.MustParseAddr("1.0.0.1"))
	require.NoError(t, err)

	require.Equal(t, []Codec{staticCodecs["G722"], pcma, pcmu, dtmfCodec}, mediaOf(t, got).Codecs)
}

func TestAnswerForRejectsTheLinesStreamDidNotSee(t *testing.T) {
	offer := offerAt("c=IN IP4 1.1.1.1\r\n",
		"m=video 30002 RTP/AVP 96\r\nm=audio 30000 RTP/AVP 0\r\nm=text 30004 RTP/AVP 98\r\n")
	answer := buildSDP(Media{Addr: "1.0.0.2", Port: 20000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, 2, 1)

	var got sdp.SessionDescription
	require.NoError(t, got.Unmarshal(answerFor(offer, answer)))

	require.Len(t, got.MediaDescriptions, 3)
	for i, want := range []struct {
		media string
		port  int
	}{{"video", 0}, {"audio", 20000}, {"text", 0}} {
		md := got.MediaDescriptions[i]
		require.Equal(t, want.media, md.MediaName.Media)
		require.Equal(t, want.port, md.MediaName.Port.Value)
	}
	require.Equal(t, []string{"98"}, got.MediaDescriptions[2].MediaName.Formats)
}

func TestAnswerForGivesEveryLineAnAddressWhenStreamNamesItOnlyOnItsMediaLine(t *testing.T) {
	offer := offerAt("c=IN IP4 1.1.1.1\r\n", "m=video 30002 RTP/AVP 96\r\nm=audio 30000 RTP/AVP 0\r\n")
	answer := []byte("v=0\r\no=- 2 1 IN IP4 1.0.0.2\r\ns=-\r\nt=0 0\r\nm=audio 20000 RTP/AVP 0\r\nc=IN IP4 1.0.0.2\r\na=rtpmap:0 PCMU/8000\r\n")

	var got sdp.SessionDescription
	require.NoError(t, got.Unmarshal(answerFor(offer, answer)))

	require.Len(t, got.MediaDescriptions, 2)
	for _, md := range got.MediaDescriptions {
		// The session has no c=, so each m= line needs its own (RFC 8866 §5.7).
		require.NotNil(t, md.ConnectionInformation, md.MediaName.Media)
	}
}

func TestAnswerForPassesOnAnAnswerItDoesNotFit(t *testing.T) {
	answer := buildSDP(Media{Addr: "1.0.0.2", Port: 20000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, 2, 1)
	two := offerAt("c=IN IP4 1.1.1.1\r\n", "m=video 30002 RTP/AVP 96\r\nm=audio 30000 RTP/AVP 0\r\n")
	twoAnswer := append(append([]byte{}, answer...), "m=video 0 RTP/AVP 96\r\n"...)
	for name, tc := range map[string]struct{ offer, answer []byte }{
		// Line feeds alone, which a rewrite would turn into CRLF: the answer must keep its bytes.
		"one line offered":      {offer: offerAt("c=IN IP4 1.1.1.1\r\n", ""), answer: []byte(strings.ReplaceAll(string(answer), "\r\n", "\n"))},
		"offer not sdp":         {offer: []byte("offer"), answer: answer},
		"answer not sdp":        {offer: two, answer: []byte("answer")},
		"answer with two lines": {offer: two, answer: twoAnswer},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.answer, answerFor(tc.offer, tc.answer))
		})
	}
}

// twoStreamLines is an offer with an audio and a video line, one line per element.
var twoStreamLines = []string{
	"v=0", "o=- 1 1 IN IP4 1.1.1.1", "s=-", "c=IN IP4 1.1.1.1", "t=0 0",
	"m=audio 30000 RTP/AVP 0", "a=rtpmap:0 PCMU/8000", "m=video 30002 RTP/AVP 96", "a=rtpmap:96 VP8/90000",
}

// lineEndings is twoStreamLines written with each way an SDP body may end its lines.
var lineEndings = map[string]string{
	"CRLF":                    strings.Join(twoStreamLines, "\r\n") + "\r\n",
	"LF":                      strings.Join(twoStreamLines, "\n") + "\n",
	"CR":                      strings.Join(twoStreamLines, "\r") + "\r",
	"no final line ending":    strings.Join(twoStreamLines, "\r\n"),
	"mixed":                   mixedEndings(twoStreamLines),
	"bare CR after the audio": strings.Replace(strings.Join(twoStreamLines, "\r\n")+"\r\n", "0\r\na=rtpmap:0", "0\ra=rtpmap:0", 1),
	"bare CR between m=":      "v=0\r\no=- 1 1 IN IP4 1.1.1.1\r\ns=-\r\nc=IN IP4 1.1.1.1\r\nt=0 0\r\nm=audio 30000 RTP/AVP 0\rm=video 30002 RTP/AVP 96\r\n",
}

func mixedEndings(lines []string) string {
	var b strings.Builder
	for i, line := range lines {
		b.WriteString(line + []string{"\r\n", "\n", "\r"}[i%3])
	}
	return b.String()
}

// requireAudioThenRejectedVideo checks an answer to a twoStreamLines offer: Stream's audio, then the
// video line rejected.
func requireAudioThenRejectedVideo(t *testing.T, answer []byte) {
	t.Helper()
	var got sdp.SessionDescription
	require.NoError(t, got.Unmarshal(answer))
	require.Len(t, got.MediaDescriptions, 2)
	require.Equal(t, "audio", got.MediaDescriptions[0].MediaName.Media)
	require.NotZero(t, got.MediaDescriptions[0].MediaName.Port.Value)
	require.Equal(t, "video", got.MediaDescriptions[1].MediaName.Media)
	require.Zero(t, got.MediaDescriptions[1].MediaName.Port.Value)
}

func TestAnswerForFitsAnOfferWhateverItsLineEndings(t *testing.T) {
	answer := buildSDP(Media{Addr: "1.0.0.2", Port: 20000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, 2, 1)
	for name, offer := range lineEndings {
		t.Run(name, func(t *testing.T) {
			var got []byte
			require.NotPanics(t, func() { got = answerFor([]byte(offer), answer) })
			requireAudioThenRejectedVideo(t, got)
		})
	}
}

func TestToStreamReadsAnOfferWhateverItsLineEndings(t *testing.T) {
	for name, offer := range lineEndings {
		t.Run(name, func(t *testing.T) {
			got, err := toStream([]byte(offer), testTrunkAddr)
			require.NoError(t, err)
			require.Equal(t, Media{Addr: "1.1.1.1", Port: 30000, Codecs: []Codec{pcmu}, Direction: DirSendRecv}, mediaOf(t, got))
		})
	}
}
