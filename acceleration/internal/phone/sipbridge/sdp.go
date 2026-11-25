package sipbridge

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/pion/sdp/v3"

	"github.com/GetStream/Vision-Agents/acceleration/internal/egress"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

const (
	DirSendRecv = "sendrecv"
	DirInactive = "inactive"
)

// The placeholder is the address in flow B's first offer to Stream, before the customer
// has answered. The default is a documentation address (RFC 5737) that routes nowhere.
const (
	defaultPlaceholderAddr = "192.0.2.1"
	defaultPlaceholderPort = 20000
)

// rejectAnswer's address is unrelated to the flow B placeholder: the stream is rejected, so
// nothing is ever sent to it.
const rejectAddr = "192.0.2.1"

// Codec is one RTP payload format.
type Codec struct {
	Name        string
	PayloadType uint8
	ClockRate   uint32
}

// staticCodecs are the audio codecs both Stream and this package accept, keyed by name.
// G722's clock rate is 8000 in SDP for historical reasons (RFC 3551).
var staticCodecs = map[string]Codec{
	"PCMU": {Name: "PCMU", PayloadType: 0, ClockRate: 8000},
	"PCMA": {Name: "PCMA", PayloadType: 8, ClockRate: 8000},
	"G722": {Name: "G722", PayloadType: 9, ClockRate: 8000},
}

var dtmfCodec = Codec{Name: "telephone-event", PayloadType: 101, ClockRate: 8000}

func codecsFromNames(names []string) ([]Codec, error) {
	if len(names) == 0 {
		return nil, stack.Wrap(fmt.Errorf("at least one codec is required"))
	}
	codecs := make([]Codec, 0, len(names))
	for _, name := range names {
		codec, ok := staticCodecs[name]
		if !ok {
			return nil, stack.Wrap(fmt.Errorf("unsupported codec %q, use PCMU, PCMA or G722", name))
		}
		codecs = append(codecs, codec)
	}
	return codecs, nil
}

// CheckCodecs reports whether every name is a codec Dial can offer, so a caller can refuse a
// trunk when it is saved rather than when it is first dialled.
func CheckCodecs(names []string) error {
	_, err := codecsFromNames(names)
	return err
}

func staticCodecByPayloadType(pt uint8) (Codec, bool) {
	for _, codec := range staticCodecs {
		if codec.PayloadType == pt {
			return codec, true
		}
	}
	return Codec{}, false
}

// Media is the first audio stream of an SDP body: where to send RTP and in which formats.
type Media struct {
	Addr      string
	Port      int
	Codecs    []Codec
	Direction string
}

func (m Media) audioCodec() (Codec, bool) {
	for _, codec := range m.Codecs {
		if codec.Name != dtmfCodec.Name {
			return codec, true
		}
	}
	return Codec{}, false
}

func (m Media) hasDTMF() bool {
	for _, codec := range m.Codecs {
		if codec.Name == dtmfCodec.Name {
			return true
		}
	}
	return false
}

// ParseMedia reads the first audio m= line of body.
func ParseMedia(body []byte) (Media, error) {
	m, _, _, err := parseAudio(body)
	return m, err
}

// parseAudio is ParseMedia that also returns the whole body and the index of the m= line it read.
func parseAudio(body []byte) (Media, *sdp.SessionDescription, int, error) {
	sd, _, err := parseSDP(body)
	if err != nil {
		return Media{}, nil, 0, err
	}
	at := slices.IndexFunc(sd.MediaDescriptions, func(md *sdp.MediaDescription) bool {
		return md.MediaName.Media == "audio"
	})
	if at < 0 {
		return Media{}, nil, 0, stack.Wrap(errors.New("sdp has no media of type audio"))
	}
	md := sd.MediaDescriptions[at]

	m := Media{Port: md.MediaName.Port.Value, Direction: DirSendRecv}
	switch {
	case md.ConnectionInformation != nil && md.ConnectionInformation.Address != nil:
		m.Addr = md.ConnectionInformation.Address.Address
	case sd.ConnectionInformation != nil && sd.ConnectionInformation.Address != nil:
		m.Addr = sd.ConnectionInformation.Address.Address
	default:
		return Media{}, nil, 0, stack.Wrap(errors.New("sdp has no connection address"))
	}

	for _, format := range md.MediaName.Formats {
		pt, err := strconv.ParseUint(format, 10, 8)
		if err != nil {
			return Media{}, nil, 0, stack.Wrap(fmt.Errorf("sdp payload type %q: %w", format, err))
		}
		if codec, err := sd.GetCodecForPayloadType(uint8(pt)); err == nil {
			name := codec.Name
			// Normalize codec names: telephone-event variants -> "telephone-event", others -> uppercase
			if strings.EqualFold(name, "telephone-event") {
				name = dtmfCodec.Name
			} else {
				name = strings.ToUpper(name)
			}
			m.Codecs = append(m.Codecs, Codec{Name: name, PayloadType: uint8(pt), ClockRate: codec.ClockRate})
			continue
		}
		if codec, ok := staticCodecByPayloadType(uint8(pt)); ok {
			m.Codecs = append(m.Codecs, codec)
		}
	}

	for _, attr := range md.Attributes {
		switch attr.Key {
		case "sendrecv", "sendonly", "recvonly", "inactive":
			m.Direction = attr.Key
		}
	}
	return m, sd, at, nil
}

// pionMedia are the media types the SDP parser accepts in an m= line (RFC 8866 §8 lists more).
var pionMedia = []string{"audio", "video", "text", "application", "message"}

// parseSDP unmarshals body, and also returns each m= line as written, without the "m=". The parser
// refuses a type it does not know, such as m=image (T.38 fax), or formats that are not numbers, which
// would fail a call that has a usable audio line next to it. Such a line is read as an empty
// m=application line, and the lines returned tell what it was, so an answer can still reject it by its
// own type, protocol and formats.
//
// The parser ends some lines at a bare CR as well as at LF, so body is split at either and rejoined
// with CRLF before it is parsed: the parser then sees exactly the m= lines returned, one each.
func parseSDP(body []byte) (*sdp.SessionDescription, []string, error) {
	var mLines []string
	lines := strings.FieldsFunc(string(body), func(r rune) bool { return r == '\r' || r == '\n' })
	for i, line := range lines {
		rest, ok := strings.CutPrefix(line, "m=")
		if !ok {
			continue
		}
		mLines = append(mLines, rest)
		if name, _, _ := strings.Cut(rest, " "); !slices.Contains(pionMedia, name) {
			lines[i] = "m=application 0 RTP/AVP 0"
		}
	}
	var sd sdp.SessionDescription
	if err := sd.Unmarshal([]byte(strings.Join(lines, "\r\n") + "\r\n")); err != nil {
		return nil, nil, stack.Wrap(fmt.Errorf("parse sdp: %w", err))
	}
	if len(sd.MediaDescriptions) != len(mLines) {
		return nil, nil, stack.Wrap(fmt.Errorf("parse sdp: %d media sections in %d m= lines", len(sd.MediaDescriptions), len(mLines)))
	}
	return &sd, mLines, nil
}

func buildSDP(m Media, sessionID, version uint64) []byte {
	pts := make([]string, 0, len(m.Codecs))
	for _, codec := range m.Codecs {
		pts = append(pts, strconv.Itoa(int(codec.PayloadType)))
	}

	family := "IP4"
	if addr, err := netip.ParseAddr(m.Addr); err == nil && addr.Is6() {
		family = "IP6"
	}
	var b strings.Builder
	b.WriteString("v=0\r\n")
	fmt.Fprintf(&b, "o=- %d %d IN %s %s\r\n", sessionID, version, family, m.Addr)
	b.WriteString("s=sipbridge\r\n")
	fmt.Fprintf(&b, "c=IN %s %s\r\n", family, m.Addr)
	b.WriteString("t=0 0\r\n")
	fmt.Fprintf(&b, "m=audio %d RTP/AVP %s\r\n", m.Port, strings.Join(pts, " "))
	for _, codec := range m.Codecs {
		fmt.Fprintf(&b, "a=rtpmap:%d %s/%d\r\n", codec.PayloadType, codec.Name, codec.ClockRate)
		if codec.Name == dtmfCodec.Name {
			fmt.Fprintf(&b, "a=fmtp:%d 0-16\r\n", codec.PayloadType)
		}
	}
	b.WriteString("a=ptime:20\r\n")
	fmt.Fprintf(&b, "a=%s\r\n", m.Direction)
	return []byte(b.String())
}

// sdpSession is our side of one SDP session. RFC 3264 wants the o= session id to stay the
// same across re-INVITEs and the version to go up on every new offer.
type sdpSession struct {
	id      uint64
	version uint64
}

func newSDPSession() *sdpSession {
	return &sdpSession{id: rand.Uint64N(1 << 62)}
}

func (s *sdpSession) build(m Media) []byte {
	s.version++
	return buildSDP(m, s.id, s.version)
}

// placeholderOffer opens a session in Stream before we know where the customer's RTP is.
func placeholderOffer(addr string, port int, codecs []Codec) Media {
	all := append(append([]Codec{}, codecs...), dtmfCodec)
	return Media{Addr: addr, Port: port, Codecs: all, Direction: DirSendRecv}
}

// holdOffer puts Stream on hold while the phone rings.
func holdOffer(offer Media) Media {
	offer.Direction = DirInactive
	return offer
}

// customerOffer is what the customer is offered: Stream's RTP address and only the codec Stream
// picked.
func customerOffer(stream Media) (Media, error) {
	audio, ok := stream.audioCodec()
	if !ok {
		return Media{}, stack.Wrap(errors.New("stream answered with no audio codec"))
	}
	codecs := []Codec{audio}
	if stream.hasDTMF() {
		codecs = append(codecs, dtmfCodec)
	}
	return Media{Addr: stream.Addr, Port: stream.Port, Codecs: codecs, Direction: DirSendRecv}, nil
}

// retargetOffer points Stream's RTP at the customer, keeping the codecs Stream already uses.
func retargetOffer(customer, offered Media) (Media, error) {
	if customer.Port == 0 {
		return Media{}, stack.Wrap(errors.New("customer rejected the audio stream"))
	}
	want, ok := offered.audioCodec()
	if !ok {
		return Media{}, stack.Wrap(errors.New("offer to the customer had no audio codec"))
	}
	got, ok := customer.audioCodec()
	if !ok || got.Name != want.Name {
		return Media{}, stack.Wrap(fmt.Errorf("customer answered with %v, but only %s was offered", customer.Codecs, want.Name))
	}
	return Media{Addr: customer.Addr, Port: customer.Port, Codecs: offered.Codecs, Direction: DirSendRecv}, nil
}

// rejectAnswer answers an offer we cannot use. Port 0 rejects the stream (RFC 3264 §6); the
// m= line still has to name at least one format to be valid.
func rejectAnswer(offer Media) Media {
	codecs := offer.Codecs
	if len(codecs) == 0 {
		codecs = []Codec{staticCodecs["PCMU"]}
	}
	return Media{Addr: rejectAddr, Port: 0, Codecs: codecs, Direction: DirInactive}
}

// toStream rebuilds an SDP body from the customer leg, offer or answer, into the one Stream gets. Only
// what buildSDP writes reaches Stream: the first audio stream, the codecs Stream accepts and an address
// Stream may send RTP to. The customer's other attributes (a=rtcp, ICE candidates) and other m= lines are
// dropped. The o= line keeps the customer's session id and version, as the body passed on unchanged did,
// so a re-INVITE that changes nothing still says so (RFC 3264 §8).
func toStream(body []byte, trunk netip.Addr) ([]byte, error) {
	m, sd, _, err := parseAudio(body)
	if err != nil {
		return nil, err
	}
	m.Codecs = slices.DeleteFunc(m.Codecs, func(c Codec) bool {
		_, ok := staticCodecs[c.Name]
		return !ok && c.Name != dtmfCodec.Name
	})
	if _, ok := m.audioCodec(); !ok {
		return nil, stack.Wrap(errors.New("the customer's sdp has no codec Stream accepts"))
	}
	return buildSDP(reachable(m, trunk), sd.Origin.SessionID, sd.Origin.SessionVersion), nil
}

// reachable returns m with an address Stream may send RTP to. A trunk behind NAT often names its
// private address while its RTP comes from a public one, so a c= that is not a public IP address is
// replaced with the trunk's, which the guard checked, rather than failing the call. c=0.0.0.0 asks the
// other side not to send (RFC 3264 §8.4); once the address is replaced, the direction says so.
func reachable(m Media, trunk netip.Addr) Media {
	addr, err := netip.ParseAddr(m.Addr)
	if err == nil && egress.IsPublic(addr) {
		m.Addr = addr.Unmap().String()
		return m
	}
	m.Addr = trunk.String()
	if err == nil && addr.Unmap().IsUnspecified() {
		switch m.Direction {
		case DirSendRecv:
			m.Direction = "sendonly"
		case "recvonly":
			m.Direction = DirInactive
		}
	}
	return m
}

// answerFor fits a one-stream answer to an offer with more m= lines than the answerer saw: Stream's
// answer to an offer the customer made, or the customer's answer rebuilt by toStream. An answer has one
// m= line per offered line, in order (RFC 3264 §6), so the audio goes where the offer's first audio line
// was and every other line is rejected with port 0. Any other answer is passed on as it came.
func answerFor(offer, answer []byte) []byte {
	_, _, at, err := parseAudio(offer)
	if err != nil {
		return answer
	}
	o, mLines, err := parseSDP(offer)
	if err != nil || len(o.MediaDescriptions) == 1 || len(mLines) != len(o.MediaDescriptions) || at >= len(mLines) {
		return answer
	}
	var a sdp.SessionDescription
	if err := a.Unmarshal(answer); err != nil || len(a.MediaDescriptions) != 1 {
		return answer
	}
	audio := a.MediaDescriptions[0]
	lines := make([]*sdp.MediaDescription, len(o.MediaDescriptions))
	for i := range o.MediaDescriptions {
		if i == at {
			lines[i] = audio
			continue
		}
		fields := strings.Fields(mLines[i])
		if len(fields) < 4 {
			return answer
		}
		lines[i] = &sdp.MediaDescription{
			MediaName: sdp.MediaName{
				Media: fields[0], Protos: strings.Split(fields[2], "/"), Formats: fields[3:],
			},
			ConnectionInformation: audio.ConnectionInformation,
		}
	}
	a.MediaDescriptions = lines
	out, err := a.Marshal()
	if err != nil {
		return answer
	}
	return out
}
