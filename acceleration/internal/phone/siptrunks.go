package phone

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/egress"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/sipbridge"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// SIPTrunkVendor is the vendor every number on a customer's own SIP trunk is recorded
// under. It is not in the registry: the registry is what every customer is offered to buy
// from, and nobody buys from this one.
const SIPTrunkVendor = "sip_trunk"

// sipTrunkRecord is the record label of SIP trunk passwords in the appkeys counters.
const sipTrunkRecord = "sip_trunk"

// The counter starts here so a dashboard or alert on the sip_trunk series reads 0 from the
// router's start, not "no data" until the first fallback or open under the router's key.
func init() { appkeys.StartCounting(sipTrunkRecord) }

// ErrSIPTrunksDisabled says this deployment has no key to seal a trunk's password with.
var ErrSIPTrunksDisabled = errors.New("phone: sip trunks are off on this deployment: it has no key " +
	"encryption key to seal their passwords with")

// ErrInvalidSIPTrunk starts every reason a trunk or a number on one is refused for, the way
// dlc.ErrInvalid does for 10DLC, so a caller can show what follows it to a person.
var ErrInvalidSIPTrunk = errors.New("sip_trunk: invalid")

// invalid is one ErrInvalidSIPTrunk naming every reason, nil when there is none.
func invalid(reasons []string) error {
	if len(reasons) == 0 {
		return nil
	}
	return stack.Wrap(fmt.Errorf("%w: %s", ErrInvalidSIPTrunk, strings.Join(reasons, "; ")))
}

const (
	defaultSIPPort      = 5060
	defaultSIPTransport = "tcp"
)

var countryPattern = regexp.MustCompile(`^[A-Z]{2}$`)

// SIPTrunk is a customer's own SIP trunk as a call is dialled through it, password open.
type SIPTrunk struct {
	Host      string
	Port      int
	Transport string
	Username  string
	Password  string
	LateOffer bool
	Codecs    []string
}

// SIPTrunkSettings is what a customer says about a trunk.
type SIPTrunkSettings struct {
	Name      string
	Host      string
	Port      int
	Transport string
	Username  string
	// Password is nil on an update that keeps the stored one.
	Password  *string
	LateOffer bool
	Codecs    []string
}

// SettingsOf is what a stored trunk says, without its password, for an update to change
// some of it.
func SettingsOf(trunk store.SIPTrunk) SIPTrunkSettings {
	return SIPTrunkSettings{
		Name: trunk.Name, Host: trunk.Host, Port: trunk.Port, Transport: trunk.Transport,
		Username: trunk.Username, LateOffer: trunk.LateOffer, Codecs: trunk.Codecs,
	}
}

func (settings SIPTrunkSettings) normalized() SIPTrunkSettings {
	settings.Host = strings.TrimSpace(settings.Host)
	if settings.Port == 0 {
		settings.Port = defaultSIPPort
	}
	if settings.Transport == "" {
		settings.Transport = defaultSIPTransport
	}
	settings.Transport = strings.ToLower(settings.Transport)
	if len(settings.Codecs) == 0 {
		settings.Codecs = []string{"PCMU", "PCMA"}
	}
	codecs := make([]string, len(settings.Codecs))
	for i, codec := range settings.Codecs {
		codecs[i] = strings.ToUpper(codec)
	}
	settings.Codecs = codecs
	return settings
}

// nonPublicIP says host is an IP address calls may not go to. A name is only checked when a
// call is placed, since what it resolves to can change after it is saved.
func nonPublicIP(host string) bool {
	ip, err := netip.ParseAddr(host)
	return err == nil && !egress.IsPublic(ip)
}

// validate reports every problem at once, so a form is fixed in one pass. creating says the
// password has to be there; an update without one keeps the stored password.
func (settings SIPTrunkSettings) validate(creating bool) error {
	var reasons []string
	if settings.Name == "" {
		reasons = append(reasons, "name is required")
	}
	switch {
	case settings.Host == "":
		reasons = append(reasons, "host is required")
	case strings.ContainsAny(settings.Host, ":@/; "):
		reasons = append(reasons, fmt.Sprintf("host %q must be a bare hostname, without sip: or a port", settings.Host))
	case nonPublicIP(settings.Host):
		reasons = append(reasons, fmt.Sprintf("host %q is not a public address", settings.Host))
	}
	if settings.Port < 1 || settings.Port > 65535 {
		reasons = append(reasons, fmt.Sprintf("port %d must be 1..65535", settings.Port))
	}
	if settings.Transport != "udp" && settings.Transport != "tcp" && settings.Transport != "tls" {
		reasons = append(reasons, fmt.Sprintf("transport %q must be udp, tcp or tls", settings.Transport))
	}
	if settings.Username == "" {
		reasons = append(reasons, "username is required")
	}
	switch {
	case settings.Password == nil && creating:
		reasons = append(reasons, "password is required")
	case settings.Password != nil && *settings.Password == "":
		reasons = append(reasons, "password cannot be empty")
	}
	for _, codec := range settings.Codecs {
		if sipbridge.CheckCodecs([]string{codec}) != nil {
			reasons = append(reasons, fmt.Sprintf("codec %q is not supported, use PCMU, PCMA or G722", codec))
		}
	}
	return invalid(reasons)
}

// TrunkNumber is a number a customer says is on one of their own trunks.
type TrunkNumber struct {
	Owner   routing.Owner
	TrunkID string
	E164    string
	// Country is the ISO 3166-1 alpha-2 code. It is asked for rather than worked out,
	// because nothing here parses numbers.
	Country string
}

func (s *Service) sipTrunksReady() error {
	if s.sealer == nil || s.sipTrunks == nil {
		return stack.Wrap(ErrSIPTrunksDisabled)
	}
	if s.store == nil {
		return stack.Wrap(errors.New("phone: sip trunks need a database"))
	}
	return nil
}

// sipTrunkAAD binds a sealed password to its trunk, so ciphertext copied onto another row
// does not open.
func sipTrunkAAD(customerID, id string) []byte {
	return []byte("sip_trunks/" + customerID + "/" + id)
}

// bridgeAAD binds a number's sealed trunk password to the customer holding it, so the
// ciphertext does not open on another customer's row or another number.
func bridgeAAD(customerID, e164 string) []byte {
	return []byte("phone_numbers/" + customerID + "/" + e164)
}

// CreateSIPTrunk stores a customer's trunk with its password sealed.
func (s *Service) CreateSIPTrunk(ctx context.Context, customerID string, settings SIPTrunkSettings) (store.SIPTrunk, error) {
	if err := s.sipTrunksReady(); err != nil {
		return store.SIPTrunk{}, err
	}
	if customerID == "" {
		return store.SIPTrunk{}, stack.Wrap(errors.New("phone: a sip trunk must belong to a customer"))
	}
	settings = settings.normalized()
	if err := settings.validate(true); err != nil {
		return store.SIPTrunk{}, err
	}
	trunk := store.SIPTrunk{ID: uuid.NewString(), CustomerID: customerID}
	if err := s.applySIPTrunk(ctx, &trunk, settings); err != nil {
		return store.SIPTrunk{}, err
	}
	if err := s.store.SIPTrunks.Insert(ctx, &trunk); err != nil {
		return store.SIPTrunk{}, err
	}
	return trunk, nil
}

// SIPTrunks returns a customer's trunks.
func (s *Service) SIPTrunks(ctx context.Context, customerID string) ([]store.SIPTrunk, error) {
	if err := s.sipTrunksReady(); err != nil {
		return nil, err
	}
	return s.store.SIPTrunks.Select(ctx, customerID)
}

// SIPTrunk returns one of a customer's trunks.
func (s *Service) SIPTrunk(ctx context.Context, customerID, id string) (store.SIPTrunk, error) {
	if err := s.sipTrunksReady(); err != nil {
		return store.SIPTrunk{}, err
	}
	return s.store.SIPTrunks.Get(ctx, customerID, id)
}

// UpdateSIPTrunk replaces what a trunk says. A nil password keeps the stored one.
func (s *Service) UpdateSIPTrunk(ctx context.Context, customerID, id string, settings SIPTrunkSettings) (store.SIPTrunk, error) {
	if err := s.sipTrunksReady(); err != nil {
		return store.SIPTrunk{}, err
	}
	trunk, err := s.store.SIPTrunks.Get(ctx, customerID, id)
	if err != nil {
		return store.SIPTrunk{}, err
	}
	settings = settings.normalized()
	if err := settings.validate(false); err != nil {
		return store.SIPTrunk{}, err
	}
	if err := s.applySIPTrunk(ctx, &trunk, settings); err != nil {
		return store.SIPTrunk{}, err
	}
	if err := s.store.SIPTrunks.Update(ctx, &trunk); err != nil {
		return store.SIPTrunk{}, err
	}
	return trunk, nil
}

// DeleteSIPTrunk removes a trunk that has no numbers left on it.
func (s *Service) DeleteSIPTrunk(ctx context.Context, customerID, id string) error {
	if err := s.sipTrunksReady(); err != nil {
		return err
	}
	return s.store.SIPTrunks.Delete(ctx, customerID, id)
}

// AddTrunkNumber records a number on one of the customer's trunks. Nothing is bought and
// nothing is checked with a carrier: whether the number is really the customer's is the
// trunk's carrier's to decide when it is dialled from.
func (s *Service) AddTrunkNumber(ctx context.Context, request TrunkNumber) (store.PhoneNumber, error) {
	if err := s.sipTrunksReady(); err != nil {
		return store.PhoneNumber{}, err
	}
	country := strings.ToUpper(request.Country)
	var reasons []string
	if !store.IsE164(request.E164) {
		reasons = append(reasons, fmt.Sprintf("e164 %q is not an E.164 number, e.g. +15551234567", request.E164))
	}
	if !countryPattern.MatchString(country) {
		reasons = append(reasons, fmt.Sprintf("country %q must be a two-letter code, e.g. US", request.Country))
	}
	if err := request.Owner.Tags.Validate(); err != nil {
		reasons = append(reasons, strings.TrimPrefix(err.Error(), "routing: "))
	}
	if err := invalid(reasons); err != nil {
		return store.PhoneNumber{}, err
	}
	number := store.PhoneNumber{
		E164:         request.E164,
		Vendor:       SIPTrunkVendor,
		Country:      country,
		Capabilities: []string{string(Voice)},
		CustomerID:   request.Owner.CustomerID,
		Tags:         request.Owner.Tags,
		SIPTrunkID:   request.TrunkID,
		PurchasedAt:  time.Now().UTC(),
	}
	apart, err := s.heldApart(ctx, number.CustomerID, number.E164)
	if err != nil {
		return store.PhoneNumber{}, err
	}
	if err := s.store.PhoneNumbers.InsertOnSIPTrunk(ctx, &number, apart); err != nil {
		return store.PhoneNumber{}, err
	}
	return number, nil
}

// heldApart are the customers holding a number whose lines are made in another Stream app than
// the customer's. Stream lists a number on one trunk per app, so on the customer's own trunk the
// number can only block a holder whose lines are made where the customer's are. A holder whose
// app, or the customer's, is known to be unreadable (disconnected, no identity) is taken to share
// it; a read that failed is returned as it is, so the add is tried again rather than refused.
func (s *Service) heldApart(ctx context.Context, customer, e164 string) ([]string, error) {
	held, err := s.store.PhoneNumbers.Holders(ctx, e164)
	if err != nil {
		return nil, err
	}
	others := slices.DeleteFunc(held, func(holder string) bool { return holder == customer })
	if len(others) == 0 {
		return nil, nil
	}
	mine, err := s.placed(ctx, customer)
	if !settled(err) {
		return nil, err
	}
	if err != nil {
		// Every holder shares an app nobody can name.
		return nil, nil
	}
	var apart []string
	for _, holder := range others {
		theirs, err := s.placed(ctx, holder)
		if !settled(err) {
			return nil, err
		}
		if err == nil && theirs != mine {
			apart = append(apart, holder)
		}
	}
	return apart, nil
}

// applySIPTrunk copies settings onto a trunk row and seals the password when one was given.
// With sealing on, the password goes under the app's data key. In every other case, including
// KMS being down, it goes under the router's key, so a trunk is never lost to a KMS outage.
func (s *Service) applySIPTrunk(ctx context.Context, trunk *store.SIPTrunk, settings SIPTrunkSettings) error {
	trunk.Name = settings.Name
	trunk.Host = settings.Host
	trunk.Port = settings.Port
	trunk.Transport = settings.Transport
	trunk.Username = settings.Username
	trunk.LateOffer = settings.LateOffer
	trunk.Codecs = settings.Codecs
	if settings.Password == nil {
		return nil
	}
	password, aad := *settings.Password, sipTrunkAAD(trunk.CustomerID, trunk.ID)
	sealed, err := s.dataKeys.SealRecord(ctx, sipTrunkPasswordRecord(*trunk), []byte(password), aad,
		func() ([]byte, int, error) {
			blob, err := s.sealer.SealWithAAD(password, aad)
			return blob, s.sealer.CurrentVersion(), err
		})
	if err != nil {
		return stack.Wrap(fmt.Errorf("phone: seal sip trunk password: %w", err))
	}
	trunk.PasswordSealed, trunk.PasswordKEKVersion, trunk.PasswordDataKeyVersion = sealed.Blob, sealed.KEKVersion,
		sealed.DataKeyVersion
	return nil
}

// openSIPTrunk returns the trunk's settings and its opened password, for a call to dial through.
// With sealing on, it moves a password sealed under the router's key to the app's data key.
func (s *Service) openSIPTrunk(ctx context.Context, customerID, id string) (*SIPTrunk, error) {
	trunk, err := s.store.SIPTrunks.Get(ctx, customerID, id)
	if err != nil {
		return nil, err
	}
	if !trunk.HasPassword() {
		return nil, stack.Wrap(Invalid(fmt.Errorf("phone: sip trunk %s has no password; set one before calling through it", id)))
	}
	aad := sipTrunkAAD(customerID, id)
	plain, err := s.dataKeys.OpenRecord(ctx, sipTrunkPasswordRecord(trunk), trunk.DataKeyVersion(), trunk.PasswordSealed,
		aad, func() ([]byte, error) {
			password, err := s.sealer.OpenWithAADVersion(trunk.PasswordSealed, aad, trunk.PasswordKEKVersion)
			return []byte(password), err
		})
	if err != nil {
		// The error can name the KMS key, so it goes to the log only: callers of a dial answer with
		// text of their own. A KMS or database failure says nothing about the trunk, so a later try
		// can work. A password that no key opens fails every time, so that error is lasting.
		err = fmt.Errorf("phone: open sip trunk password: %w", err)
		if trunk.DataKeyVersion() == nil || errors.Is(err, appkeys.ErrDoesNotOpen) || errors.Is(err, appkeys.ErrNoKeys) {
			err = lastingError{err}
		}
		return nil, stack.Wrap(err)
	}
	if trunk.DataKeyVersion() == nil {
		s.resealSIPTrunkPassword(ctx, trunk, plain)
	}
	return &SIPTrunk{
		Host: trunk.Host, Port: trunk.Port, Transport: trunk.Transport, Username: trunk.Username,
		Password: string(plain), LateOffer: trunk.LateOffer, Codecs: trunk.Codecs,
	}, nil
}

// resealSIPTrunkPassword moves a password that opened under the router's key to the app's data
// key when sealing is on. A failure is logged and leaves the row as it was, since the password
// opened and the call can still use it. The next dial tries again.
func (s *Service) resealSIPTrunkPassword(ctx context.Context, trunk store.SIPTrunk, password []byte) {
	sealed, err := s.dataKeys.ResealRecord(ctx, sipTrunkPasswordRecord(trunk), password,
		sipTrunkAAD(trunk.CustomerID, trunk.ID))
	// ResealRecord seals nothing and returns no version with sealing off or no data keys.
	if err == nil && sealed.DataKeyVersion != nil {
		_, err = s.store.SIPTrunks.ResealPassword(ctx, trunk, sealed.Blob, *sealed.DataKeyVersion)
	}
	if err != nil {
		s.logger.WarnContext(ctx, "left a sip trunk password under the router's key, since it could not be sealed "+
			"again under the app's data key", "app_id", trunk.CustomerID, "sip_trunk", trunk.ID, "error", err)
	}
}

// sipTrunkPasswordRecord is the password of a customer's SIP trunk, as the appkeys counters and
// log lines name it.
func sipTrunkPasswordRecord(trunk store.SIPTrunk) appkeys.Record {
	return appkeys.Record{Kind: sipTrunkRecord, CustomerID: trunk.CustomerID,
		Attrs: []slog.Attr{slog.String("sip_trunk", trunk.ID)}}
}

// ResumedTrunkCall is what a node continuing a call through a customer's own SIP trunk needs
// of the service.
type ResumedTrunkCall struct {
	Trunk  *SIPTrunk
	Bridge Bridge
	// Ended reports how the call ended, as the node that placed it would have.
	Ended func(CallStatus)
}

// ResumeTrunkCall opens the credentials of both sides of a call another node placed on a
// shared line, read fresh since none are kept with the call.
func (s *Service) ResumeTrunkCall(ctx context.Context, ref TrunkCallRef) (ResumedTrunkCall, error) {
	trunk, err := s.openSIPTrunk(ctx, ref.CustomerID, ref.SIPTrunkID)
	if err != nil {
		return ResumedTrunkCall{}, err
	}
	shared, err := s.store.StreamTrunks.Get(ctx, ref.CustomerID, ref.StreamTrunkID)
	if err != nil {
		return ResumedTrunkCall{}, err
	}
	line, err := s.lineOn(ctx, shared, ref.From, "")
	if err != nil {
		return ResumedTrunkCall{}, err
	}
	return ResumedTrunkCall{Trunk: trunk, Bridge: line.Bridge, Ended: s.trunkCallEnded(ctx, ref.Token, ref.CallID)}, nil
}

// Close hands the calls this node holds through customers' own trunks to another node, or
// hangs them up when no other node takes them or there is no Redis. Calls placed at a vendor
// are the vendor's to hold and are not touched. The agent checks still waiting stop, and their
// calls continue.
func (s *Service) Close(ctx context.Context) error {
	s.closeOnce.Do(func() { close(s.closing) })
	if closer, ok := s.sipTrunks.(interface{ Close(context.Context) error }); ok {
		return closer.Close(ctx)
	}
	return nil
}
