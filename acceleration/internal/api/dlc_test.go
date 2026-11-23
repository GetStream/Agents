//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	dlctelnyx "github.com/GetStream/Vision-Agents/acceleration/internal/dlc/telnyx"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// campaignRegistry is Telnyx's 10DLC API as far as the router uses it. Every brand is
// verified at once, every campaign waits until a test provisions it, and the numbers
// assigned to each are kept for a test to read back. Ids are fresh UUIDs, as Postgres keeps
// the campaigns of every run before.
type campaignRegistry struct {
	mu        sync.Mutex
	campaigns map[string]string
	assigned  map[string]string
	// brandAnswers and campaignAnswers are the statuses an app's next registrations are
	// answered with, each registered whatever the answer; past them every one is answered 200.
	// brandsAsked and campaignsAsked count each app's registrations asked for. A poll asks
	// about every app's use cases, so each test reads its own app's: one known by its
	// profile's contact email, and a campaign by its brand's app.
	brandAnswers, campaignAnswers map[string][]int
	brandsAsked, campaignsAsked   map[string]int
	// brands are the brand registered for each app and owners the app of each brand, and
	// listed the campaigns a list of a brand's answers, with the reference each carried.
	brands, owners map[string]string
	listed         []listedCampaign
}

// listedCampaign is a campaign as a list of its brand's names it.
type listedCampaign struct{ id, brand, reference string }

// campaignRegistryFails is a campaign status no vendor sends: the fake answers a read of a
// campaign set to it with a 500.
const campaignRegistryFails = "REGISTRY_FAILS"

func (r *campaignRegistry) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var body map[string]string
	raw, _ := io.ReadAll(request.Body)
	_ = json.Unmarshal(raw, &body)
	path := request.URL.Path
	switch {
	case request.Method == http.MethodPost && path == "/v2/10dlc/brand":
		app := body["email"]
		r.brandsAsked[app]++
		if answer := nextAnswer(r.brandAnswers, app); answer != http.StatusOK {
			w.WriteHeader(answer)
			return
		}
		r.brands[app] = uuid.NewString()
		r.owners[r.brands[app]] = app
		fmt.Fprintf(w, `{"brandId":%q,"status":"OK","identityStatus":"VERIFIED"}`, r.brands[app])
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/v2/10dlc/brand/"):
		fmt.Fprintf(w, `{"brandId":%q,"status":"OK","identityStatus":"VERIFIED"}`, strings.TrimPrefix(path, "/v2/10dlc/brand/"))
	case request.Method == http.MethodPost && path == "/v2/10dlc/campaignBuilder":
		app := r.owners[body["brandId"]]
		r.campaignsAsked[app]++
		id := uuid.NewString()
		r.campaigns[id] = "TCR_PENDING"
		r.listed = append(r.listed, listedCampaign{id: id, brand: body["brandId"], reference: body["referenceId"]})
		if answer := nextAnswer(r.campaignAnswers, app); answer != http.StatusOK {
			w.WriteHeader(answer)
			return
		}
		fmt.Fprintf(w, `{"campaignId":%q,"campaignStatus":"TCR_PENDING"}`, id)
	case request.Method == http.MethodGet && path == "/v2/10dlc/campaign":
		var records []string
		for _, listed := range r.listed {
			if listed.brand == request.URL.Query().Get("brandId") {
				records = append(records, fmt.Sprintf(`{"campaignId":%q,"campaignStatus":%q,"referenceId":%q}`,
					listed.id, r.campaigns[listed.id], listed.reference))
			}
		}
		fmt.Fprintf(w, `{"records":[%s],"page":1,"totalRecords":%d}`, strings.Join(records, ","), len(records))
	case request.Method == http.MethodGet && strings.HasPrefix(path, "/v2/10dlc/campaign/"):
		id := strings.TrimPrefix(path, "/v2/10dlc/campaign/")
		if r.campaigns[id] == campaignRegistryFails {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, `{"campaignId":%q,"campaignStatus":%q}`, id, r.campaigns[id])
	case request.Method == http.MethodPost && path == "/v2/10dlc/phone_number_campaigns":
		r.assigned[body["phoneNumber"]] = body["campaignId"]
		_, _ = io.WriteString(w, `{}`)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// nextAnswer takes the first of app's answers, or 200 when there is none.
func nextAnswer(answers map[string][]int, app string) int {
	if len(answers[app]) == 0 {
		return http.StatusOK
	}
	answer := answers[app][0]
	answers[app] = answers[app][1:]
	return answer
}

// answer has app's next brand and campaign registrations answered with brands and campaigns,
// and reports how many of each it asked for so far.
func (r *campaignRegistry) answer(app string, brands, campaigns []int) (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.brandAnswers[app], r.campaignAnswers[app] = brands, campaigns
	return r.brandsAsked[app], r.campaignsAsked[app]
}

// asked is how many brand and campaign registrations app asked for so far.
func (r *campaignRegistry) asked(app string) (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.brandsAsked[app], r.campaignsAsked[app]
}

// campaignFor is the campaign registered with reference, empty when there is none.
func (r *campaignRegistry) campaignFor(reference string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, listed := range r.listed {
		if listed.reference == reference {
			return listed.id
		}
	}
	return ""
}

// brandOf is the brand registered for app.
func (r *campaignRegistry) brandOf(app string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.brands[app]
}

// unlist forgets every campaign registered for app so far, as registrations Telnyx never
// carried out.
func (r *campaignRegistry) unlist(app string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listed = slices.DeleteFunc(r.listed, func(listed listedCampaign) bool { return r.owners[listed.brand] == app })
}

func (r *campaignRegistry) set(campaignID, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.campaigns[campaignID] = status
}

func (r *campaignRegistry) campaignOf(e164 string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.assigned[e164]
}

// DLCSuite is an app registering for 10DLC on the hosted router: sandboxed until a use case
// is approved, reviewed by Stream staff, registered with Telnyx.
type DLCSuite struct {
	dlcRegistration
}

func TestDLCSuite(t *testing.T) {
	runSuite(t, new(DLCSuite))
}

// dlcRegistration is the harness DLCSuite and DLCBookingSuite share.
type dlcRegistration struct {
	RouterSuite
	registry *campaignRegistry
	signing  ed25519.PrivateKey
	// staff is Stream's own review tool, holding the ops key and no app's credentials.
	staff *testClient
	// logged is what the router logged, at debug and up.
	logged *lockedLog
	// holding is whether the test running stored use cases through held.
	holding bool
}

func (s *dlcRegistration) SetupSuite() {
	s.registry = &campaignRegistry{campaigns: map[string]string{}, assigned: map[string]string{},
		brandAnswers: map[string][]int{}, campaignAnswers: map[string][]int{}, brandsAsked: map[string]int{},
		campaignsAsked: map[string]int{}, brands: map[string]string{}, owners: map[string]string{}}
	fake := httptest.NewServer(s.registry)
	s.T().Cleanup(fake.Close)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	s.Require().NoError(err)
	s.signing = private
	s.registrar, err = dlctelnyx.New(dlctelnyx.Options{
		APIKey: "key", PublicKey: base64.StdEncoding.EncodeToString(public), BaseURL: fake.URL,
	})
	s.Require().NoError(err)
	s.sandbox = dlc.Sandbox{Enabled: true, Recipients: 2, MessagesPerDay: 30, AudioMinutesPerDay: 30}
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
}

// Every test is an app of its own, so what one spent of its sandbox is not another's.
func (s *dlcRegistration) SetupTest() {
	s.useApp(s.data.createApp())
	s.staff = &testClient{suite: &s.RouterSuite, header: http.Header{opsKeyHeader: {suiteOpsKey}}, kind: noCredential}
}

func (s *dlcRegistration) completeProfile() {
	address := store.PostalAddress{Street: "1 Main St", City: "Boulder", State: "CO", PostalCode: "80301", Country: "US"}
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/business-profile", BusinessProfileRequest{
		LegalBusinessName: "Acme Inc", LegalEntityType: "corporation", OrganizationType: "private",
		BusinessRegistrationCountry: "US", TaxId: "12-3456789", RegisteredAddress: &address,
		WebsiteUrl: "https://acme.example", Industry: "technology",
		AuthorizedContactEmail: s.contactEmail(), AuthorizedContactPhone: "+15551230000",
	}, nil))
}

// contactEmail is the app's profile's contact email, which tells the fake registry its
// registrations from other apps'.
func (s *dlcRegistration) contactEmail() string {
	return "ops+" + s.customerID() + "@acme.example"
}

func completeUseCase(name string) UseCaseRequest {
	return UseCaseRequest{
		Name:           name,
		UseCaseType:    "CUSTOMER_CARE",
		Description:    "Order updates and support replies for Acme customers who wrote in.",
		MessageFlow:    "Customers opt in by ticking a box on the checkout page at acme.example.",
		MessageSamples: []string{"Your order 123 has shipped.", "Reply HELP for help, STOP to stop."},
		HelpMessage:    "Acme support: help@acme.example",
		OptOutMessage:  "You will receive no more messages from Acme.",
	}
}

// submitted is a complete use case the app sent to Stream's review.
func (s *dlcRegistration) submitted() UseCase {
	s.completeProfile()
	var created, sent UseCase
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/phone/use-cases", completeUseCase("support"), &created))
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/phone/use-cases/"+created.Id+"/submit", nil, &sent))
	return sent
}

func (s *dlcRegistration) review(id string, decision ReviewDecision, notes string) (int, UseCaseForReview) {
	var reviewed UseCaseForReview
	status := s.staff.do(http.MethodPost, "/v1/ops/use-cases/"+id+"/review",
		ReviewUseCaseRequest{Decision: decision, Notes: notes, Reviewer: "reviewer@getstream.io"}, &reviewed)
	return status, reviewed
}

// report posts what Telnyx would send about a campaign, signed with the suite's key.
func (s *dlcRegistration) report(body string, signer ed25519.PrivateKey) int {
	status, err := s.sendReport(body, signer)
	s.Require().NoError(err)
	return status
}

// sendReport is report for a goroutine other than the test's, which must not call Require.
func (s *dlcRegistration) sendReport(body string, signer ed25519.PrivateKey) (int, error) {
	seconds := strconv.FormatInt(time.Now().Unix(), 10)
	request, err := http.NewRequest(http.MethodPost, s.server.URL+dlc.HookPath, bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Telnyx-Timestamp", seconds)
	request.Header.Set("Telnyx-Signature-Ed25519", base64.StdEncoding.EncodeToString(
		ed25519.Sign(signer, []byte(seconds+"|"+body))))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

func (s *dlcRegistration) dial(to string) (int, string) {
	return s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{From: "+15550000001", To: to})
}

func (s *dlcRegistration) transfer(to string) (int, string) {
	return s.serverClient.failure(http.MethodPost, "/v1/phone/calls/transfer",
		TransferCallRequest{From: "+15550000001", To: to, SessionId: s.data.storedSession()})
}

func (s *dlcRegistration) spend(field string, amount int) {
	key := "dlc:" + time.Now().UTC().Format("20060102") + ":" + s.customerID()
	redis := s.live.Redis()
	s.Require().NoError(redis.Do(context.Background(),
		redis.B().Hincrby().Key(key).Field(field).Increment(int64(amount)).Build()).Error())
}

func (s *DLCSuite) TestAUseCaseIsApprovedByStreamAndThenByTheVendor() {
	number := store.PhoneNumber{CustomerID: s.customerID(), E164: s.utils.number(), Vendor: "telnyx"}
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &number))
	sent := s.submitted()
	s.Equal(UseCaseStatus(dlc.Submitted), sent.Status)

	status, reviewed := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.Equal(UseCaseStatus(dlc.VendorPending), reviewed.Status)
	s.Require().NotNil(reviewed.VendorCampaignId, "the brand is verified, so the campaign is registered at once")

	s.registry.set(*reviewed.VendorCampaignId, "MNO_PROVISIONED")
	s.Equal(http.StatusNoContent, s.report(`{"campaignId":"`+*reviewed.VendorCampaignId+`"}`, s.signing))

	var approved UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+sent.Id, nil, &approved))
	s.Equal(UseCaseStatus(dlc.Approved), approved.Status)
	s.Equal(*reviewed.VendorCampaignId, s.registry.campaignOf(number.E164), "the default sends from every number")

	var history ReviewPage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+sent.Id+"/reviews", nil, &history))
	var actors []ReviewActor
	for _, review := range history.Items {
		actors = append(actors, review.Actor)
	}
	s.Equal([]ReviewActor{dlc.ActorApp, dlc.ActorStaff, dlc.ActorVendor, dlc.ActorVendor}, actors)
	s.Equal(UseCaseStatus(dlc.Approved), history.Items[len(history.Items)-1].ToStatus)

	var sandbox PhoneSandbox
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/sandbox", nil, &sandbox))
	s.False(sandbox.Sandboxed)
	dialed, failure := s.dial("+15559990000")
	s.Equal(http.StatusBadRequest, dialed, "past the gate, the call fails for want of Stream: "+failure)
	s.Contains(failure, "no Stream app")
}

// A brand Telnyx refused registered nothing, and is asked for again. One whose registration was
// answered with a failure may be registered, and billed, all the same, so it is not asked for
// again, however often the poller looks.
func (s *DLCSuite) TestABrandWhoseRegistrationWasNotAnsweredIsNotRegisteredTwice() {
	brands, _ := s.registry.answer(s.contactEmail(), []int{http.StatusBadRequest, http.StatusInternalServerError}, nil)
	sent := s.submitted()

	status, reviewed := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.Equal(UseCaseStatus(dlc.VendorPending), reviewed.Status)
	s.registrar10DLC.Poll(context.Background()) // asks again after the refusal
	s.registrar10DLC.Poll(context.Background())

	asked, _ := s.registry.asked(s.contactEmail())
	s.Equal(2, asked-brands, "refused, then asked once more")
	s.Empty(s.loggedLines("a 10DLC registration was not answered", s.customerID()), "one that may still be registering is not reported lost")
}

// A brand whose registration was not answered, and still has no answer once it could no longer
// be running, may be registered and billed at Telnyx, which keeps no reference of ours to find
// it by: the use case is handed back to the app as vendor_rejected, which it may delete, and the
// brand is not asked for again until support has looked it up.
func (s *DLCSuite) TestABrandWhoseAnswerWasLostIsLeftToSupportAndFreesTheUseCase() {
	brands, _ := s.registry.answer(s.contactEmail(), []int{http.StatusInternalServerError}, nil)
	sent := s.submitted()
	status, _ := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.backdate("business_profiles", "brand_requested_at", "customer_id", s.customerID())

	s.registrar10DLC.Poll(context.Background())
	s.registrar10DLC.Poll(context.Background())

	s.Equal(UseCaseStatus(dlc.VendorRejected), s.useCase(sent.Id).Status)
	asked, _ := s.registry.asked(s.contactEmail())
	s.Equal(1, asked-brands, "never asked for again")
	s.Len(s.loggedLines("a 10DLC registration was not answered", s.customerID()), 1)
	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/use-cases/"+sent.Id, nil, nil))
}

// A campaign whose registration was not answered is found at Telnyx by the use case it was
// registered for, and recorded, without asking for a second one.
func (s *DLCSuite) TestACampaignWhoseAnswerWasLostIsFoundAtTheVendor() {
	_, campaigns := s.registry.answer(s.contactEmail(), nil, []int{http.StatusInternalServerError})
	sent := s.submitted()
	status, reviewed := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.Require().Nil(reviewed.VendorCampaignId)
	s.backdate("dlc_use_cases", "campaign_requested_at", "id", sent.Id)

	s.registrar10DLC.Poll(context.Background())

	found := s.useCase(sent.Id)
	s.Require().NotNil(found.VendorCampaignId)
	s.Equal(s.registry.campaignFor(sent.Id), *found.VendorCampaignId)
	_, asked := s.registry.asked(s.contactEmail())
	s.Equal(1, asked-campaigns)
}

// A campaign Telnyx does not hold, its registration answered with a failure, was never
// registered: it is asked for again.
func (s *DLCSuite) TestACampaignTheVendorNeverRegisteredIsAskedForAgain() {
	_, campaigns := s.registry.answer(s.contactEmail(), nil, []int{http.StatusInternalServerError})
	sent := s.submitted()
	status, _ := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.registry.unlist(s.contactEmail())
	s.backdate("dlc_use_cases", "campaign_requested_at", "id", sent.Id)

	s.registrar10DLC.Poll(context.Background())
	s.registrar10DLC.Poll(context.Background())

	found := s.useCase(sent.Id)
	s.Require().NotNil(found.VendorCampaignId)
	s.Equal(s.registry.campaignFor(sent.Id), *found.VendorCampaignId)
	_, asked := s.registry.asked(s.contactEmail())
	s.Equal(2, asked-campaigns)
}

// A brand Telnyx registered whose id the database would not take is logged with its id, so
// support can record it, and is not asked for again.
func (s *DLCSuite) TestABrandRegisteredWhoseIdCouldNotBeSavedIsLoggedWithIt() {
	s.failQueries(`SET vendor_brand_id = '`)
	sent := s.submitted()

	status, _ := s.review(sent.Id, dlc.Approve, "")

	s.Require().Equal(http.StatusOK, status)
	line := s.loggedLine("could not save a brand the vendor registered", s.customerID())
	s.Contains(line, "level=ERROR")
	s.Contains(line, "vendor_brand_id="+s.registry.brandOf(s.contactEmail()))
}

// A campaign Telnyx registered whose save the database failed is logged with its id, and found
// at Telnyx by the poller once the database is back.
func (s *DLCSuite) TestACampaignRegisteredWhoseSaveFailedIsFoundAtTheVendorLater() {
	_, campaigns := s.registry.asked(s.contactEmail())
	s.failQueries(registeredNote)
	sent := s.submitted()
	status, _ := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	line := s.loggedLine("could not save a campaign the vendor registered", sent.Id)
	s.Contains(line, "level=ERROR")
	s.Contains(line, "vendor_campaign_id="+s.registry.campaignFor(sent.Id))
	s.failQueries("no query holds this")
	s.backdate("dlc_use_cases", "campaign_requested_at", "id", sent.Id)

	s.registrar10DLC.Poll(context.Background())

	found := s.useCase(sent.Id)
	s.Require().NotNil(found.VendorCampaignId)
	s.Equal(s.registry.campaignFor(sent.Id), *found.VendorCampaignId)
	_, asked := s.registry.asked(s.contactEmail())
	s.Equal(1, asked-campaigns)
}

// registeredNote begins the review entry a campaign's registration writes.
const registeredNote = "Registered as campaign "

// backdate moves a registration's request time past the time one may still be running, as if
// it had been asked for long ago.
func (s *dlcRegistration) backdate(table, column, key, value string) {
	_, err := s.store.GlobalDB().NewUpdate().Table(table).
		Set("? = ? - interval '11 minutes'", bun.Ident(column), bun.Ident(column)).
		Where("? = ?", bun.Ident(key), value).
		Exec(context.Background())
	s.Require().NoError(err)
}

// loggedLines are the lines logged with message that name about, the suite's tests sharing
// one log.
func (s *dlcRegistration) loggedLines(message, about string) []string {
	var found []string
	for line := range strings.Lines(s.logged.String()) {
		if strings.Contains(line, `msg="`+message) && strings.Contains(line, "="+about+" ") {
			found = append(found, line)
		}
	}
	return found
}

// loggedLine is the one line logged with message that names about.
func (s *dlcRegistration) loggedLine(message, about string) string {
	found := s.loggedLines(message, about)
	s.Require().Len(found, 1, "one line %q about %s", message, about)
	return found[0]
}

// useCase is one of the app's use cases.
func (s *dlcRegistration) useCase(id string) UseCase {
	var found UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+id, nil, &found))
	return found
}

// A campaign whose registration was answered with a failure may be registered, and billed, all
// the same, so it is not asked for again.
func (s *DLCSuite) TestACampaignWhoseRegistrationWasNotAnsweredIsNotRegisteredTwice() {
	_, campaigns := s.registry.answer(s.contactEmail(), nil, []int{http.StatusInternalServerError})
	sent := s.submitted()

	status, reviewed := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.Nil(reviewed.VendorCampaignId)
	s.registrar10DLC.Poll(context.Background())

	_, asked := s.registry.asked(s.contactEmail())
	s.Equal(1, asked-campaigns)
}

// pendingWithTheVendor is a use case Stream approved and Telnyx holds a campaign for.
func (s *dlcRegistration) pendingWithTheVendor() (string, string) {
	sent := s.submitted()
	status, reviewed := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.Require().Equal(UseCaseStatus(dlc.VendorPending), reviewed.Status)
	s.Require().NotNil(reviewed.VendorCampaignId)
	return sent.Id, *reviewed.VendorCampaignId
}

// campaignsFailed is the count a monitor reads for campaigns Telnyx failed. Only DLCSuite fails
// campaigns, and its tests run one at a time, so a test reads the change its own report made.
func campaignsFailed() float64 { return accountEvents("telnyx", "10dlc.campaign_failed", "warn") }

// A campaign Telnyx rejected cannot send, so it is counted once, when the use case moves to
// vendor_rejected, with one line naming Telnyx's status; Telnyx telling again moves nothing.
// Catches the count missing, counted at another level, logged without the status, or counted
// again on a repeated report.
func (s *DLCSuite) TestACampaignTheVendorRejectedIsCountedOnce() {
	since := len(s.logged.String())
	id, campaign := s.pendingWithTheVendor()
	before := campaignsFailed()
	s.registry.set(campaign, "MNO_REJECTED")

	s.Equal(http.StatusNoContent, s.report(`{"campaignId":"`+campaign+`"}`, s.signing))
	s.Equal(http.StatusNoContent, s.report(`{"campaignId":"`+campaign+`"}`, s.signing))

	var rejected UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+id, nil, &rejected))
	s.Equal(UseCaseStatus(dlc.VendorRejected), rejected.Status)
	s.Equal(1.0, campaignsFailed()-before)
	lines := 0
	for _, line := range strings.Split(s.logged.String()[since:], "\n") {
		if strings.Contains(line, "a vendor failed a 10DLC campaign") {
			lines++
			s.Contains(line, "level=WARN ")
			s.Contains(line, "vendor=telnyx event=10dlc.campaign_failed event_level=warn use_case="+id+" vendor_status=MNO_REJECTED")
		}
	}
	s.Equal(1, lines)
}

// A rejection the hook missed is found by the poller, and it counts the same.
// Catches the count made only on the hook's report.
func (s *DLCSuite) TestARejectionThePollerFindsIsCountedOnce() {
	id, campaign := s.pendingWithTheVendor()
	before := campaignsFailed()
	s.registry.set(campaign, "MNO_REJECTED")
	s.registrar10DLC.Poll(context.Background())
	s.registrar10DLC.Poll(context.Background())
	var rejected UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+id, nil, &rejected))
	s.Equal(UseCaseStatus(dlc.VendorRejected), rejected.Status)
	s.Equal(1.0, campaignsFailed()-before)
}

// Use cases past the first page of those the vendor holds are still asked about: one Poll
// reads them all. They are dated before any other test's, so the 55th and the 230th of 250
// are the 55th and 230th the poll reads, past the 51 a poll once read and past the first page
// of the poll's own paging. Catches a poll that reads one page and stops, or a cursor that
// does not carry it to the next.
func (s *DLCSuite) TestOnePollReachesAUseCasePastTheFirstPage() {
	held := s.held(s.customerID(), 250, 54, 229)

	s.registrar10DLC.Poll(context.Background())

	for _, i := range []int{54, 229} {
		var read UseCase
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+held[i].ID, nil, &read))
		s.Equal(UseCaseStatus(dlc.Approved), read.Status, "use case %d", i+1)
	}
}

// heldSince dates the use cases held stores before any other test's, so a poll reads them first.
var heldSince = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// held stores n use cases of customerID the vendor holds, a second apart from heldSince on,
// which the vendor reports as provisioned at the indexes in provisioned and as pending
// otherwise. Every app's held use cases are polled, so they are deleted when the test ends,
// and any a killed run left are deleted before the test's first.
func (s *dlcRegistration) held(customerID string, n int, provisioned ...int) []store.UseCase {
	ctx := context.Background()
	forget := func() {
		_, err := s.store.GlobalDB().NewDelete().Model((*store.UseCase)(nil)).
			Where("name LIKE 'held %'").Where("created_at < ?", heldSince.AddDate(1, 0, 0)).Exec(context.Background())
		s.NoError(err)
	}
	if !s.holding {
		forget()
		s.holding = true
		s.T().Cleanup(func() {
			forget()
			s.holding = false
		})
	}
	useCases := make([]store.UseCase, n)
	for i := range useCases {
		campaign := uuid.NewString()
		status := "TCR_PENDING"
		if slices.Contains(provisioned, i) {
			status = "MNO_PROVISIONED"
		}
		s.registry.set(campaign, status)
		at := heldSince.Add(time.Duration(i) * time.Second)
		useCases[i] = store.UseCase{
			ID: strings.ReplaceAll(uuid.NewString(), "-", ""), CustomerID: customerID, Name: fmt.Sprintf("held %d", i),
			Status: dlc.VendorPending, UseCaseType: "CUSTOMER_CARE", MessageSamples: []string{},
			Vendor: "telnyx", VendorCampaignID: campaign, VendorStatus: "TCR_PENDING", CreatedAt: at, UpdatedAt: at,
		}
	}
	_, err := s.store.GlobalDB().NewInsert().Model(&useCases).Exec(ctx)
	s.Require().NoError(err)
	return useCases
}

// statusOf is where a use case stands now.
func (s *dlcRegistration) statusOf(useCase store.UseCase) string {
	read, err := s.store.UseCases.GetByID(context.Background(), useCase.ID)
	s.Require().NoError(err)
	return read.Status
}

// DLCPollSuite pages the poll through the use cases a vendor holds, two at a time and two pages
// a pass, in a database of its own: a pass that reached the end depends on what else is held.
type DLCPollSuite struct {
	dlcRegistration
}

func TestDLCPollSuite(t *testing.T) {
	polls := new(DLCPollSuite)
	polls.database = "dlc_poll"
	runSuite(t, polls)
}

// pollingService polls two use cases a page and two pages a pass.
func (s *DLCPollSuite) pollingService() *dlc.Service {
	service, err := dlc.NewService(dlc.Options{Store: s.store, Registrar: s.registrar, PollPage: 2, PollPages: 2,
		Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	return service
}

// A pass the page cap cuts short leaves the next to carry on where it stopped, and a pass that
// reached the end leaves the next to start from the top. Catches a pass that always starts
// from the top, so the fifth is never asked about, and a cursor kept past the end.
func (s *DLCPollSuite) TestAPassCutShortIsCarriedOnAndOneThatEndedStartsAgain() {
	held := s.held(s.customerID(), 5, 4)
	service := s.pollingService()

	service.Poll(context.Background())
	s.Equal(dlc.VendorPending, s.statusOf(held[4]), "the first pass reads four")

	service.Poll(context.Background())
	s.Equal(dlc.Approved, s.statusOf(held[4]), "the second carries on from the fifth")

	s.registry.set(held[0].VendorCampaignID, "MNO_PROVISIONED")
	service.Poll(context.Background())
	s.Equal(dlc.Approved, s.statusOf(held[0]), "the third starts from the top")
}

// A page that cannot be read ends the pass where it was, so the next carries on from there.
// Catches a failed read that throws the cursor away, which starts the next pass from the top.
func (s *DLCPollSuite) TestAPageThatCannotBeReadKeepsTheCursor() {
	held := s.held(s.customerID(), 5, 4)
	service := s.pollingService()
	service.Poll(context.Background())

	// Only a read that carries on from a cursor names one.
	s.failQueries("(updated_at, id) >")
	service.Poll(context.Background())
	s.Equal(dlc.VendorPending, s.statusOf(held[4]))
	s.failing.value.Store(nil)

	service.Poll(context.Background())
	s.Equal(dlc.Approved, s.statusOf(held[4]))
}

// A report on a brand registers what waited on it: the use cases the vendor holds of that
// brand's app, and no other app's. Catches a report that polls every app's, or none.
func (s *DLCPollSuite) TestABrandReportRefreshesOnlyItsAppsUseCases() {
	ctx := context.Background()
	s.completeProfile()
	brand := uuid.NewString()
	s.Require().NoError(s.store.BusinessProfiles.SetBrand(ctx, s.customerID(), brand, "VERIFIED"))
	held := s.held(s.customerID(), 2, 0, 1)
	other := s.held("customer-"+uuid.NewString(), 1, 0)

	s.Equal(http.StatusNoContent, s.report(`{"brandId":"`+brand+`"}`, s.signing))

	s.Equal(dlc.Approved, s.statusOf(held[0]))
	s.Equal(dlc.Approved, s.statusOf(held[1]))
	s.Equal(dlc.VendorPending, s.statusOf(other[0]), "another app's is left to the poll")
}

// Telnyx retries a report and the poller reads the same campaign, so several refreshes can
// find the use case pending at once; only the one whose move is stored counts. Catches the
// count made before the move, or after a move another refresh won.
func (s *DLCSuite) TestARejectionReportedManyTimesAtOnceIsCountedOnce() {
	id, campaign := s.pendingWithTheVendor()
	before := campaignsFailed()
	s.registry.set(campaign, "MNO_REJECTED")

	var reports sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		reports.Go(func() {
			if _, err := s.sendReport(`{"campaignId":"`+campaign+`"}`, s.signing); err != nil {
				failures <- err
			}
		})
	}
	reports.Wait()
	close(failures)

	for err := range failures {
		s.NoError(err)
	}
	var rejected UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+id, nil, &rejected))
	s.Equal(UseCaseStatus(dlc.VendorRejected), rejected.Status)
	s.Equal(1.0, campaignsFailed()-before)
}

// Every failed status Telnyx has is counted, not only a rejection. Catches a count tied to one
// status rather than to the outcome.
func (s *DLCSuite) TestACampaignTheVendorSuspendedIsCounted() {
	_, campaign := s.pendingWithTheVendor()
	before := campaignsFailed()
	s.registry.set(campaign, "TCR_SUSPENDED")

	s.Equal(http.StatusNoContent, s.report(`{"campaignId":"`+campaign+`"}`, s.signing))

	s.Equal(1.0, campaignsFailed()-before)
}

// A campaign carriers provisioned, or one still on its way, failed nothing. Catches a count on
// every move of a use case.
func (s *DLCSuite) TestACampaignStillPendingOrProvisionedIsNotCounted() {
	id, campaign := s.pendingWithTheVendor()
	before := campaignsFailed()

	s.registry.set(campaign, "TCR_ACCEPTED")
	s.Equal(http.StatusNoContent, s.report(`{"campaignId":"`+campaign+`"}`, s.signing))
	s.registry.set(campaign, "MNO_PROVISIONED")
	s.Equal(http.StatusNoContent, s.report(`{"campaignId":"`+campaign+`"}`, s.signing))

	var approved UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases/"+id, nil, &approved))
	s.Equal(UseCaseStatus(dlc.Approved), approved.Status)
	s.Equal(0.0, campaignsFailed()-before)
}

// The report only names the campaign: what it claims is asked of Telnyx, so a forged one
// moves nothing even before the signature is checked.
func (s *DLCSuite) TestAReportNotSignedByTheVendorIsRefused() {
	_, stranger, err := ed25519.GenerateKey(rand.Reader)
	s.Require().NoError(err)

	s.Equal(http.StatusUnauthorized, s.report(`{"campaignId":"C1"}`, stranger))
}

// Why a report is refused can name the deployment's settings: it goes to the log, and the
// sender, which needs no credential, is told only that the report was refused.
func (s *DLCSuite) TestARefusedReportIsAnsweredWithoutTheReason() {
	_, stranger, err := ed25519.GenerateKey(rand.Reader)
	s.Require().NoError(err)
	body := `{"campaignId":"C1"}`
	seconds := strconv.FormatInt(time.Now().Unix(), 10)
	request, err := http.NewRequest(http.MethodPost, s.server.URL+dlc.HookPath, bytes.NewBufferString(body))
	s.Require().NoError(err)
	request.Header.Set("Telnyx-Timestamp", seconds)
	request.Header.Set("Telnyx-Signature-Ed25519", base64.StdEncoding.EncodeToString(
		ed25519.Sign(stranger, []byte(seconds+"|"+body))))
	before := len(s.logged.String())

	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answered, err := io.ReadAll(response.Body)
	s.Require().NoError(err)

	s.Equal(http.StatusUnauthorized, response.StatusCode)
	var failure ErrorResponse
	s.Require().NoError(json.Unmarshal(answered, &failure), string(answered))
	s.Equal("that is not a report from the registrar", failure.Error.Message)
	s.NotContains(string(answered), "dlc:")
	s.Contains(s.logged.String()[before:], "dlc: refused", "the operator still learns why")
}

// A number the app does not hold is the app's to correct, and the answer names it.
func (s *DLCSuite) TestAUseCaseNamingANumberTheAppDoesNotHoldIsRefusedNamingIt() {
	held := store.PhoneNumber{CustomerID: s.customerID(), E164: s.utils.number(), Vendor: "telnyx"}
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &held))
	stranger := s.utils.number()
	request := completeUseCase("support")
	request.Numbers = []string{held.E164, stranger}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/use-cases", request)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, stranger+" is not a number this app holds")
	s.NotContains(failure, "store:")
	s.Empty(s.useCases(), "a refused create leaves no use case")
}

// useCases are the app's use cases.
func (s *dlcRegistration) useCases() []UseCase {
	var page UseCasePage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/use-cases", nil, &page))
	return page.Items
}

// Assigning numbers that the database fails is the router's failure: a 500 that says nothing
// of the database, not a 400 the app could not act on. The use case is not made without them.
func (s *DLCSuite) TestAUseCaseWhoseNumbersTheDatabaseFailsIsAnInternalError() {
	held := store.PhoneNumber{CustomerID: s.customerID(), E164: s.utils.number(), Vendor: "telnyx"}
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &held))
	request := completeUseCase("support")
	request.Numbers = []string{held.E164}
	s.failQueries("SET dlc_use_case_id = '")

	status, payload := s.serverClient.call(http.MethodPost, "/v1/phone/use-cases", request)

	s.Equal(http.StatusInternalServerError, status, string(payload))
	var failure ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &failure), string(payload))
	s.Equal("internal_error", failure.Error.Code)
	s.Equal(somethingWentWrong, failure.Error.Message)
	for _, internal := range []string{"store:", "dlc:", "deadline", "assign numbers"} {
		s.NotContains(string(payload), internal)
	}
	s.Empty(s.useCases(), "a use case without the numbers it was made with is not left behind")
}

func (s *DLCSuite) TestOnlyStreamStaffReviewUseCases() {
	s.Equal(http.StatusUnauthorized, s.serverClient.do(http.MethodGet, "/v1/ops/use-cases", nil, nil),
		"an app's own backend is not staff")
	wrong := &testClient{suite: &s.RouterSuite, header: http.Header{opsKeyHeader: {"guess"}}, kind: noCredential}
	s.Equal(http.StatusUnauthorized, wrong.do(http.MethodGet, "/v1/ops/use-cases", nil, nil))

	sent := s.submitted()
	var queue ReviewQueue
	s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, "/v1/ops/use-cases?limit=200", nil, &queue))
	found := false
	for _, item := range queue.Items {
		if item.Id == sent.Id {
			found = true
			s.Equal(s.customerID(), item.CustomerId)
			s.Require().NotNil(item.BusinessProfile)
			s.Equal("Acme Inc", item.BusinessProfile.LegalBusinessName)
		}
	}
	s.True(found || queue.HasMore, "a submitted use case waits in the queue")
}

func (s *DLCSuite) TestAUseCaseHandedBackIsEditedAndSubmittedAgain() {
	sent := s.submitted()
	edited := completeUseCase("support, take two")
	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPut, "/v1/phone/use-cases/"+sent.Id, edited, nil),
		"Stream is reading it")

	status, _ := s.review(sent.Id, dlc.RequestChanges, "")
	s.Equal(http.StatusBadRequest, status, "the app is told what to change")
	status, reviewed := s.review(sent.Id, dlc.RequestChanges, "Say where the opt-in checkbox is.")
	s.Require().Equal(http.StatusOK, status)
	s.Equal(UseCaseStatus(dlc.ChangesRequested), reviewed.Status)

	s.Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/use-cases/"+sent.Id, edited, nil))
	var again UseCase
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/phone/use-cases/"+sent.Id+"/submit", nil, &again))
	s.Equal(UseCaseStatus(dlc.Submitted), again.Status)
	s.Equal("support, take two", again.Name)
}

func (s *DLCSuite) TestAUseCaseMissingWhatTheRegistryAsksForIsNotSubmitted() {
	var created UseCase
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/phone/use-cases", UseCaseRequest{Name: "bare"}, &created))
	s.Equal(UseCaseStatus(dlc.Draft), created.Status)
	s.True(created.IsDefault, "an app's first use case is its default")

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/use-cases/"+created.Id+"/submit", nil)
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "business profile")

	s.completeProfile()
	status, failure = s.serverClient.failure(http.MethodPost, "/v1/phone/use-cases/"+created.Id+"/submit", nil)
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "message_samples")
}

func (s *DLCSuite) TestOnlyABackendManagesUseCases() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/phone/use-cases", UseCaseRequest{Name: "posture"}, nil)
	})
}

func (s *DLCSuite) TestAUseCaseIsHiddenFromOtherApps() {
	var created UseCase
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/phone/use-cases", UseCaseRequest{Name: "mine"}, &created))

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/phone/use-cases/"+created.Id, nil, nil)
	})
}

func (s *DLCSuite) TestADialToSomebodyWhoOptedOutIsRefused() {
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/sandbox/recipients",
		SetSandboxRecipientsRequest{Recipients: []string{"+15551110000"}}, nil))
	var optOut OptOut
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/phone/opt-outs",
		CreateOptOutRequest{Recipient: "+15551110000", Channel: OptOutChannel(dlc.Voice)}, &optOut))

	status, failure := s.dial("+15551110000")
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "opted out")

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/opt-outs/"+optOut.Id, nil, nil))
	status, failure = s.dial("+15551110000")
	s.Equal(http.StatusBadRequest, status, "past the gate, the call fails for want of Stream")
	s.Contains(failure, "no Stream app")
}

func (s *DLCSuite) TestASandboxedAppReachesOnlyItsTwoRecipients() {
	three := SetSandboxRecipientsRequest{Recipients: []string{"+15551110000", "+15552220000", "+15553330000"}}
	s.Equal(http.StatusBadRequest, s.serverClient.do(http.MethodPut, "/v1/phone/sandbox/recipients", three, nil))
	two := SetSandboxRecipientsRequest{Recipients: three.Recipients[:2]}
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/sandbox/recipients", two, nil))

	status, failure := s.dial("+15553330000")
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "sandbox recipients")
	status, failure = s.dial("+15551110000")
	s.Equal(http.StatusBadRequest, status, "a recipient passes the gate")
	s.Contains(failure, "no Stream app")
}

func (s *DLCSuite) TestATransferToSomebodyWhoOptedOutIsRefused() {
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/sandbox/recipients",
		SetSandboxRecipientsRequest{Recipients: []string{"+15551110000"}}, nil))
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/phone/opt-outs",
		CreateOptOutRequest{Recipient: "+15551110000", Channel: OptOutChannel(dlc.Voice)}, nil))

	status, failure := s.transfer("+15551110000")
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "opted out")
}

func (s *DLCSuite) TestASandboxedAppTransfersOnlyToItsRecipients() {
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/sandbox/recipients",
		SetSandboxRecipientsRequest{Recipients: []string{"+15551110000"}}, nil))

	status, failure := s.transfer("+15553330000")
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "sandbox recipients")
	status, failure = s.transfer("+15551110000")
	s.Equal(http.StatusBadRequest, status, "a recipient passes the gate")
	s.Contains(failure, "no Stream app")
}

func (s *DLCSuite) TestThirtyMinutesOfCallsInADayRefuseTheNextDial() {
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/sandbox/recipients",
		SetSandboxRecipientsRequest{Recipients: []string{"+15551110000"}}, nil))
	s.spend("audio_seconds", 30*60)

	status, failure := s.dial("+15551110000")
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "30 minutes")
	var sandbox PhoneSandbox
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/sandbox", nil, &sandbox))
	s.Equal(int64(30*60), sandbox.AudioSecondsToday)
}

// Registering costs a brand fee once, a review per campaign, and the campaign's first three
// months at once; asking the vendor again, or the monthly job running in one of those months,
// bills none of it again.
func (s *DLCSuite) TestARegisteredBrandAndCampaignAreBilledOnce() {
	sent := s.submitted()
	status, reviewed := s.review(sent.Id, dlc.Approve, "")
	s.Require().Equal(http.StatusOK, status)
	s.Require().NotNil(reviewed.VendorCampaignId)

	fees := map[string][]store.Request{}
	read := func() {
		var rows []store.Request
		s.Require().NoError(s.store.ShardDB().NewSelect().Model(&rows).
			Where("customer_id = ?", s.customerID()).Where("modality = 'phone'").Scan(context.Background()))
		fees = map[string][]store.Request{}
		for _, row := range rows {
			fees[row.Model] = append(fees[row.Model], row)
		}
	}
	s.Require().Eventually(func() bool {
		read()
		return len(fees[dlc.ModelCampaignMonth]) == dlc.UpfrontMonths
	}, settleFor, 50*time.Millisecond, "the campaign's first months were not billed")

	s.registrar10DLC.Poll(context.Background())
	billed, err := s.registrar10DLC.BillCampaigns(context.Background(), time.Now().AddDate(0, -2, 0))
	s.Require().NoError(err)
	s.Zero(billed, "the month the job finds is one already billed")
	time.Sleep(500 * time.Millisecond)
	read()
	s.Require().Len(fees[dlc.ModelBrand], 1)
	fee := pricing.Quantities{pricing.Requests: 1}
	s.Equal(s.vendorPriced("telnyx", dlc.ModelBrand, nil, fee), fees[dlc.ModelBrand][0].CostMicros)
	s.Equal(dlc.OriginFees, fees[dlc.ModelBrand][0].APIKey, "Stream's review is no request of the app's")
	s.Require().Len(fees[dlc.ModelCampaign], 1)
	s.Equal(s.vendorPriced("telnyx", dlc.ModelCampaign, nil, fee), fees[dlc.ModelCampaign][0].CostMicros, "the carriers' review")
	customerCare := s.vendorPriced("telnyx", dlc.ModelCampaignMonth,
		map[string]string{dlc.AttributeUseCaseType: "CUSTOMER_CARE"}, fee)
	for _, month := range fees[dlc.ModelCampaignMonth] {
		s.Equal(customerCare, month.CostMicros, "a customer care campaign's month")
		s.True(month.Billable)
	}
}

func (s *DLCSuite) TestTheThirtyFirstMessageOfADayIsRefused() {
	ctx := context.Background()
	s.Require().NoError(s.store.SandboxRecipients.Replace(ctx, s.customerID(), []string{"+15551110000"}))
	s.spend("messages", 29)
	s.NoError(s.gate.Allow(ctx, s.customerID(), dlc.SMS, "+15551110000"))

	s.gate.Sent(ctx, s.customerID(), dlc.SMS, "+15551110000")

	s.ErrorIs(s.gate.Allow(ctx, s.customerID(), dlc.SMS, "+15551110000"), dlc.ErrRefused)
}
