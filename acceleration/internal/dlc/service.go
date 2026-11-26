package dlc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/vendorevent"
)

// HookPath is where a registrar reports on brands and campaigns.
const HookPath = "/v1/phone/hooks/10dlc"

// Service moves use cases through review and registration.
type Service struct {
	store     *store.Store
	registrar Registrar
	hookURL   string
	recorder  *routing.Recorder
	booker    routing.Booker
	clock     func() time.Time
	logger    *slog.Logger

	// pollPage is how many use cases Poll reads at a time, and pollPages how many pages one
	// pass reads at most.
	pollPage, pollPages int
	// pollMu guards pollAfter, where the next Poll starts: set when the last pass stopped at its
	// page cap or at a failed read, nil when it read the last page.
	pollMu    sync.Mutex
	pollAfter *store.CreatedPosition
}

// Options configures a Service. Without a registrar Stream's approval is the last word,
// which is what a deployment registering nothing with any vendor has.
type Options struct {
	Store     *store.Store
	Registrar Registrar
	// PublicURL is where the registrar's reports reach this router.
	PublicURL string
	// Recorder files the registrar's fees as rows: a brand, a campaign's review and each
	// month of a campaign. Nil records none.
	Recorder *routing.Recorder
	// Booker books each fee out of the app's credit before the registrar is paid it, and checks
	// a use case submitted for review could pay them all (Gate.Book). Nil books nothing.
	Booker routing.Booker
	// Now is the clock a campaign's months are counted by. Nil is the wall clock.
	Now func() time.Time
	// PollPage is how many use cases Poll reads at a time, and PollPages how many pages one
	// pass reads at most. Zero is 200 and 10.
	PollPage, PollPages int
	Logger              *slog.Logger
}

// NewService returns a Service.
func NewService(options Options) (*Service, error) {
	if options.Store == nil {
		return nil, errors.New("dlc: a store is required")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	hookURL := ""
	if options.PublicURL != "" {
		hookURL = options.PublicURL + HookPath
	}
	return &Service{store: options.Store, registrar: options.Registrar, hookURL: hookURL,
		recorder: options.Recorder, booker: options.Booker, clock: options.Now, logger: options.Logger,
		pollPage: cmp.Or(options.PollPage, pollPageSize), pollPages: cmp.Or(options.PollPages, pollPages)}, nil
}

// Run checks the use cases a vendor holds every interval until ctx ends, for the reports a
// hook missed.
func (s *Service) Run(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Poll(ctx)
		}
	}
}

// pollPageSize is how many use cases Poll reads at a time, and pollPages how many pages
// one pass asks the vendor about at most.
const (
	pollPageSize = 200
	pollPages    = 10
)

// Poll asks the vendor where the use cases it holds stand, those that moved longest ago
// first, page after page. A poll that finds nothing new writes nothing, so the order never
// changes: a pass the page cap cuts short leaves a cursor for the next one to carry on from,
// or the use cases past the cap would never be asked about.
func (s *Service) Poll(ctx context.Context) {
	if s.registrar == nil {
		return
	}
	// Held for the whole pass: Poll is exported as well as run by Run's ticker, and two passes
	// at once would ask the vendor twice and overwrite each other's cursor.
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	s.pollAfter = s.poll(ctx, s.pollAfter)
}

// poll refreshes up to a pass's pages of the use cases a vendor holds, starting after after,
// and returns where the next pass starts: nil once it read the last page.
func (s *Service) poll(ctx context.Context, after *store.CreatedPosition) *store.CreatedPosition {
	limit := store.DLCLimit(s.pollPage)
	for range s.pollPages {
		// A stopping router ends the pass quietly, where it got to.
		if ctx.Err() != nil {
			return after
		}
		useCases, err := s.store.UseCases.SelectInStatus(ctx, VendorPending, limit, after)
		if err != nil {
			s.logger.Error("could not list the use cases a vendor holds", "error", err)
			return after
		}
		last := len(useCases) <= limit
		if !last {
			useCases = useCases[:limit]
		}
		for i, useCase := range useCases {
			err := s.refresh(ctx, useCase, nil)
			// A stopping router ends the pass here, rather than logging every use case left,
			// and the next pass starts with the one it cut off.
			if ctx.Err() != nil {
				if i == 0 {
					return after
				}
				end := useCases[i-1]
				return &store.CreatedPosition{At: end.UpdatedAt, ID: end.ID}
			}
			if err != nil {
				s.logger.Error("could not refresh a use case", "use_case", useCase.ID, "error", err)
			}
		}
		if last {
			return nil
		}
		end := useCases[len(useCases)-1]
		after = &store.CreatedPosition{At: end.UpdatedAt, ID: end.ID}
	}
	return after
}

// Create records a new draft use case and the numbers that send as it, as one: a use case whose
// numbers cannot be assigned is not left behind.
func (s *Service) Create(ctx context.Context, useCase *store.UseCase, numbers []string) error {
	if err := s.checkNumbers(ctx, useCase.CustomerID, numbers); err != nil {
		return err
	}
	useCase.Status = Draft
	return s.store.UseCases.Insert(ctx, useCase, numbers...)
}

// Update replaces what an app wrote on a use case it may still edit. numbers nil leaves
// the assignment as it was.
func (s *Service) Update(ctx context.Context, useCase *store.UseCase, numbers []string) error {
	if !Editable(useCase.Status) {
		return stack.Wrap(fmt.Errorf("%w: a %s use case cannot be edited", ErrLocked, useCase.Status))
	}
	if err := s.store.UseCases.Update(ctx, useCase); err != nil {
		return err
	}
	if numbers == nil {
		return nil
	}
	return s.assign(ctx, *useCase, numbers)
}

// assign sends numbers as the use case.
func (s *Service) assign(ctx context.Context, useCase store.UseCase, numbers []string) error {
	if err := s.checkNumbers(ctx, useCase.CustomerID, numbers); err != nil {
		return err
	}
	return s.store.UseCases.AssignNumbers(ctx, useCase.CustomerID, useCase.ID, numbers)
}

// checkNumbers refuses numbers a use case of the app could not send from. A number the app
// does not hold, or one named twice, is the caller's to correct; a store failure is not.
func (s *Service) checkNumbers(ctx context.Context, customerID string, numbers []string) error {
	if len(numbers) == 0 {
		return nil
	}
	held, err := s.store.PhoneNumbers.Select(ctx, customerID, store.PhoneNumberFilter{})
	if err != nil {
		return err
	}
	holds := make(map[string]bool, len(held))
	for _, number := range held {
		holds[number.E164] = true
	}
	for i, number := range numbers {
		if !holds[number] {
			return stack.Wrap(fmt.Errorf("%w: %s is not a number this app holds", ErrInvalid, number))
		}
		if slices.Contains(numbers[:i], number) {
			return stack.Wrap(fmt.Errorf("%w: %s is named twice", ErrInvalid, number))
		}
	}
	return nil
}

// Delete removes a use case the vendor does not hold.
func (s *Service) Delete(ctx context.Context, customerID, id string) error {
	useCase, err := s.store.UseCases.Get(ctx, customerID, id)
	if err != nil {
		return err
	}
	if !Deletable(useCase.Status) {
		return stack.Wrap(fmt.Errorf("%w: a %s use case cannot be deleted", ErrLocked, useCase.Status))
	}
	return s.store.UseCases.Delete(ctx, customerID, id)
}

// Submit sends a use case to Stream's review, once it and the app's profile are complete.
func (s *Service) Submit(ctx context.Context, customerID, id string) (store.UseCase, error) {
	useCase, err := s.store.UseCases.Get(ctx, customerID, id)
	if err != nil {
		return store.UseCase{}, err
	}
	if !Editable(useCase.Status) {
		return store.UseCase{}, stack.Wrap(fmt.Errorf("%w: a %s use case cannot be submitted", ErrLocked, useCase.Status))
	}
	profile, err := s.store.BusinessProfiles.Get(ctx, customerID)
	if errors.Is(err, store.ErrNoBusinessProfile) {
		return store.UseCase{}, stack.Wrap(fmt.Errorf("%w: save a business profile first", ErrInvalid))
	}
	if err != nil {
		return store.UseCase{}, err
	}
	if err := errors.Join(ValidateProfile(profile), ValidateUseCase(useCase)); err != nil {
		return store.UseCase{}, stack.Wrap(err)
	}
	// Registration comes after the review, maybe days later, so nothing is booked here: the
	// credit is only checked to cover every fee it will book then.
	fees := s.campaignFees(ctx, useCase)
	if profile.VendorBrandID == "" {
		fees += s.brandFee(ctx, useCase)
	}
	checked, err := s.book(ctx, useCase.CustomerID, fees)
	if err != nil {
		return store.UseCase{}, err
	}
	checked.Release()

	from := useCase.Status
	now := time.Now().UTC()
	useCase.Status, useCase.SubmittedAt = Submitted, &now
	err = s.store.UseCases.Move(ctx, &useCase, from, &store.ReviewLog{Actor: ActorApp, SubmittedAt: &now})
	return useCase, err
}

// Review records Stream's decision on a submitted use case. Approving sends it to the
// vendor, or approves it outright with no vendor to send it to.
func (s *Service) Review(ctx context.Context, id, decision, notes, reviewer string) (store.UseCase, error) {
	useCase, err := s.store.UseCases.GetByID(ctx, id)
	if err != nil {
		return store.UseCase{}, err
	}
	if useCase.Status != Submitted {
		return store.UseCase{}, stack.Wrap(fmt.Errorf("%w: only a submitted use case is reviewed, this one is %s", ErrLocked, useCase.Status))
	}
	log := &store.ReviewLog{Actor: ActorStaff, ActorName: reviewer, Notes: notes}
	switch decision {
	case Reject:
		useCase.Status = Rejected
	case RequestChanges:
		useCase.Status = ChangesRequested
	case Approve:
		if s.registrar == nil {
			approve(&useCase, log)
			break
		}
		useCase.Status = VendorPending
		useCase.Vendor = s.registrar.Name()
	default:
		return store.UseCase{}, stack.Wrap(fmt.Errorf("%w: decision %q", ErrInvalid, decision))
	}
	if err := s.store.UseCases.Move(ctx, &useCase, Submitted, log); err != nil {
		return store.UseCase{}, err
	}
	if useCase.Status != VendorPending {
		return useCase, nil
	}
	if err := s.refresh(ctx, useCase, nil); err != nil {
		// The poller tries again: the review itself is recorded, and the vendor being down
		// is no reason to make the reviewer decide twice.
		s.logger.Error("could not register a use case with its vendor", "use_case", id, "error", err)
	}
	return s.store.UseCases.GetByID(ctx, id)
}

// Hook takes a vendor's report on a brand or a campaign, verified, and moves whichever use
// cases it is about. A report on a campaign is counted as its use case's app's phone work. One on
// a brand is not, and has the brand's app's waiting use cases registered. A report on an
// approved use case's campaign the vendor failed (suspended) records the vendor's status and
// leaves the use case approved.
func (s *Service) Hook(ctx context.Context, header http.Header, body []byte) error {
	if s.registrar == nil {
		return fmt.Errorf("%w: no registrar is configured", ErrRefused)
	}
	if err := s.registrar.VerifyHook(header, body, time.Now()); err != nil {
		return err
	}
	campaignID, brandID := s.registrar.HookSubject(body)
	if campaignID != "" {
		useCase, err := s.store.UseCases.GetByCampaign(ctx, s.registrar.Name(), campaignID)
		if errors.Is(err, store.ErrUnknownUseCase) {
			return nil
		}
		if err != nil {
			return err
		}
		activity.NoteCustomer(ctx, useCase.CustomerID, activity.AreaPhoneSMS)
		if useCase.Status == Approved {
			return s.suspended(ctx, useCase, body)
		}
		return s.refresh(ctx, useCase, body)
	}
	if brandID != "" {
		return s.brandReported(ctx, brandID)
	}
	return nil
}

// brandReported refreshes the use cases the vendor holds of the app it registered as brandID,
// which registers those that waited on the brand. The vendor waits on this, so it asks about
// that one app's and no other's.
func (s *Service) brandReported(ctx context.Context, brandID string) error {
	profile, err := s.store.BusinessProfiles.GetByBrand(ctx, brandID)
	if errors.Is(err, store.ErrNoBusinessProfile) {
		return nil
	}
	if err != nil {
		return err
	}
	// An app holds a handful of use cases; past the first page, Run's passes reach the rest.
	held, err := s.store.UseCases.Select(ctx, profile.CustomerID, store.DLCLimit(pollPageSize), nil)
	if err != nil {
		return err
	}
	for _, useCase := range held {
		if useCase.Status != VendorPending {
			continue
		}
		if err := s.refresh(ctx, useCase, nil); err != nil {
			s.logger.Error("could not refresh a use case", "use_case", useCase.ID, "error", err)
		}
	}
	return nil
}

// refresh asks the vendor where a use case it holds stands, registering the brand and the
// campaign it is still missing, and moves it to where the vendor says.
func (s *Service) refresh(ctx context.Context, useCase store.UseCase, report []byte) error {
	if s.registrar == nil || useCase.Status != VendorPending {
		return nil
	}
	if useCase.VendorCampaignID == "" {
		return s.register(ctx, useCase)
	}
	campaign, err := s.registrar.Campaign(ctx, useCase.VendorCampaignID)
	if err != nil {
		return err
	}
	if report == nil {
		report = campaign.Raw
	}
	log := &store.ReviewLog{Actor: ActorVendor, ActorName: s.registrar.Name(), VendorPayload: report}
	switch campaign.Outcome {
	case Accepted:
		if err := s.assignAtVendor(ctx, useCase); err != nil {
			return err
		}
		useCase.VendorStatus = campaign.Status
		approve(&useCase, log)
	case Failed:
		useCase.VendorStatus, useCase.Status = campaign.Status, VendorRejected
		log.Notes = "The vendor answered " + campaign.Status + "."
	default:
		if campaign.Status == useCase.VendorStatus {
			return nil
		}
		useCase.VendorStatus = campaign.Status
	}
	if err := s.store.UseCases.Move(ctx, &useCase, VendorPending, log); err != nil {
		return err
	}
	// Counted once the move is stored: a report and the poller reading the same rejection
	// both get here, and only the one that moved the use case off VendorPending counts it.
	// A failed campaign leaves its numbers unable to send A2P texts, so somebody looks at it in working hours.
	if campaign.Outcome == Failed {
		vendorevent.Count(s.registrar.Name(), vendorevent.DLCCampaignFailed, vendorevent.LevelWarn)
		s.logger.Warn("a vendor failed a 10DLC campaign", "vendor", s.registrar.Name(),
			"event", vendorevent.DLCCampaignFailed, "event_level", string(vendorevent.LevelWarn), "use_case", useCase.ID, "vendor_status", campaign.Status)
	}
	return nil
}

// suspended records on an approved use case the vendor status the vendor now reports for its
// campaign, such as suspended after carriers provisioned it, or provisioned again after that.
// The use case stays approved, so the app's calls, messages and campaign billing do not change:
// a person reads the page and acts. Only a status that failed is counted.
func (s *Service) suspended(ctx context.Context, useCase store.UseCase, report []byte) error {
	campaign, err := s.registrar.Campaign(ctx, useCase.VendorCampaignID)
	if err != nil {
		return err
	}
	if campaign.Status == useCase.VendorStatus {
		return nil
	}
	useCase.VendorStatus = campaign.Status
	log := &store.ReviewLog{Actor: ActorVendor, ActorName: s.registrar.Name(), VendorPayload: report,
		Notes: "The vendor answered " + campaign.Status + " after the campaign was approved."}
	if err := s.store.UseCases.Move(ctx, &useCase, Approved, log); err != nil {
		return err
	}
	// Counted once the status is stored: only the report that changed the stored vendor status
	// counts it, and a repeated one finds it stored already. A status that is no failure, such
	// as a reinstatement, is stored so that the next suspension differs from it and counts.
	// Texts that were sending have stopped, so somebody is woken for a failure.
	if campaign.Outcome != Failed {
		return nil
	}
	vendorevent.Count(s.registrar.Name(), vendorevent.DLCCampaignSuspended, vendorevent.LevelPage)
	s.logger.Warn("a vendor suspended an approved 10DLC campaign", "vendor", s.registrar.Name(),
		"event", vendorevent.DLCCampaignSuspended, "event_level", string(vendorevent.LevelPage), "use_case", useCase.ID,
		"vendor_status", campaign.Status)
	return nil
}

// requestSettles is how long a registration asked for may still be running, on this router
// or another: far longer than the registrar's timeout, so one asked for longer ago had its
// answer lost.
const requestSettles = 10 * time.Minute

// saveAttempts and saveFirstPause bound how often what the vendor answered to a registration
// is saved: the first save and two more, 100 ms and then 200 ms after the one before, for a
// dropped connection or a failover. The vendor has registered it, and billed it, by then, so
// the save is what is tried again, never the registration.
const (
	saveAttempts   = 3
	saveFirstPause = 100 * time.Millisecond
)

// register registers the app's brand if it has none, and the campaign once the brand is
// verified. A brand the vendor refused leaves nothing to register the campaign under. Each is
// asked for once (requested): only a request the vendor did not carry out is asked for again,
// and one whose answer was lost is settled (brandUnanswered, campaignUnanswered).
func (s *Service) register(ctx context.Context, useCase store.UseCase) error {
	profile, err := s.store.BusinessProfiles.Get(ctx, useCase.CustomerID)
	if err != nil {
		return err
	}
	var brand Brand
	if profile.VendorBrandID == "" {
		booked, err := s.book(ctx, useCase.CustomerID, s.brandFee(ctx, useCase))
		if err != nil {
			return err
		}
		requestedAt := s.now().UTC().Truncate(time.Microsecond)
		requested, err := s.store.BusinessProfiles.RequestBrand(ctx, useCase.CustomerID, requestedAt)
		if err != nil || !requested {
			booked.Release()
			if err != nil {
				return err
			}
			return s.brandUnanswered(ctx, useCase, profile)
		}
		brand, err = routing.Call(ctx, s.feeMeter(ctx, ModelBrand, useCase.CustomerID, nil),
			func(ctx context.Context) (Brand, error) { return s.registrar.RegisterBrand(ctx, profile) })
		if err != nil {
			booked.Release()
			if errors.Is(err, ErrNotCarriedOut) {
				err = errors.Join(err, s.store.BusinessProfiles.ForgetBrandRequest(ctx, useCase.CustomerID, requestedAt))
			}
			return err
		}
		booked.Charged()
	} else {
		brand, err = s.registrar.Brand(ctx, profile.VendorBrandID)
	}
	if err != nil {
		return err
	}
	if brand.ID != profile.VendorBrandID || brand.Status != profile.BrandStatus {
		err := saved(ctx, func(ctx context.Context) error {
			return s.store.BusinessProfiles.SetBrand(ctx, useCase.CustomerID, brand.ID, brand.Status)
		})
		if err != nil && profile.VendorBrandID == "" {
			// Its request stays, so it is never registered again; support records the id.
			s.logger.Error("could not save a brand the vendor registered; record its id on the app's business profile",
				"app_id", useCase.CustomerID, "vendor", s.registrar.Name(), "vendor_brand_id", brand.ID, "error", err)
			return nil
		}
		if err != nil {
			return err
		}
	}

	switch brand.Outcome {
	case Pending:
		return nil
	case Failed:
		useCase.Status, useCase.VendorStatus = VendorRejected, brand.Status
		log := &store.ReviewLog{Actor: ActorVendor, ActorName: s.registrar.Name(),
			Notes: "The vendor refused the brand: " + brand.Status + ".", VendorPayload: brand.Raw}
		if err := s.store.UseCases.Move(ctx, &useCase, VendorPending, log); err != nil {
			return err
		}
		// Counted once the move is stored, as a failed campaign is (refresh): only use cases
		// still on VendorPending are registered, so a report after the move counts nothing.
		vendorevent.Count(s.registrar.Name(), vendorevent.DLCBrandFailed, vendorevent.LevelWarn)
		s.logger.Warn("a vendor refused a 10DLC brand", "vendor", s.registrar.Name(),
			"event", vendorevent.DLCBrandFailed, "event_level", string(vendorevent.LevelWarn), "use_case", useCase.ID,
			"vendor_status", brand.Status)
		return nil
	}
	booked, err := s.book(ctx, useCase.CustomerID, s.campaignFees(ctx, useCase))
	if err != nil {
		return err
	}
	requestedAt := s.now().UTC().Truncate(time.Microsecond)
	requested, err := s.store.UseCases.RequestCampaign(ctx, useCase.ID, requestedAt)
	if err != nil || !requested {
		booked.Release()
		if err != nil {
			return err
		}
		return s.campaignUnanswered(ctx, useCase, brand.ID)
	}
	campaign, err := routing.Call(ctx, s.feeMeter(ctx, ModelCampaign, useCase.CustomerID, campaignAttributes(useCase)),
		func(ctx context.Context) (Campaign, error) {
			return s.registrar.RegisterCampaign(ctx, brand.ID, useCase, s.hookURL)
		})
	if err != nil {
		booked.Release()
		if errors.Is(err, ErrNotCarriedOut) {
			err = errors.Join(err, s.store.UseCases.ForgetCampaignRequest(ctx, useCase.ID, requestedAt))
		}
		return err
	}
	defer booked.Charged()
	return s.registered(ctx, useCase, campaign, requestedAt)
}

// registered records the campaign the vendor registered for a use case, asked for at
// requestedAt, and bills the months the registrar charged for it then. A save that does not
// land is logged with the campaign's id, and the use case's request stays: campaignUnanswered
// finds the campaign at the vendor later.
func (s *Service) registered(ctx context.Context, useCase store.UseCase, campaign Campaign, requestedAt time.Time) error {
	useCase.VendorCampaignID, useCase.VendorStatus = campaign.ID, campaign.Status
	if useCase.Vendor == "" {
		useCase.Vendor = s.registrar.Name()
	}
	// A registrar charges a campaign's first months when it registers it.
	s.billUpfront(useCase, requestedAt)
	err := saved(ctx, func(ctx context.Context) error {
		log := &store.ReviewLog{Actor: ActorVendor, ActorName: s.registrar.Name(),
			Notes: registeredNote + campaign.ID + ".", VendorPayload: campaign.Raw}
		return s.store.UseCases.Move(ctx, &useCase, VendorPending, log)
	})
	if err != nil && !errors.Is(err, store.ErrUseCaseMoved) {
		s.logger.Error("could not save a campaign the vendor registered; the poller finds it at the vendor",
			"use_case", useCase.ID, "vendor", s.registrar.Name(), "vendor_campaign_id", campaign.ID, "error", err)
		return nil
	}
	return err
}

// brandUnanswered settles the app's brand asked for before, with no answer recorded. One asked
// for less than requestSettles ago may still be registering and is left to it. The vendor
// keeps no reference of the caller's on a brand, so one whose answer was lost cannot be found
// there for sure: it is unanswered.
func (s *Service) brandUnanswered(ctx context.Context, useCase store.UseCase, profile store.BusinessProfile) error {
	requestedAt := profile.BrandRequestedAt
	if requestedAt == nil || s.now().Sub(*requestedAt) < requestSettles {
		return nil
	}
	return s.unanswered(ctx, useCase, "brand", *requestedAt)
}

// campaignUnanswered settles a use case's campaign asked for before, with no answer recorded.
// One asked for less than requestSettles ago may still be registering and is left to it.
// Otherwise the vendor is asked for it by the use case's id (CampaignFinder): one it holds is
// recorded and billed as its answer would have been, under its own id, so nothing billed
// already is billed again; one it does not hold was never registered, so the request is
// forgotten and the next poll asks for it again. A registrar that cannot be asked leaves it
// unanswered.
func (s *Service) campaignUnanswered(ctx context.Context, useCase store.UseCase, brandID string) error {
	requestedAt := useCase.CampaignRequestedAt
	if requestedAt == nil || s.now().Sub(*requestedAt) < requestSettles {
		return nil
	}
	finder, ok := s.registrar.(CampaignFinder)
	if !ok {
		return s.unanswered(ctx, useCase, "campaign", *requestedAt)
	}
	// Another router may be settling it too: only the one holding the lock goes on, and it reads
	// the use case again, as another may have settled it between this one's read and its lock.
	return s.store.UseCases.WithSettleLock(ctx, useCase, func() error {
		current, err := s.store.UseCases.GetByID(ctx, useCase.ID)
		if err != nil {
			return err
		}
		if current.VendorCampaignID != "" || current.CampaignRequestedAt == nil || !current.CampaignRequestedAt.Equal(*requestedAt) {
			return nil
		}
		campaign, found, err := finder.FindCampaign(ctx, brandID, useCase.ID)
		if err != nil {
			return err
		}
		if !found {
			return s.store.UseCases.ForgetCampaignRequest(ctx, useCase.ID, *requestedAt)
		}
		if err := routing.Settle(s.feeMeter(ctx, ModelCampaign, useCase.CustomerID, campaignAttributes(useCase)), campaign); err != nil {
			s.logger.Error("could not bill a 10DLC fee", "model", ModelCampaign, "error", err)
		}
		s.logger.Warn("recorded a campaign the vendor registered whose answer was lost", "use_case", useCase.ID,
			"vendor", s.registrar.Name(), "vendor_campaign_id", campaign.ID)
		return s.registered(ctx, useCase, campaign, *requestedAt)
	})
}

// unanswered hands a use case whose brand or campaign registration was not answered to the app
// and to support: it moves to vendor_rejected, which the app may edit and delete, with a note
// saying why. The request stays, so nothing is registered, and billed, again until support has
// looked it up at the vendor, then recorded its id or forgotten the request.
func (s *Service) unanswered(ctx context.Context, useCase store.UseCase, registration string, requestedAt time.Time) error {
	useCase.Status = VendorRejected
	log := &store.ReviewLog{Actor: ActorVendor, ActorName: s.registrar.Name(),
		Notes: "The vendor did not answer when it was asked to register the " + registration +
			", so it may hold it. Contact support: it is not asked for again until they have looked it up."}
	err := s.store.UseCases.Move(ctx, &useCase, VendorPending, log)
	if errors.Is(err, store.ErrUseCaseMoved) {
		// Another router's poll moved it first.
		return nil
	}
	if err != nil {
		return err
	}
	s.logger.Error("a 10DLC registration was not answered; look it up at the vendor, then record its id or forget its request",
		"use_case", useCase.ID, "app_id", useCase.CustomerID, "vendor", s.registrar.Name(), "registration", registration,
		"requested_at", requestedAt)
	return nil
}

// saved runs save on a context a caller that went away does not end, until it lands or
// saveAttempts ran, while its failure is one a retry may fix: not a row the store or Postgres
// refused, nor a use case that moved meanwhile.
func saved(ctx context.Context, save func(context.Context) error) error {
	ctx = context.WithoutCancel(ctx)
	err := save(ctx)
	pause := saveFirstPause
	for attempt := 1; attempt < saveAttempts && err != nil && !store.Unwritable(err) && !errors.Is(err, store.ErrUseCaseMoved); attempt++ {
		time.Sleep(pause)
		pause *= 2
		err = save(ctx)
	}
	return err
}

// campaignAttributes are what a use case's campaign fee is priced by.
func campaignAttributes(useCase store.UseCase) map[string]string {
	return map[string]string{AttributeUseCaseType: useCaseType(useCase)}
}

// assignAtVendor tells the vendor which of its numbers send as the campaign. A number from
// another vendor is not the registrar's to assign.
func (s *Service) assignAtVendor(ctx context.Context, useCase store.UseCase) error {
	numbers, err := s.store.UseCases.Numbers(ctx, useCase)
	if err != nil {
		return err
	}
	for _, number := range numbers {
		if number.Vendor != s.registrar.Name() {
			continue
		}
		if err := s.registrar.AssignNumber(ctx, useCase.VendorCampaignID, number.E164); err != nil {
			return err
		}
	}
	return nil
}

func approve(useCase *store.UseCase, log *store.ReviewLog) {
	now := time.Now().UTC()
	useCase.Status, useCase.ApprovedAt = Approved, &now
	log.ApprovedAt = &now
}
