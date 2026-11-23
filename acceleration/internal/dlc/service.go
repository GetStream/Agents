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
			// A stopping router ends the pass here, rather than logging every use case left.
			if ctx.Err() != nil {
				if i == 0 {
					return after
				}
				end := useCases[i-1]
				return &store.CreatedPosition{At: end.UpdatedAt, ID: end.ID}
			}
			if err := s.refresh(ctx, useCase, nil); err != nil {
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

// Create records a new draft use case and the numbers that send as it.
func (s *Service) Create(ctx context.Context, useCase *store.UseCase, numbers []string) error {
	useCase.Status = Draft
	if err := s.store.UseCases.Insert(ctx, useCase); err != nil {
		return err
	}
	return s.assign(ctx, *useCase, numbers)
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

// assign sends numbers as the use case. A number the app does not hold, or one named twice,
// is the caller's to correct; a store failure is not.
func (s *Service) assign(ctx context.Context, useCase store.UseCase, numbers []string) error {
	if len(numbers) > 0 {
		held, err := s.store.PhoneNumbers.Select(ctx, useCase.CustomerID, store.PhoneNumberFilter{})
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
	}
	return s.store.UseCases.AssignNumbers(ctx, useCase.CustomerID, useCase.ID, numbers)
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
// a brand is not, and has the brand's app's waiting use cases registered.
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

// register registers the app's brand if it has none, and the campaign once the brand is
// verified. A brand the vendor refused leaves nothing to register the campaign under.
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
		brand, err = routing.Call(ctx, s.feeMeter(ctx, ModelBrand, useCase.CustomerID, nil),
			func(ctx context.Context) (Brand, error) { return s.registrar.RegisterBrand(ctx, profile) })
		if err != nil {
			booked.Release()
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
		if err := s.store.BusinessProfiles.SetBrand(ctx, useCase.CustomerID, brand.ID, brand.Status); err != nil {
			return err
		}
	}

	log := &store.ReviewLog{Actor: ActorVendor, ActorName: s.registrar.Name()}
	switch brand.Outcome {
	case Pending:
		return nil
	case Failed:
		useCase.Status, useCase.VendorStatus = VendorRejected, brand.Status
		log.Notes, log.VendorPayload = "The vendor refused the brand: "+brand.Status+".", brand.Raw
		return s.store.UseCases.Move(ctx, &useCase, VendorPending, log)
	}
	booked, err := s.book(ctx, useCase.CustomerID, s.campaignFees(ctx, useCase))
	if err != nil {
		return err
	}
	campaign, err := routing.Call(ctx, s.feeMeter(ctx, ModelCampaign, useCase.CustomerID,
		map[string]string{AttributeUseCaseType: useCaseType(useCase)}),
		func(ctx context.Context) (Campaign, error) {
			return s.registrar.RegisterCampaign(ctx, brand.ID, useCase, s.hookURL)
		})
	if err != nil {
		booked.Release()
		return err
	}
	defer booked.Charged()
	useCase.VendorCampaignID, useCase.VendorStatus = campaign.ID, campaign.Status
	if useCase.Vendor == "" {
		useCase.Vendor = s.registrar.Name()
	}
	// A registrar charges a campaign's first months when it registers it.
	s.billUpfront(useCase, s.now().UTC())
	log.Notes, log.VendorPayload = registeredNote+campaign.ID+".", campaign.Raw
	return s.store.UseCases.Move(ctx, &useCase, VendorPending, log)
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
