package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// UseCase is one 10DLC campaign an app sends text and calls as.
type UseCase struct {
	bun.BaseModel `bun:"table:dlc_use_cases,alias:uc"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// IsDefault is the use case every number assigned to no other one sends as.
	IsDefault bool   `bun:"is_default,notnull"`
	Status    string `bun:"status,notnull"`
	// UseCaseType is the campaign registry's use case, such as CUSTOMER_CARE.
	UseCaseType string `bun:"use_case_type,notnull"`
	Description string `bun:"description,notnull"`
	// MessageFlow is how a recipient opts in, as the registry asks for it.
	MessageFlow    string   `bun:"message_flow,notnull"`
	MessageSamples []string `bun:"message_samples,array"`
	HelpMessage    string   `bun:"help_message,notnull"`
	OptOutMessage  string   `bun:"opt_out_message,notnull"`
	OptInMessage   string   `bun:"opt_in_message,notnull"`
	EmbeddedLinks  bool     `bun:"embedded_links,notnull"`
	EmbeddedPhone  bool     `bun:"embedded_phone,notnull"`
	AgeGated       bool     `bun:"age_gated,notnull"`
	DirectLending  bool     `bun:"direct_lending,notnull"`
	// Channels holds what RCS, WhatsApp, iMessage and voice ask for. It is reviewed with
	// the rest and not sent to any vendor yet.
	Channels UseCaseChannels `bun:"channels,type:jsonb,notnull"`
	// Vendor is who the campaign was registered with, VendorCampaignID its id there and
	// VendorStatus the vendor's own word for where it stands.
	Vendor           string `bun:"vendor,notnull"`
	VendorCampaignID string `bun:"vendor_campaign_id,notnull"`
	VendorStatus     string `bun:"vendor_status,notnull"`
	// CampaignRequestedAt is when the vendor was asked to register the campaign
	// (RequestCampaign). Set with no VendorCampaignID, the vendor's answer was lost.
	CampaignRequestedAt *time.Time `bun:"campaign_requested_at"`
	CreatedAt           time.Time  `bun:"created_at,notnull"`
	UpdatedAt           time.Time  `bun:"updated_at,notnull"`
	SubmittedAt         *time.Time `bun:"submitted_at"`
	ApprovedAt          *time.Time `bun:"approved_at"`
}

// UseCaseChannels are the fields the channels besides 10DLC ask for. Each is absent for a
// channel the use case is not for.
type UseCaseChannels struct {
	RCS      *RCSProfile      `json:"rcs,omitempty"`
	WhatsApp *WhatsAppProfile `json:"whatsapp,omitempty"`
	IMessage *IMessageProfile `json:"imessage,omitempty"`
	Voice    *VoiceProfile    `json:"voice,omitempty"`
}

// RCSProfile is the agent and launch review an RCS agent needs.
type RCSProfile struct {
	DisplayName              string   `json:"display_name,omitempty" maxLength:"40"`
	Description              string   `json:"description,omitempty" maxLength:"100"`
	UseCase                  string   `json:"use_case,omitempty" doc:"otp, transactional, promotional or multi_use."`
	LogoURL                  string   `json:"logo_url,omitempty" doc:"224 by 224 PNG or JPEG, at most 50 KB, on public HTTPS."`
	HeroURL                  string   `json:"hero_url,omitempty" doc:"1440 by 448 PNG or JPEG, at most 200 KB, on public HTTPS."`
	BrandColor               string   `json:"brand_color,omitempty" doc:"Hex, such as #1A73E8, with at least 4.5:1 contrast against white."`
	SupportPhone             string   `json:"support_phone,omitempty"`
	SupportEmail             string   `json:"support_email,omitempty"`
	SupportLabel             string   `json:"support_label,omitempty"`
	CompanyOverview          string   `json:"company_overview,omitempty"`
	AgentOverview            string   `json:"agent_overview,omitempty"`
	InteractionTypes         string   `json:"interaction_types,omitempty"`
	MessageExamples          []string `json:"message_examples,omitempty"`
	OptInMethods             string   `json:"opt_in_methods,omitempty"`
	CallToActionText         string   `json:"call_to_action_text,omitempty"`
	CallToActionURL          string   `json:"call_to_action_url,omitempty"`
	DoubleOptIn              bool     `json:"double_opt_in,omitempty"`
	OptInConfirmationMessage string   `json:"opt_in_confirmation_message,omitempty"`
	TestVideoURL             string   `json:"test_video_url,omitempty" doc:"Shows consent, example interactions, HELP and STOP."`
}

// WhatsAppProfile is what Meta's Embedded Signup asks for.
type WhatsAppProfile struct {
	BusinessPortfolio  string `json:"business_portfolio,omitempty" doc:"The Meta business portfolio, existing or to create."`
	BusinessAccount    string `json:"business_account,omitempty" doc:"The WhatsApp Business account, existing or to create."`
	PhoneNumber        string `json:"phone_number,omitempty"`
	DisplayName        string `json:"display_name,omitempty"`
	VerificationMethod string `json:"verification_method,omitempty" doc:"sms or voice."`
}

// IMessageProfile is the contact card and consent an iMessage line through Linq carries.
type IMessageProfile struct {
	ContactCardName  string   `json:"contact_card_name,omitempty"`
	ContactCardImage string   `json:"contact_card_image_url,omitempty" doc:"Square, at least 200 by 200."`
	ConsentMethod    string   `json:"consent_method,omitempty"`
	FallbackChannels []string `json:"fallback_channels,omitempty" doc:"imessage, rcs or sms, in the order to try them."`
}

// VoiceProfile is what outbound AI calling is for and how its consent was collected.
type VoiceProfile struct {
	CallerIDNumber          string   `json:"caller_id_number,omitempty" doc:"A number bought here, or a verified external one."`
	CallingPurpose          string   `json:"calling_purpose,omitempty" doc:"Support, reminders, sales and so on."`
	DestinationCountries    []string `json:"destination_countries,omitempty"`
	ExpectedCallVolume      string   `json:"expected_call_volume,omitempty"`
	ConsentCollectionMethod string   `json:"consent_collection_method,omitempty"`
	ConsentDisclosureText   string   `json:"consent_disclosure_text,omitempty"`
	ConsentEvidenceLocation string   `json:"consent_evidence_location,omitempty"`
	OptOutHandling          string   `json:"opt_out_handling,omitempty"`
	CallRecording           bool     `json:"call_recording_enabled,omitempty"`
	RecordingDisclosure     string   `json:"recording_disclosure,omitempty" doc:"How a recorded call is disclosed and consented to."`
}

// Validate refuses a use case no app could send as.
func (u *UseCase) Validate() error {
	return validate("use case").
		required("customer_id", u.CustomerID).
		required("name", u.Name).
		required("status", u.Status).
		done()
}

// UseCases are the 10DLC campaigns apps send as, in the shared database.
type UseCases struct {
	store *Store
}

// ErrUnknownUseCase is a use case id the app does not hold.
var ErrUnknownUseCase = errors.New("store: there is no such use case")

// ErrUseCaseMoved is a use case somebody else moved between reading and writing it.
var ErrUseCaseMoved = errors.New("store: the use case changed status meanwhile")

// How many use cases, reviews or opt-outs a page holds.
const (
	defaultDLCLimit = 50
	maxDLCLimit     = 200
)

// DLCLimit is the page size the use case, review and opt-out lists use for the limit asked
// for. They return one row more than this, so a caller can tell the page is not the last.
func DLCLimit(asked int) int { return clampLimit(asked, defaultDLCLimit, maxDLCLimit) }

// Select pages through an app's use cases, newest first.
func (r *UseCases) Select(ctx context.Context, customerID string, limit int, after *CreatedPosition) ([]UseCase, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: a customer is required"))
	}
	useCases := []UseCase{}
	query := r.store.global.NewSelect().Model(&useCases).Where("customer_id = ?", customerID)
	if after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.At, after.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(DLCLimit(limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: use cases: %w", err))
	}
	return useCases, nil
}

// Get returns one use case an app holds.
func (r *UseCases) Get(ctx context.Context, customerID, id string) (UseCase, error) {
	if customerID == "" || id == "" {
		return UseCase{}, stack.Wrap(ErrUnknownUseCase)
	}
	return r.useCase(ctx, r.store.global.NewSelect().Where("customer_id = ?", customerID).Where("id = ?", id))
}

// GetForNumber returns the use case a number sends as: the one it is assigned to, or
// the app's default. A number the app does not hold sends as the default too.
func (r *UseCases) GetForNumber(ctx context.Context, customerID, e164 string) (UseCase, error) {
	if customerID == "" {
		return UseCase{}, ErrUnknownUseCase
	}
	assigned := r.store.global.NewSelect().Model((*PhoneNumber)(nil)).
		Column("dlc_use_case_id").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL")
	return r.useCase(ctx, r.store.global.NewSelect().
		Where("customer_id = ?", customerID).
		WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
			return q.Where("id IN (?)", assigned).WhereOr("is_default")
		}).
		OrderExpr("is_default"))
}

// GetByID returns a use case whoever holds it, for Stream's own review.
func (r *UseCases) GetByID(ctx context.Context, id string) (UseCase, error) {
	if id == "" {
		return UseCase{}, stack.Wrap(ErrUnknownUseCase)
	}
	return r.useCase(ctx, r.store.global.NewSelect().Where("id = ?", id))
}

// GetByCampaign returns the use case a vendor knows by a campaign id.
func (r *UseCases) GetByCampaign(ctx context.Context, vendor, campaignID string) (UseCase, error) {
	if vendor == "" || campaignID == "" {
		return UseCase{}, ErrUnknownUseCase
	}
	return r.useCase(ctx, r.store.global.NewSelect().Where("vendor = ?", vendor).Where("vendor_campaign_id = ?", campaignID))
}

func (r *UseCases) useCase(ctx context.Context, query *bun.SelectQuery) (UseCase, error) {
	var useCase UseCase
	err := query.Model(&useCase).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return UseCase{}, stack.Wrap(ErrUnknownUseCase)
	}
	if err != nil {
		return UseCase{}, stack.Wrap(fmt.Errorf("store: use case: %w", err))
	}
	return useCase, nil
}

// SelectInStatus pages through every app's use cases in one status, longest waiting
// first, which is the order Stream reviews them in.
func (r *UseCases) SelectInStatus(ctx context.Context, status string, limit int, after *CreatedPosition) ([]UseCase, error) {
	useCases := []UseCase{}
	query := r.store.global.NewSelect().Model(&useCases).Where("status = ?", status)
	if after != nil {
		query = query.Where("(updated_at, id) > (?, ?)", after.At, after.ID)
	}
	err := query.Order("updated_at", "id").Limit(DLCLimit(limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: use cases in status: %w", err))
	}
	return useCases, nil
}

// RegisteredCampaign is a use case registered as a campaign at a vendor, and when it was:
// the first vendor review entry whose notes are registered followed by the campaign's id and a
// full stop, or when the use case was created where there is none.
type RegisteredCampaign struct {
	UseCase      `bun:",extend"`
	RegisteredAt time.Time `bun:"registered_at"`
}

// SelectWithCampaigns is every use case registered as a campaign at a vendor and in one of
// statuses, with when it was registered: what a vendor bills a campaign's months for.
func (r *UseCases) SelectWithCampaigns(ctx context.Context, registered string, statuses ...string) ([]RegisteredCampaign, error) {
	campaigns := []RegisteredCampaign{}
	err := r.store.global.NewSelect().Model(&campaigns).
		ColumnExpr("uc.*").
		ColumnExpr(`COALESCE((SELECT min(rl.created_at) FROM dlc_review_logs AS rl
			WHERE rl.use_case_id = uc.id AND rl.actor = 'vendor' AND rl.notes = ? || uc.vendor_campaign_id || '.'),
			uc.created_at) AS registered_at`, registered).
		Where("uc.vendor_campaign_id <> ''").
		Where("uc.status IN (?)", bun.In(statuses)).
		Order("uc.id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: use cases with campaigns: %w", err))
	}
	return campaigns, nil
}

// HasIn reports whether an app holds a use case in any of statuses.
func (r *UseCases) HasIn(ctx context.Context, customerID string, statuses ...string) (bool, error) {
	found, err := r.store.global.NewSelect().Model((*UseCase)(nil)).
		Where("customer_id = ?", customerID).
		Where("status IN (?)", bun.In(statuses)).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: has use case: %w", err))
	}
	return found, nil
}

// errDefaultTaken rolls back a create whose use case would be the app's default when another
// create made one meanwhile.
var errDefaultTaken = errors.New("store: another use case became the app's default meanwhile")

// Insert records a new use case and makes numbers the ones that send as it, as one. An app's
// first one is its default, and a new default takes over from the old one.
func (r *UseCases) Insert(ctx context.Context, useCase *UseCase, numbers ...string) error {
	if err := useCase.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	useCase.ID = newID()
	useCase.CreatedAt, useCase.UpdatedAt = now, now
	if useCase.MessageSamples == nil {
		useCase.MessageSamples = []string{}
	}
	asked := useCase.IsDefault
	var err error
	// Twice at most: two creates at once for an app with no use case both find none, and the
	// one whose default lands second is made again as what it is then.
	for range 2 {
		useCase.IsDefault = asked
		err = r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			held, err := tx.NewSelect().Model((*UseCase)(nil)).Where("customer_id = ?", useCase.CustomerID).Exists(ctx)
			if err != nil {
				return fmt.Errorf("store: create use case: %w", err)
			}
			useCase.IsDefault = useCase.IsDefault || !held
			if useCase.IsDefault {
				if err := clearDefaultUseCase(ctx, tx, useCase.CustomerID, ""); err != nil {
					return err
				}
			}
			result, err := tx.NewInsert().Model(useCase).On("CONFLICT (customer_id) WHERE is_default DO NOTHING").Exec(ctx)
			if err != nil {
				return fmt.Errorf("store: create use case: %w", err)
			}
			if inserted, err := result.RowsAffected(); err != nil {
				return fmt.Errorf("store: create use case: %w", err)
			} else if inserted == 0 {
				return errDefaultTaken
			}
			return assignNumbers(ctx, tx, useCase.CustomerID, useCase.ID, numbers)
		})
		if !errors.Is(err, errDefaultTaken) {
			return stack.Wrap(err)
		}
	}
	return stack.Wrap(fmt.Errorf("store: create use case: %w", err))
}

// Update replaces what an app wrote on a use case. Its status, vendor fields and
// review timestamps are left alone: those move through Move.
func (r *UseCases) Update(ctx context.Context, useCase *UseCase) error {
	if err := useCase.Validate(); err != nil {
		return stack.Wrap(err)
	}
	useCase.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	if useCase.MessageSamples == nil {
		useCase.MessageSamples = []string{}
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if useCase.IsDefault {
			if err := clearDefaultUseCase(ctx, tx, useCase.CustomerID, useCase.ID); err != nil {
				return err
			}
		}
		result, err := tx.NewUpdate().Model(useCase).
			Column("name", "is_default", "use_case_type", "description", "message_flow", "message_samples",
				"help_message", "opt_out_message", "opt_in_message", "embedded_links", "embedded_phone",
				"age_gated", "direct_lending", "channels", "updated_at").
			Where("customer_id = ?", useCase.CustomerID).
			Where("id = ?", useCase.ID).
			Where("status = ?", useCase.Status).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: update use case: %w", err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return ErrUseCaseMoved
		}
		return nil
	}))
}

func clearDefaultUseCase(ctx context.Context, tx bun.Tx, customerID, keep string) error {
	_, err := tx.NewUpdate().Model((*UseCase)(nil)).
		Set("is_default = FALSE").
		Where("customer_id = ?", customerID).
		Where("is_default").
		Where("id <> ?", keep).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: clear default use case: %w", err)
	}
	return nil
}

// Delete removes a use case, its review log and its number assignments.
func (r *UseCases) Delete(ctx context.Context, customerID, id string) error {
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewDelete().Model((*UseCase)(nil)).
			Where("customer_id = ?", customerID).
			Where("id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: delete use case: %w", err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return ErrUnknownUseCase
		}
		_, err = tx.NewUpdate().Model((*PhoneNumber)(nil)).
			Set("dlc_use_case_id = NULL").
			Where("customer_id = ?", customerID).
			Where("dlc_use_case_id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: delete use case: %w", err)
		}
		return nil
	}))
}

// Move writes a use case's new status, vendor fields and timestamps, and the log row
// saying who moved it, as one. from is the status it was read in: a use case that moved
// since answers ErrUseCaseMoved and nothing is written.
func (r *UseCases) Move(ctx context.Context, useCase *UseCase, from string, log *ReviewLog) error {
	now := time.Now().UTC().Truncate(time.Microsecond)
	useCase.UpdatedAt = now
	log.ID = newID()
	log.UseCaseID, log.CustomerID = useCase.ID, useCase.CustomerID
	log.FromStatus, log.ToStatus = from, useCase.Status
	log.CreatedAt = now
	if err := useCase.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if err := log.Validate(); err != nil {
		return stack.Wrap(err)
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewUpdate().Model(useCase).
			Column("status", "vendor", "vendor_campaign_id", "vendor_status", "submitted_at", "approved_at", "updated_at").
			Where("id = ?", useCase.ID).
			Where("status = ?", from).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: move use case: %w", err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return ErrUseCaseMoved
		}
		if _, err := tx.NewInsert().Model(log).Exec(ctx); err != nil {
			return fmt.Errorf("store: move use case: %w", err)
		}
		return nil
	}))
}

// RequestCampaign records that the vendor is about to be asked to register a use case's
// campaign. It reports false, and records nothing, for a use case with a campaign, or one the
// vendor was asked for already: that registration may be running, or its answer was lost, and
// asking again could register and bill a second campaign.
func (r *UseCases) RequestCampaign(ctx context.Context, id string, at time.Time) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*UseCase)(nil)).
		Set("campaign_requested_at = ?", at.UTC()).
		Where("id = ?", id).
		Where("vendor_campaign_id = ''").
		Where("campaign_requested_at IS NULL").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: request campaign: %w", err))
	}
	requested, _ := result.RowsAffected()
	return requested == 1, nil
}

// ForgetCampaignRequest forgets the campaign request made at at, which the vendor did not carry
// out or does not hold, so the campaign may be asked for again. A request made since is
// another's, and is kept.
func (r *UseCases) ForgetCampaignRequest(ctx context.Context, id string, at time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*UseCase)(nil)).
		Set("campaign_requested_at = NULL").
		Where("id = ?", id).
		Where("vendor_campaign_id = ''").
		Where("campaign_requested_at = ?", at.UTC()).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget campaign request: %w", err))
	}
	return nil
}

// AssignNumbers makes numbers exactly the ones that send as a use case. A number named
// leaves whatever use case it sent as before; one no longer named goes back to the default.
func (r *UseCases) AssignNumbers(ctx context.Context, customerID, useCaseID string, numbers []string) error {
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewUpdate().Model((*PhoneNumber)(nil)).
			Set("dlc_use_case_id = NULL").
			Where("customer_id = ?", customerID).
			Where("dlc_use_case_id = ?", useCaseID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: assign numbers: %w", err)
		}
		return assignNumbers(ctx, tx, customerID, useCaseID, numbers)
	}))
}

// assignNumbers makes numbers send as a use case, in tx.
func assignNumbers(ctx context.Context, tx bun.Tx, customerID, useCaseID string, numbers []string) error {
	if len(numbers) == 0 {
		return nil
	}
	result, err := tx.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("dlc_use_case_id = ?", useCaseID).
		Where("customer_id = ?", customerID).
		Where("e164 IN (?)", bun.In(numbers)).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: assign numbers: %w", err)
	}
	if affected, _ := result.RowsAffected(); int(affected) != len(numbers) {
		return fmt.Errorf("store: assign numbers: not every number is one %s holds", customerID)
	}
	return nil
}

// AssignedNumbers returns the numbers assigned to a use case by name, in order.
func (r *UseCases) AssignedNumbers(ctx context.Context, customerID, useCaseID string) ([]string, error) {
	numbers := []string{}
	err := r.store.global.NewSelect().Model((*PhoneNumber)(nil)).
		Column("e164").
		Where("customer_id = ?", customerID).
		Where("dlc_use_case_id = ?", useCaseID).
		Where("released_at IS NULL").
		Order("e164").
		Scan(ctx, &numbers)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: assigned numbers: %w", err))
	}
	return numbers, nil
}

// Numbers returns the numbers that send as a use case: those assigned to it, and for
// the default those assigned to none.
func (r *UseCases) Numbers(ctx context.Context, useCase UseCase) ([]PhoneNumber, error) {
	numbers := []PhoneNumber{}
	query := r.store.global.NewSelect().Model(&numbers).
		Where("customer_id = ?", useCase.CustomerID).
		Where("released_at IS NULL")
	if useCase.IsDefault {
		query = query.WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
			return q.Where("dlc_use_case_id = ?", useCase.ID).WhereOr("dlc_use_case_id IS NULL")
		})
	} else {
		query = query.Where("dlc_use_case_id = ?", useCase.ID)
	}
	if err := query.Order("e164").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: use case numbers: %w", err))
	}
	return numbers, nil
}
