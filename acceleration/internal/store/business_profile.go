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

// BusinessProfile is who an app is, written once and reused by every channel it registers
// for. Which fields a channel needs is the dlc package's to say, so none is required here.
type BusinessProfile struct {
	bun.BaseModel `bun:"table:business_profiles,alias:bp"`

	CustomerID            string        `bun:"customer_id,pk"`
	LegalBusinessName     string        `bun:"legal_business_name,notnull"`
	BrandName             string        `bun:"brand_name,notnull"`
	LegalEntityType       string        `bun:"legal_entity_type,notnull"`
	OrganizationType      string        `bun:"organization_type,notnull"`
	RegistrationCountry   string        `bun:"business_registration_country,notnull"`
	TaxID                 string        `bun:"tax_id,notnull"`
	TaxIDCountry          string        `bun:"tax_id_issuing_country,notnull"`
	Address               PostalAddress `bun:"registered_address,type:jsonb,notnull"`
	WebsiteURL            string        `bun:"website_url,notnull"`
	Industry              string        `bun:"industry,notnull"`
	ContactFirstName      string        `bun:"authorized_contact_first_name,notnull"`
	ContactLastName       string        `bun:"authorized_contact_last_name,notnull"`
	ContactTitle          string        `bun:"authorized_contact_title,notnull"`
	ContactEmail          string        `bun:"authorized_contact_email,notnull"`
	ContactPhone          string        `bun:"authorized_contact_phone,notnull"`
	PrivacyPolicyURL      string        `bun:"privacy_policy_url,notnull"`
	TermsURL              string        `bun:"terms_and_conditions_url,notnull"`
	StockSymbol           string        `bun:"stock_symbol,notnull"`
	StockExchange         string        `bun:"stock_exchange,notnull"`
	VerificationDocuments []string      `bun:"business_verification_documents,array"`
	// VendorBrandID is the brand the vendor registered the app as, and BrandStatus the
	// vendor's own word for where it stands. Empty until a use case was sent.
	VendorBrandID string `bun:"vendor_brand_id,notnull"`
	BrandStatus   string `bun:"brand_status,notnull"`
	// BrandRequestedAt is when the vendor was asked to register the brand (RequestBrand). Set
	// with no VendorBrandID, the vendor's answer was lost.
	BrandRequestedAt *time.Time `bun:"brand_requested_at"`
	CreatedAt        time.Time  `bun:"created_at,notnull"`
	UpdatedAt        time.Time  `bun:"updated_at,notnull"`
}

// PostalAddress is where a business is registered.
type PostalAddress struct {
	Street     string `json:"street,omitempty"`
	City       string `json:"city,omitempty"`
	State      string `json:"state,omitempty" doc:"State or region."`
	PostalCode string `json:"postal_code,omitempty"`
	Country    string `json:"country,omitempty" doc:"ISO 3166-1 alpha-2."`
}

// Validate refuses a profile no app owns. Which other fields a channel needs is the dlc
// package's to say.
func (p *BusinessProfile) Validate() error {
	return validate("business profile").required("customer_id", p.CustomerID).done()
}

// BusinessProfiles are who apps said they are, in the shared database.
type BusinessProfiles struct {
	store *Store
}

// ErrNoBusinessProfile is an app that has not said who it is yet.
var ErrNoBusinessProfile = errors.New("store: this app has no business profile")

// Get returns who an app said it is.
func (r *BusinessProfiles) Get(ctx context.Context, customerID string) (BusinessProfile, error) {
	if customerID == "" {
		return BusinessProfile{}, stack.Wrap(ErrNoBusinessProfile)
	}
	var profile BusinessProfile
	err := r.store.global.NewSelect().Model(&profile).Where("customer_id = ?", customerID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return BusinessProfile{}, stack.Wrap(ErrNoBusinessProfile)
	}
	if err != nil {
		return BusinessProfile{}, stack.Wrap(fmt.Errorf("store: business profile: %w", err))
	}
	return profile, nil
}

// GetByBrand returns the profile a vendor registered as brandID. A vendor reports on a brand
// rarely and an app holds one profile, so the read has no index of its own.
func (r *BusinessProfiles) GetByBrand(ctx context.Context, brandID string) (BusinessProfile, error) {
	if brandID == "" {
		return BusinessProfile{}, stack.Wrap(ErrNoBusinessProfile)
	}
	var profile BusinessProfile
	err := r.store.global.NewSelect().Model(&profile).Where("vendor_brand_id = ?", brandID).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return BusinessProfile{}, stack.Wrap(ErrNoBusinessProfile)
	}
	if err != nil {
		return BusinessProfile{}, stack.Wrap(fmt.Errorf("store: business profile by brand: %w", err))
	}
	return profile, nil
}

// Upsert writes an app's profile, replacing what it said before. The brand the
// vendor registered stays: it is the vendor's to change, through SetBrand.
func (r *BusinessProfiles) Upsert(ctx context.Context, profile *BusinessProfile) error {
	if err := profile.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	profile.CreatedAt, profile.UpdatedAt = now, now
	if profile.VerificationDocuments == nil {
		profile.VerificationDocuments = []string{}
	}
	_, err := r.store.global.NewInsert().Model(profile).
		On("CONFLICT (customer_id) DO UPDATE").
		Set("legal_business_name = EXCLUDED.legal_business_name").
		Set("brand_name = EXCLUDED.brand_name").
		Set("legal_entity_type = EXCLUDED.legal_entity_type").
		Set("organization_type = EXCLUDED.organization_type").
		Set("business_registration_country = EXCLUDED.business_registration_country").
		Set("tax_id = EXCLUDED.tax_id").
		Set("tax_id_issuing_country = EXCLUDED.tax_id_issuing_country").
		Set("registered_address = EXCLUDED.registered_address").
		Set("website_url = EXCLUDED.website_url").
		Set("industry = EXCLUDED.industry").
		Set("authorized_contact_first_name = EXCLUDED.authorized_contact_first_name").
		Set("authorized_contact_last_name = EXCLUDED.authorized_contact_last_name").
		Set("authorized_contact_title = EXCLUDED.authorized_contact_title").
		Set("authorized_contact_email = EXCLUDED.authorized_contact_email").
		Set("authorized_contact_phone = EXCLUDED.authorized_contact_phone").
		Set("privacy_policy_url = EXCLUDED.privacy_policy_url").
		Set("terms_and_conditions_url = EXCLUDED.terms_and_conditions_url").
		Set("stock_symbol = EXCLUDED.stock_symbol").
		Set("stock_exchange = EXCLUDED.stock_exchange").
		Set("business_verification_documents = EXCLUDED.business_verification_documents").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("*").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save business profile: %w", err))
	}
	return nil
}

// SetBrand records the brand a vendor registered an app as, and where it stands.
func (r *BusinessProfiles) SetBrand(ctx context.Context, customerID, brandID, status string) error {
	_, err := r.store.global.NewUpdate().Model((*BusinessProfile)(nil)).
		Set("vendor_brand_id = ?", brandID).
		Set("brand_status = ?", status).
		Set("updated_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set brand: %w", err))
	}
	return nil
}

// RequestBrand records that the vendor is about to be asked to register the app's brand. It
// reports false, and records nothing, for an app with a brand, or one the vendor was asked for
// already: that registration may be running, or its answer was lost, and asking again could
// register and bill a second brand.
func (r *BusinessProfiles) RequestBrand(ctx context.Context, customerID string, at time.Time) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*BusinessProfile)(nil)).
		Set("brand_requested_at = ?", at.UTC()).
		Where("customer_id = ?", customerID).
		Where("vendor_brand_id = ''").
		Where("brand_requested_at IS NULL").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: request brand: %w", err))
	}
	requested, _ := result.RowsAffected()
	return requested == 1, nil
}

// ForgetBrandRequest forgets the brand request made at at, which the vendor did not carry out,
// so the brand may be asked for again. A request made since is another's, and is kept.
func (r *BusinessProfiles) ForgetBrandRequest(ctx context.Context, customerID string, at time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*BusinessProfile)(nil)).
		Set("brand_requested_at = NULL").
		Where("customer_id = ?", customerID).
		Where("vendor_brand_id = ''").
		Where("brand_requested_at = ?", at.UTC()).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget brand request: %w", err))
	}
	return nil
}
