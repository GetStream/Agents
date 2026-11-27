package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

// The key, the tier and the usage categories every usage row is named by.

const usageKey, usageSecret = "vak_live_0123456789abcdef00000000", "vas_live_s3cret"

func keyedForUsage(t *testing.T) Authenticator {
	t.Helper()
	authenticator, err := New(APIKey, lookupOf(usageKey, App{OrganizationID: "org-1", AppID: "app-1", Secret: usageSecret}))
	require.NoError(t, err)
	return authenticator
}

func keyedRequest(t *testing.T, claims jwt.MapClaims) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/v1/calls", nil)
	r.Header.Set(APIKeyHeader, usageKey)
	r.Header.Set("Authorization", "Bearer "+signedClaims(t, usageSecret, claims))
	return r
}

func proxiedRequest(categories string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/calls", nil)
	r.Header.Set(AppHeader, "42")
	r.Header.Set(OrganizationHeader, "7")
	r.Header.Set(AuthTypeHeader, AuthTypeJWT)
	r.Header.Set(UserHeader, "user-1")
	r.Header.Set(APIKeyIDHeader, "key-a")
	r.Header.Set(TierHeader, "tier-a")
	if categories != "" {
		r.Header.Set(UsageCategoriesHeader, categories)
	}
	return r
}

func TestTheProxyNamesTheKeyTierAndCategories(t *testing.T) {
	principal, err := NewProxy(ProxyOptions{DeclaresKind: true}).Authenticate(context.Background(),
		proxiedRequest("project:onboarding,team:finance"))

	require.NoError(t, err)
	require.Equal(t, "key-a", principal.APIKey)
	require.Equal(t, "tier-a", principal.OrganizationTier)
	require.Equal(t, []string{"project:onboarding", "team:finance"}, principal.UsageCategories)
}

func TestAProxyNamingNoCategoriesNamesNone(t *testing.T) {
	principal, err := NewProxy(ProxyOptions{DeclaresKind: true}).Authenticate(context.Background(), proxiedRequest(""))

	require.NoError(t, err)
	require.Nil(t, principal.UsageCategories)
}

func TestAKeyedRequestNamesItsKey(t *testing.T) {
	principal, err := keyedForUsage(t).Authenticate(context.Background(), keyedRequest(t, jwt.MapClaims{"user_id": "user-1"}))

	require.NoError(t, err)
	require.Equal(t, usageKey, principal.APIKey)
	require.Empty(t, principal.OrganizationTier, "only the proxy names a tier")
}

func TestATokensCategoriesAndTeamAreRead(t *testing.T) {
	principal, err := keyedForUsage(t).Authenticate(context.Background(), keyedRequest(t, jwt.MapClaims{
		"user_id": "user-1", "usage_categories": []string{"project:onboarding", "project:onboarding"}, "team": "finance",
	}))

	require.NoError(t, err)
	require.Equal(t, []string{"project:onboarding", "team:finance"}, principal.UsageCategories)
}

func TestATeamAmongATokensCategoriesIsRefused(t *testing.T) {
	_, err := keyedForUsage(t).Authenticate(context.Background(), keyedRequest(t, jwt.MapClaims{
		"user_id": "user-1", "usage_categories": []string{"Team:finance"},
	}))

	require.ErrorIs(t, err, ErrInvalidUsageCategories)
	require.ErrorContains(t, err, "use the team claim")
}

func TestAnEndUserCannotNameCategoriesInTheHeader(t *testing.T) {
	r := keyedRequest(t, jwt.MapClaims{"user_id": "user-1"})
	r.Header.Set(UsageCategoriesHeader, "project:free")

	principal, err := keyedForUsage(t).Authenticate(context.Background(), r)

	require.NoError(t, err)
	require.Nil(t, principal.UsageCategories, "a device's header is a claim its backend never signed")
}

func TestABackendNamesItsCategoriesInTheHeader(t *testing.T) {
	r := keyedRequest(t, jwt.MapClaims{"server": true})
	r.Header.Set(AuthTypeHeader, AuthTypeServer)
	r.Header.Set(UsageCategoriesHeader, "team:finance, project:onboarding")

	principal, err := keyedForUsage(t).Authenticate(context.Background(), r)

	require.NoError(t, err)
	require.Equal(t, []string{"project:onboarding", "team:finance"}, principal.UsageCategories)
}

func TestNoauthNamesItself(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/calls", nil)
	r.Header.Set(CustomerHeader, "examples")
	r.Header.Set(UsageCategoriesHeader, "project:local")

	principal, err := open{}.Authenticate(context.Background(), r)

	require.NoError(t, err)
	require.Equal(t, NoAuthKey, principal.APIKey)
	require.Equal(t, []string{"project:local"}, principal.UsageCategories)
}

func TestInvalidCategoriesAreRefused(t *testing.T) {
	_, err := NewProxy(ProxyOptions{DeclaresKind: true}).Authenticate(context.Background(), proxiedRequest("project onboarding"))

	require.ErrorIs(t, err, ErrInvalidUsageCategories)
}

// Defect: an authenticator that answers a category error with an empty principal, so the
// refusal names no app on its access line and is counted under none.
func TestACategoryRefusalNamesTheAppItsCredentialProved(t *testing.T) {
	local := httptest.NewRequest(http.MethodGet, "/v1/calls", nil)
	local.Header.Set(CustomerHeader, "examples")
	local.Header.Set(UsageCategoriesHeader, "project local")
	backend := keyedRequest(t, jwt.MapClaims{"server": true})
	backend.Header.Set(AuthTypeHeader, AuthTypeServer)
	backend.Header.Set(UsageCategoriesHeader, "project local")

	for _, tc := range []struct {
		name          string
		authenticator Authenticator
		r             *http.Request
		want          Principal
	}{
		{"noauth", open{}, local, Principal{AppID: "examples", APIKey: NoAuthKey}},
		{"proxy", NewProxy(ProxyOptions{DeclaresKind: true}), proxiedRequest("project onboarding"),
			Principal{OrganizationID: "7", AppID: "42", APIKey: "key-a"}},
		{"token claim", keyedForUsage(t), keyedRequest(t, jwt.MapClaims{"user_id": "user-1", "usage_categories": []string{"Team:x"}}),
			Principal{OrganizationID: "org-1", AppID: "app-1", APIKey: usageKey}},
		{"backend header", keyedForUsage(t), backend, Principal{OrganizationID: "org-1", AppID: "app-1", APIKey: usageKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principal, err := tc.authenticator.Authenticate(context.Background(), tc.r)

			require.ErrorIs(t, err, ErrInvalidUsageCategories)
			require.Equal(t, tc.want, Principal{OrganizationID: principal.OrganizationID, AppID: principal.AppID, APIKey: principal.APIKey})
		})
	}
}

// Defect: categories read before the token is verified (from its claims unchecked, or from the
// header of a request whose token fails), so a forged credential is refused under the key's app
// as a 400 instead of as unauthenticated, naming no app.
func TestACategoryRefusalNeedsAVerifiedToken(t *testing.T) {
	signed := func(method jwt.SigningMethod, key any, claims jwt.MapClaims) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/v1/calls", nil)
		r.Header.Set(APIKeyHeader, usageKey)
		token, err := jwt.NewWithClaims(method, claims).SignedString(key)
		require.NoError(t, err)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}
	inTheHeader := func(r *http.Request) *http.Request {
		r.Header.Set(AuthTypeHeader, AuthTypeServer)
		r.Header.Set(UsageCategoriesHeader, "project local")
		return r
	}
	future, past := time.Now().Add(time.Hour).Unix(), time.Now().Add(-time.Hour).Unix()
	badClaim := []string{"Team:x"}

	for _, tc := range []struct {
		name string
		r    *http.Request
	}{
		{"wrong secret, claim", signed(jwt.SigningMethodHS256, []byte("vas_live_not_the_secret"),
			jwt.MapClaims{"user_id": "user-1", "usage_categories": badClaim, "exp": future})},
		{"wrong secret, header", inTheHeader(signed(jwt.SigningMethodHS256, []byte("vas_live_not_the_secret"),
			jwt.MapClaims{"server": true, "exp": future}))},
		{"expired, claim", signed(jwt.SigningMethodHS256, []byte(usageSecret),
			jwt.MapClaims{"user_id": "user-1", "usage_categories": badClaim, "exp": past})},
		{"expired, header", inTheHeader(signed(jwt.SigningMethodHS256, []byte(usageSecret),
			jwt.MapClaims{"server": true, "exp": past}))},
		{"alg none, claim", signed(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType,
			jwt.MapClaims{"usage_categories": badClaim, "exp": future})},
		{"alg none, header", inTheHeader(signed(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType,
			jwt.MapClaims{"server": true, "exp": future}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principal, err := keyedForUsage(t).Authenticate(context.Background(), tc.r)

			require.ErrorIs(t, err, ErrUnauthenticated)
			require.NotErrorIs(t, err, ErrInvalidUsageCategories)
			require.Equal(t, Principal{}, principal)
		})
	}
}

func TestMoreThanTenCategoriesAreRefused(t *testing.T) {
	eleven := make([]string, 11)
	for i := range eleven {
		eleven[i] = "project:" + string(rune('a'+i))
	}

	_, err := NewProxy(ProxyOptions{DeclaresKind: true}).Authenticate(context.Background(), proxiedRequest(strings.Join(eleven, ",")))

	require.ErrorIs(t, err, ErrInvalidUsageCategories)
}

func TestTwoTeamsAreRefused(t *testing.T) {
	_, err := NewProxy(ProxyOptions{DeclaresKind: true}).Authenticate(context.Background(), proxiedRequest("team:finance,Team:sales"))

	require.ErrorIs(t, err, ErrInvalidUsageCategories)
	require.ErrorContains(t, err, "more than one team")
}

func TestATeamOverFiftyNineCharactersIsRefused(t *testing.T) {
	// team:<name> is a category, which is at most 64 characters.
	_, err := keyedForUsage(t).Authenticate(context.Background(), keyedRequest(t, jwt.MapClaims{
		"user_id": "user-1", "team": strings.Repeat("f", 60),
	}))

	require.ErrorIs(t, err, ErrInvalidUsageCategories)
	require.ErrorContains(t, err, "a team is at most 59 characters")
}

func TestATeamOfFiftyNineCharactersIsRead(t *testing.T) {
	team := strings.Repeat("f", 59)

	principal, err := keyedForUsage(t).Authenticate(context.Background(), keyedRequest(t, jwt.MapClaims{
		"user_id": "user-1", "team": team,
	}))

	require.NoError(t, err)
	require.Equal(t, []string{"team:" + team}, principal.UsageCategories)
}
