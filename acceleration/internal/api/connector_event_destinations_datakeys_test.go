//go:build integration

package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/providers"
	"github.com/GetStream/Vision-Agents/acceleration/internal/eventforward"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/webhook"
)

// EventDestinationDataKeysSuite creates and rotates event destinations with
// auth.app_keys_seal on, the apps' data keys wrapped by an in-process KMS.
type EventDestinationDataKeysSuite struct {
	RouterSuite
	kms *appkeystest.KMS
}

func TestEventDestinationDataKeysSuite(t *testing.T) {
	runSuite(t, new(EventDestinationDataKeysSuite))
}

func (s *EventDestinationDataKeysSuite) SetupSuite() {
	s.kms = appkeystest.NewKMS()
	s.dataKeys, s.sealDataKeys = s.kms, true
	s.forwardHTTP = destinationClient()
	s.RouterSuite.SetupSuite()
	s.Require().NoError(s.store.ConnectorDefinitions.Seed(context.Background(), providers.FS))
}

func (s *EventDestinationDataKeysSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// A create and a rotation seal the new secret under the app's data key, the rotation keeps the
// old one beside it under the same key, and a receiver verifies a forward with either.
func (s *EventDestinationDataKeysSuite) TestACreateAndARotationSealTheSecretUnderTheAppsDataKey() {
	target := newDestination(s.T())
	created := s.createDestination("slack_bot", target.URL, store.ForwardAll)
	stored := s.storedDestination(created.Destination.ID)
	s.Equal(new(1), stored.SecretDataKeyVersion, "the create seals under the data key")
	s.Equal(1, stored.KEKVersion)

	var rotated ConnectorEventDestinationSecret
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		destinationPath("slack_bot", created.Destination.ID)+"/rotate-secret", nil, &rotated))
	stored = s.storedDestination(created.Destination.ID)
	s.Equal(new(1), stored.SecretDataKeyVersion, "the rotation seals under the data key")
	s.Equal(new(1), stored.PreviousSecretDataKeyVersion)

	s.Require().NoError(s.forwarder.Forward(context.Background(), eventforward.Event{
		CustomerID: s.customerID(), ConnectorID: "slack_bot", Body: []byte(`{"type":"event_callback"}`),
	}))
	s.Require().Eventually(func() bool { return len(target.requests()) == 1 }, 5*time.Second, 10*time.Millisecond)
	request := target.requests()[0]
	s.Len(strings.Fields(request.header.Get("webhook-signature")), 2)
	for _, secret := range []string{rotated.Secret, created.Secret} {
		s.Require().NoError(webhook.Verify(secret, request.header, request.body, time.Now()))
	}
}

// storedDestination reads one of the app's destinations as the table holds it.
func (s *EventDestinationDataKeysSuite) storedDestination(id string) store.EventDestination {
	var destination store.EventDestination
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&destination).Where("id = ?", id).
		Where("customer_id = ?", s.customerID()).Scan(context.Background()))
	return destination
}
