//go:build integration

package api

import (
	"cmp"
	"log/slog"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/vendors"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// A vendor fetching a plan while a shard that may park it is down is told to fetch again, not
// that the call is not waiting: the bridge may be on that shard.
func (s *CallStatusSuite) TestAPlanFetchWhileAShardIsDownIsAnsweredForTheVendorToRetry() {
	s.useApp(s.data.createApp())
	options := s.storeOptions
	// Nothing answers on port 1.
	options.Shards = map[string]string{
		"here": cmp.Or(options.ShardDSN, options.DSN), "gone": "postgres://postgres:postgres@127.0.0.1:1/gone?sslmode=disable",
	}
	served, err := store.OpenShards(options)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(served.Close()) })
	config, err := phone.DefaultConfig()
	s.Require().NoError(err)
	telephony, err := phone.NewService(phone.ServiceOptions{Registry: vendors.Registry(config), Store: served, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	other, _, _ := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler), func(o *Options) {
		o.Store, o.Phone = served, telephony
	})

	token := s.utils.uuid()
	s.Equal(http.StatusServiceUnavailable, s.unauthenticatedClient.on(other).do(http.MethodGet, "/v1/phone/answer/"+token, nil, nil))
	s.Equal(http.StatusNotFound, s.unauthenticatedClient.do(http.MethodGet, "/v1/phone/answer/"+token, nil, nil),
		"with every shard answering, it is not waiting")
}
