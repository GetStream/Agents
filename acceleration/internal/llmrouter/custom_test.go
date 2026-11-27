package llmrouter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type CustomModelsUnitSuite struct {
	suite.Suite
}

func TestCustomModelsUnitSuite(t *testing.T) {
	suite.Run(t, new(CustomModelsUnitSuite))
}

// A database that cannot be reached says nothing about whether the customer holds the model.
func (s *CustomModelsUnitSuite) TestADatabaseFailureIsNoRefusal() {
	down, err := store.Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = down.Close() })

	_, _, err = NewCustomModels(down, nil, false).CustomModel(s.T().Context(), "acme", "gemma")

	s.Require().Error(err)
	s.NotErrorIs(err, routing.ErrRequest)
	s.NotErrorIs(err, store.ErrNoCustomModel)
}

// A private endpoint is a tenant's server too: response headers past the cap are refused, ones within it pass.
func (s *CustomModelsUnitSuite) TestAPrivateEndpointsResponseHeadersAreCapped() {
	for name, size := range map[string]int{"within": 512 << 10, "past": 2 << 20} {
		s.Run(name, func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Filler", strings.Repeat("a", size))
			}))
			defer server.Close()

			response, err := NewCustomModels(nil, nil, true).client.Get(server.URL)

			if name == "within" {
				s.Require().NoError(err)
				_ = response.Body.Close()
				return
			}
			s.ErrorContains(err, "response headers exceeded")
		})
	}
}
