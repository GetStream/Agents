package routing

import (
	"context"
	"log/slog"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
)

func (s *RoutingSuite) TestAnAppAndUserSplitAtAnotherPlaceHaveAnotherSafetyIdentifier() {
	key := []byte("deployment safety key")

	s.NotEqual(safetyIdentifier(key, "a", "", "bc"), safetyIdentifier(key, "ab", "", "c"),
		"app a's user bc and app ab's user c are two people")
	s.NotEqual(safetyIdentifier(key, "a", "", "bc"), safetyIdentifier(key, "a", "b", "c"),
		"app a's user bc is not app a's party b with user c")
}

func (s *RoutingSuite) TestTheSafetyKeyIsTheKeyringsOwnForSafetyIdentifiers() {
	node, err := auth.NewSealer("deployment kek")
	s.Require().NoError(err)
	otherNode, err := auth.NewSealer("deployment kek")
	s.Require().NoError(err)
	otherDeployment, err := auth.NewSealer("another kek")
	s.Require().NoError(err)
	nodeCredential, _ := node.Derive("node-forward-v1")

	key := SafetyKey(node)

	s.Len(key, 32)
	s.Equal(key, SafetyKey(otherNode), "every node with the keyring hashes a user to the same identifier")
	s.NotEqual(key, SafetyKey(otherDeployment), "another keyring is another key")
	s.NotEqual(nodeCredential, key, "the key is derived for safety identifiers alone")
	s.Nil(SafetyKey(nil), "with no keyring there is no key")
}

// ownModel is a customer's model "prod" on their own endpoint and key.
type ownModel struct{}

func (ownModel) CustomModel(context.Context, string, string) (ProviderConfig, Endpoint, error) {
	return ProviderConfig{Provider: CustomProvider, Model: "prod", Languages: []string{"en"}},
		Endpoint{BaseURL: "https://models.example.com/v1", Model: "prod", APIKey: "their key"}, nil
}

func (s *RoutingSuite) TestACustomersOwnModelIsBuiltWithNoSafetyIdentifier() {
	identifiers := map[string]string{}
	registry := NewRegistry[*stubProvider]()
	for _, provider := range []string{"quick", CustomProvider} {
		registry.Register(provider, func(spec Spec) (*stubProvider, error) {
			identifiers[provider] = spec.SafetyIdentifier
			return &stubProvider{model: spec.Model}, nil
		})
	}
	router, err := New(Options[*stubProvider]{
		Modality: LLM, Config: s.config(), Registry: registry, Models: ownModel{},
		SafetyKey: []byte("deployment safety key"), Logger: slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	for _, target := range []string{"custom/prod", "quick/en"} {
		_, _, err := router.Select(s.ctx, Request{CustomerID: "acme", Attribution: Attribution{UserID: "u1"}, Target: target})
		s.Require().NoError(err, target)
	}

	s.Empty(identifiers[CustomProvider], "the customer's own model is on their own account")
	s.NotEmpty(identifiers["quick"], "a model on the deployment's key in the same router is identified")
}
