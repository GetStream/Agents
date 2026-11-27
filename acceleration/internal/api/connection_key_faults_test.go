//go:build integration

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/apikey"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// refusedKMSKey stands for a deployment's Cloud KMS key, in the resource name format Cloud KMS
// errors quote. Synthetic.
const refusedKMSKey = "projects/example-project/locations/global/keyRings/example-ring/cryptoKeys/example-key"

// TestAKMSFailureIsA503ThatRecordsNothing: KMS refusing to unwrap the app's data key says
// nothing about the connection. Validate, a credentials write and a token export each answer
// 503 with a sentence that names neither the KMS key nor the vendor's text, the request log has
// the cause, and the connection keeps its status, revision and last validation.
func (s *ConnectionToolsSuite) TestAKMSFailureIsA503ThatRecordsNothing() {
	validated := s.connected(bearer.Name)
	before := s.validated(validated)
	exported := s.connection(apikey.Name)
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/connections/"+exported+"/credentials",
		map[string]any{"expected_revision": 1, "values": map[string]string{apikey.SuppliedKey: s.token, apikey.SuppliedHeader: "X-Api-Key"}}, nil))
	s.underDataKey(validated)
	s.underDataKey(exported)
	s.kms.Fail(fmt.Errorf("cloud kms: decrypt with %s: %w", refusedKMSKey,
		status.Error(codes.PermissionDenied, "Permission 'cloudkms.cryptoKeyVersions.useToDecrypt' denied on resource '"+refusedKMSKey+"'")))
	s.T().Cleanup(func() { s.kms.Fail(nil) })

	s.answersKeyUnavailable(appkeys.ErrUnavailable, http.MethodPost, "/v1/agents/connections/"+validated+"/validate", nil)
	s.answersKeyUnavailable(appkeys.ErrUnavailable, http.MethodPut, "/v1/agents/connections/"+validated+"/credentials", s.bearerToken(2, "another-token"))
	s.answersKeyUnavailable(appkeys.ErrUnavailable, http.MethodPost, tokenPath(exported), nil)

	connection := s.get(validated)
	s.Equal(ConnectionStatus(store.ConnectionConnected), connection.Status)
	s.Equal(2, connection.Revision, "the credentials write stored nothing")
	s.Require().NotNil(connection.LastValidation)
	s.True(before.Equal(connection.LastValidation.CheckedAt), "the last validation is the one before KMS failed")
}

// TestCredentialsUnderAKeyVersionTheRouterDoesNotHoldAreA503: the keyring losing a version is
// the same kind of fault, answered the same way, and leaves the last validation alone.
func (s *ConnectionToolsSuite) TestCredentialsUnderAKeyVersionTheRouterDoesNotHoldAreA503() {
	id := s.connected(bearer.Name)
	before := s.validated(id)
	_, err := s.store.GlobalDB().ExecContext(s.T().Context(),
		"UPDATE connector_connections SET credentials_kek_version = 7 WHERE id = ?", id)
	s.Require().NoError(err)

	s.answersKeyUnavailable(auth.ErrKeyVersionUnavailable, http.MethodPost, "/v1/agents/connections/"+id+"/validate", nil)

	connection := s.get(id)
	s.Equal(ConnectionStatus(store.ConnectionConnected), connection.Status)
	s.Require().NotNil(connection.LastValidation)
	s.True(before.Equal(connection.LastValidation.CheckedAt), "the last validation is the one before the key went")
}

// validated stores a last validation for the connection, as a validate that found it connected
// would, and returns when it ran. A real validate would leave the credential in the resolver's
// cache, and the next validate would not open the stored credentials.
func (s *ConnectionToolsSuite) validated(id string) time.Time {
	checked := time.Now().UTC().Truncate(time.Microsecond)
	s.Require().NoError(s.store.ConnectorConnections.PutValidation(s.T().Context(), s.customerID(), id,
		store.ConnectorConnectionValidation{Revision: 2, Status: validationConnected, CheckedAt: checked}))
	return checked
}

// underDataKey marks the connection's credentials as sealed under a new version of the app's
// data key and stores that key. The blob stays as it was: KMS refuses before it is read.
func (s *ConnectionToolsSuite) underDataKey(id string) {
	ctx := s.T().Context()
	var version int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0) + 1 FROM app_data_keys WHERE customer_id = ?", s.customerID()).Scan(&version))
	appkeystest.AddKey(s.T(), s.store, s.kms, s.customerID(), version)
	_, err := s.store.GlobalDB().ExecContext(ctx,
		"UPDATE connector_connections SET credentials_data_key_version = ? WHERE id = ?", version, id)
	s.Require().NoError(err)
}

// answersKeyUnavailable sends the request and checks it is answered with
// errCredentialKeyUnavailable alone, and that the request log keeps the cause's text.
func (s *ConnectionToolsSuite) answersKeyUnavailable(cause error, method, path string, body any) {
	response := s.serverClient.raw(method, path, body)
	read, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	var answer struct{ Error APIError }
	s.Require().NoError(json.Unmarshal(read, &answer), string(read))
	for _, internal := range []string{"projects/", "rpc error", "app_keys_kms_key", "pgsealed", "appkeys"} {
		s.NotContains(string(read), internal, path)
	}
	s.Equal(http.StatusServiceUnavailable, response.StatusCode, path)
	s.Equal(errCredentialKeyUnavailable.Message, answer.Error.Message, path)
	s.Equal("unavailable", answer.Error.Code, path)
	requestID := response.Header.Get(RequestIDHeader)
	served := func() string { return lineWith(s.logged, `msg="served a request"`, " request_id="+requestID) }
	s.Require().Eventually(func() bool { return served() != "" }, 5*time.Second, 20*time.Millisecond, path)
	s.Contains(served(), cause.Error(), "the log keeps the cause of "+path)
}
