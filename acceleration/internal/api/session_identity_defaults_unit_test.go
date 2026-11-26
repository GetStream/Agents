package api

import (
	"reflect"
	"strings"
	"testing"
)

// A schema default on who the agent is goes into every generated client, which then sends it and
// replaces what the agent config names; and these fields are the backend's to send (CHA-7262).
func TestTheAgentsIdentityFieldsHaveNoDefaultAndSayTheyAreServerSide(t *testing.T) {
	request := reflect.TypeFor[CreateSessionRequest]()
	for _, name := range []string{"UserId", "UserName", "AgentId"} {
		field, ok := request.FieldByName(name)
		if !ok {
			t.Fatalf("CreateSessionRequest has no %s", name)
		}
		if def, has := field.Tag.Lookup("default"); has {
			t.Errorf("%s has the schema default %q", name, def)
		}
		if !strings.Contains(field.Tag.Get("doc"), "Server-side only") {
			t.Errorf("%s's doc does not say it is server-side", name)
		}
	}
}
