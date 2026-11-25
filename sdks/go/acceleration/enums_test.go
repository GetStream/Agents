package acceleration_test

import (
	"testing"

	"github.com/GetStream/Vision-Agents/sdks/go/acceleration"
)

// TestAgentLogSourceKeepsItsConstantNames pins names callers already use. oapi-codegen
// prefixes every constant of two enums that share a value (enumsConflict in
// pkg/codegen/codegen.go, v2.8.0), so a new enum with agent, system, tool or user in it
// renames these unless it names its own constants with x-enum-varnames. Renamed, this file
// no longer compiles.
func TestAgentLogSourceKeepsItsConstantNames(t *testing.T) {
	named := map[acceleration.AgentLogSource]string{
		acceleration.Agent:  "agent",
		acceleration.System: "system",
		acceleration.Tool:   "tool",
		acceleration.User:   "user",
	}
	for constant, value := range named {
		if string(constant) != value {
			t.Errorf("%q is spelled %q", value, constant)
		}
	}
}

// TestInvocationArgumentTypeKeepsItsConstantNames pins the names v0.5.1 released, which
// CustomHTTPParameterType's string, number and boolean would rename the same way.
func TestInvocationArgumentTypeKeepsItsConstantNames(t *testing.T) {
	named := map[acceleration.InvocationArgumentType]string{
		acceleration.Array:   "array",
		acceleration.Boolean: "boolean",
		acceleration.Null:    "null",
		acceleration.Number:  "number",
		acceleration.Object:  "object",
		acceleration.String:  "string",
	}
	for constant, value := range named {
		if string(constant) != value {
			t.Errorf("%q is spelled %q", value, constant)
		}
	}
}

// TestCustomHTTPToolMethodNamesItsConstants pins prefixed names for the http methods, so the
// next enum with GET or POST in it renames nothing callers already use.
func TestCustomHTTPToolMethodNamesItsConstants(t *testing.T) {
	named := map[acceleration.CustomHTTPToolMethod]string{
		acceleration.CustomHTTPToolMethodGET:    "GET",
		acceleration.CustomHTTPToolMethodHEAD:   "HEAD",
		acceleration.CustomHTTPToolMethodPOST:   "POST",
		acceleration.CustomHTTPToolMethodPUT:    "PUT",
		acceleration.CustomHTTPToolMethodPATCH:  "PATCH",
		acceleration.CustomHTTPToolMethodDELETE: "DELETE",
	}
	for constant, value := range named {
		if string(constant) != value {
			t.Errorf("%q is spelled %q", value, constant)
		}
	}
}
