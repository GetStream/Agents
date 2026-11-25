// Package providers holds the built-in connector manifests, one <id>.yaml per connector.
// They are embedded so the router binary carries them, and store.ConnectorDefinitions.Seed
// writes them to connector_definitions at every start.
package providers

import "embed"

// FS holds the manifests.
//
//go:embed *.yaml
var FS embed.FS
