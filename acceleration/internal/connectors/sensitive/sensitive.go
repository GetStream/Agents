// Package sensitive keeps a connector whose tools read private data away from tools that send
// data out, in one agent config and across a parent and the child agents it hands work to.
package sensitive

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	httpsource "github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sources/http"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ConfigComplaint refuses a config that binds a sensitive connector, one whose manifest says
// its tools read private data, together with a tool that sends data out: a model that reads
// untrusted text could be steered into sending out what it read («Nessy as a connector» on
// AI-1538, change 13). These send data out:
//   - a binding whose connector has a source that does not say outbound: false, as one of its
//     tools may write, unless each tool it grants is in the source's read_only_tools or its
//     require_approval, which holds every call for a person to allow, and a binding with
//     sandbox set, whose code calls the connector's whole API;
//   - a sandbox that may reach the network.
//
// Only a binding's require_approval asks a person before a call is sent; the others ask
// nobody. It reads the config as it is about to be stored.
func ConfigComplaint(ctx context.Context, definitions store.Definitions, customerID string, config store.AgentConfig) (string, bool, error) {
	found, err := exposureOf(ctx, definitions, customerID, config)
	if err != nil {
		return "", false, err
	}
	if found.sensitive == nil || len(found.outbound) == 0 {
		return "", true, nil
	}
	message := fmt.Sprintf("connector binding %q binds %q, whose tools read private data, and %s can send data out "+
		"with no person's approval: take them out of this config", found.sensitive.Name, found.sensitive.ConnectorID,
		strings.Join(found.outbound, ", "))
	if len(found.unheld) > 0 {
		message += ", or add to each binding's require_approval its tools that do not only read: " +
			strings.Join(found.unheld, "; ")
	}
	return message, false, nil
}

// GraphComplaint is ConfigComplaint over a parent and its child agents taken as one config: a
// parent hands its input to each child and reads each child's answer, so what one of them
// reads may reach any other, a sibling's tools included, through the parent.
func GraphComplaint(ctx context.Context, definitions store.Definitions, customerID string, configs []store.AgentConfig) (string, bool, error) {
	var sensitive *store.ConnectorBinding
	var holder string
	var outbound, unheld []string
	for _, config := range configs {
		found, err := exposureOf(ctx, definitions, customerID, config)
		if err != nil {
			return "", false, err
		}
		if found.sensitive != nil {
			sensitive, holder = found.sensitive, config.Name
		}
		for _, out := range found.outbound {
			outbound = append(outbound, fmt.Sprintf("%s of the agent %q", out, config.Name))
		}
		for _, tools := range found.unheld {
			unheld = append(unheld, fmt.Sprintf("the agent %q's %s", config.Name, tools))
		}
	}
	if sensitive == nil || len(outbound) == 0 {
		return "", true, nil
	}
	message := fmt.Sprintf("the agent %q's connector binding %q binds %q, whose tools read private data, and %s "+
		"can send data out with no person's approval, and a parent and its child agents pass on to each other "+
		"what they read: take them out of these agents", holder, sensitive.Name, sensitive.ConnectorID,
		strings.Join(outbound, ", "))
	if len(unheld) > 0 {
		message += ", or add to each binding's require_approval its tools that do not only read: " +
			strings.Join(unheld, "; ")
	}
	return message, false, nil
}

// exposure is what one config holds that the rule weighs: a binding to a sensitive connector,
// what may send data out, and the tools by binding that a require_approval would hold.
type exposure struct {
	sensitive *store.ConnectorBinding
	outbound  []string
	unheld    []string
}

func exposureOf(ctx context.Context, definitions store.Definitions, customerID string, config store.AgentConfig) (exposure, error) {
	var found exposure
	for i, binding := range config.Connectors {
		definition, err := definitions.Latest(ctx, customerID, binding.ConnectorID)
		// A forced connector delete leaves its bindings behind (store.refuseUndefined), and
		// a binding to a connector with no definition opens nothing.
		if errors.Is(err, store.ErrNoConnectorDefinition) {
			continue
		}
		if err != nil {
			return exposure{}, err
		}
		if definition.Manifest.Sensitive {
			found.sensitive = &config.Connectors[i]
		}
		if tools, sends := bindingSendsOut(binding, definition.Manifest); sends {
			found.outbound = append(found.outbound, fmt.Sprintf("connector binding %q", binding.Name))
			if len(tools) > 0 {
				found.unheld = append(found.unheld, fmt.Sprintf("%q %s", binding.Name, strings.Join(tools, ", ")))
			}
		}
	}
	if config.Sandbox.Provider != "" && !config.Sandbox.Options.Isolated() {
		found.outbound = append(found.outbound, "the sandbox's network access")
	}
	return found, nil
}

// bindingSendsOut reports whether a binding offers a tool that may send data out with nobody
// asked, and the names or patterns it grants such tools by: one of a source that does not say
// outbound: false, unless the binding grants it by name and the source lists it in
// read_only_tools, or its require_approval holds it, or the connector's API to sandbox code,
// which names none. A tool pattern matches tools only a session lists, so it sends out if any
// source may and its require_approval does not hold all it matches. A binding does not say
// which source a granted name comes from, so it sends out if any source may send it. A
// connector with no source offers no tool, and neither does a binding that grants none.
func bindingSendsOut(binding store.ConnectorBinding, manifest core.Manifest) ([]string, bool) {
	if binding.Sandbox {
		return nil, true
	}
	var unheld []string
	if binding.ToolPatterns != nil {
		if !slices.ContainsFunc(manifest.Sources, core.SourceRule.SendsOut) {
			return nil, false
		}
		for _, pattern := range *binding.ToolPatterns {
			if !holdsPattern(binding, pattern) {
				unheld = append(unheld, pattern)
			}
		}
		return unheld, len(unheld) > 0
	}
	for _, tool := range binding.Tools {
		if GrantSendsOut(manifest, tool) && !binding.RequiresApproval(tool.Name) {
			unheld = append(unheld, tool.Name)
		}
	}
	return unheld, len(unheld) > 0
}

// GrantSendsOut reports whether the tool that grant names may send data out of the conversation
// (core.Manifest.SendsOutTool), with an http tool judged as the grant pins it: a session opens
// one only at the digest its grant pins (httpsource.Source.Open), and the digest covers its
// method, path and read_only (httpsource.ToolSchemaDigest). So an http tool that only reads at
// the latest revision still sends out when the grant pins no digest, which a session pins on
// first use at the revision its connection is on, or another revision's, such as a POST that
// revision 2 turned into a read-only GET while a connection stays on revision 1 (AI-3675). An
// mcp tool is judged at the latest revision alone, as its read_only_tools say what the server's
// tool does, whatever revision lists it.
func GrantSendsOut(manifest core.Manifest, grant store.ToolGrant) bool {
	if source := core.HTTPSource(manifest.Sources); source != nil {
		for _, tool := range source.HTTPTools {
			if tool.Name != grant.Name {
				continue
			}
			// The digest is JSON of strings and maps, which always encodes: an error is no
			// digest, which pins nothing.
			digest, err := httpsource.ToolSchemaDigest(tool)
			if err != nil || digest != grant.SchemaDigest {
				return true
			}
		}
	}
	return manifest.SendsOutTool(grant.Name)
}

// holdsPattern reports whether the binding's require_approval holds every tool a tool pattern
// matches: an entry does when it matches the pattern as written and has no wildcard or escape
// but *, such as * or create_* for create_*, as each * then also matches what the pattern's
// wildcards stand for. An entry's ? or class matches one character where the pattern's * may
// stand for several, an escaped \* only the pattern's own *, and the pattern's class may stand
// for a / that no * matches, so none of them is taken to hold the pattern.
func holdsPattern(binding store.ConnectorBinding, pattern string) bool {
	if strings.Contains(pattern, "[") {
		return false
	}
	return slices.ContainsFunc(binding.RequireApproval, func(entry string) bool {
		matched, _ := path.Match(entry, pattern)
		return matched && !strings.ContainsAny(entry, `?[\`)
	})
}
