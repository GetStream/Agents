# internal/connectors/sensitive

The rule that keeps a connector whose manifest says `sensitive: true` (its tools read private data) away from what may send data out with no person's approval: a binding to a source that does not say `outbound: false` (unless each tool it grants is in `read_only_tools` or its `require_approval`), a binding with `sandbox` set, and a sandbox that reaches the network.

- `ConfigComplaint` checks one agent config. Config writes call it (`internal/api`: create, update, patch, sync), so a config that breaks it is a 400.
- `GraphComplaint` checks a parent and its child agents as one config. A parent hands its input to each child and reads each answer, so what one reads may reach any other, a sibling's tools included. Config writes call it (`sensitiveGraphComplaint`, `internal/api/child_agents.go`) over the config with its children and over each parent that calls the config with all of that parent's children, so a set that breaks it is a 400 naming the agents, the bindings and the connector, even for a save that changes another field. `childRunner.Run` (`internal/session/child_agents.go`) calls it on every delegation, with the parent as it runs and each child as stored now, and refuses a delegation that breaks it with a tool error the model reads: it covers a set stored before the writes checked it.

It lives here, not in `internal/api`, because `internal/api` imports `internal/session`, and both need it. It reads only the store and the manifest model.

Check: `go test -tags integration -run 'TestChildAgentsSuite/(TestADelegation|TestAWrite)|TestConfigsSuite/TestNessy' ./internal/api`.
