package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// DraftOfTag is the tag that marks an agent config as a test copy: a temporary copy the
// dashboard saves of another config's unsaved changes, to run a test on, naming the config it
// copies. The dashboard writes it (DRAFT_OF_TAG = 'draft_of' in volt-dashboard
// src/components/dashboard/agents/lib/draft-copies.ts:9, set on the copy in
// use-test-draft.ts:85, branch connectors/agents-ui-e at 9441e26fd4) and removes the copy after
// the test, or sweeps it once it is 2 h old (STALE_COPY_MS, agent-copies.ts:117). The router has
// no field of its own for a copy, so it reads the tag.
const DraftOfTag = "draft_of"

// TestCopy reports whether the config is a test copy (DraftOfTag). A test copy answers no
// channel message and subscribes to no event: those belong to the live config it copies, the
// one channel and event connection's owner (AI-1049, AI-1048).
func (c AgentConfig) TestCopy() bool {
	return c.Tags[DraftOfTag] != ""
}

// notTestCopy is the WHERE clause that leaves test copies out of a select of agent_configs.
const notTestCopy = "coalesce(tags->>'" + DraftOfTag + "', '') = ''"

// channelOwnerLockSeed is the second argument to hashtextextended for the lock a channel
// connection's owner is decided under. Any value other than the 731 RecordAgentLog uses and
// definitionLockSeed (833) would do; 1049 is this rule's issue, AI-1049.
const channelOwnerLockSeed = 1049

// ChannelConnectionTakenError is a write that would have a second live agent config bind a
// channel connection as fixed. A channel connection, one whose connector reads messages (its
// manifest's channel block has messages, as slack_bot's has), answers each message with one
// agent, so it belongs to the live config that bound it first.
type ChannelConnectionTakenError struct {
	// Binding is the binding of the refused config that names the connection.
	Binding      string
	ConnectionID string
	ConnectorID  string
	// OwnerID and OwnerName are the live config that already binds the connection.
	OwnerID   string
	OwnerName string
}

func (e *ChannelConnectionTakenError) Error() string {
	return fmt.Sprintf("store: connection %s (%s) is bound by agent config %s already", e.ConnectionID, e.ConnectorID, e.OwnerID)
}

// refuseSecondChannelAgent refuses a write of a live config that newly binds, as fixed, a
// channel connection another live config binds already. kept are the bindings stored before
// this write, and keptCopy whether the stored config was a test copy: a save that keeps a
// binding the live config had is not a new bind, so a config that already shared a connection
// before this rule (the oldest of them answers, take in internal/channelbridge) can still be
// saved. A test copy binds what it likes, since it answers nothing.
//
// The decision runs under a transaction-scoped advisory lock on the customer and connection,
// so two routers saving two configs that bind one connection at once take turns: the second
// select runs after the first's commit, and under READ COMMITTED it sees that config
// (https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED). The
// locks are taken in id order, so two writes binding the same two connections never wait on
// each other in a cycle.
func refuseSecondChannelAgent(ctx context.Context, tx bun.Tx, config *AgentConfig, kept []ConnectorBinding, keptCopy bool) error {
	if config.TestCopy() {
		return nil
	}
	named := map[string]string{}
	for _, binding := range config.Connectors {
		id := binding.Connection.ConnectionID
		if _, seen := named[id]; binding.Connection.Type != "fixed" || seen {
			continue
		}
		if !keptCopy && slices.ContainsFunc(kept, func(b ConnectorBinding) bool {
			return b.Connection.Type == "fixed" && b.Connection.ConnectionID == id
		}) {
			continue
		}
		named[id] = binding.Name
	}
	ids := make([]string, 0, len(named))
	for id := range named {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		connectorID, channel, err := lockChannelOwner(ctx, tx, config.CustomerID, id)
		if err != nil {
			return err
		}
		if !channel {
			continue
		}
		var owner AgentConfig
		err = tx.NewSelect().Model(&owner).Column("id", "name").
			Where("customer_id = ?", config.CustomerID).
			Where("id != ?", config.ID).
			Where("deleted_at IS NULL").
			Where(notTestCopy).
			Where(bindsFixedConnection, id).
			Order("created_at", "id").
			Limit(1).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("store: channel connection owner: %w", err)
		}
		return &ChannelConnectionTakenError{
			Binding: named[id], ConnectionID: id, ConnectorID: connectorID,
			OwnerID: owner.ID, OwnerName: owner.Name,
		}
	}
	return nil
}

// lockChannelOwner takes the lock a channel connection's owner is decided under
// (refuseSecondChannelAgent) when the live connection id is a channel connection, and returns
// its connector and whether it is one. A write that also locks the config's row takes this
// lock first, as UpdateAgentConfig does, or the two would wait on each other in a cycle.
func lockChannelOwner(ctx context.Context, tx bun.Tx, customerID, id string) (string, bool, error) {
	var connection ConnectorConnection
	err := tx.NewSelect().Model(&connection).Column("id", "connector_id", "definition_revision").
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("deleted_at IS NULL").
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		// refuseUnbindable answers a connection that is not live.
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: channel connection owner: %w", err)
	}
	definition, err := connectorDefinition(ctx, tx, customerID, connection.ConnectorID, connection.DefinitionRevision)
	if errors.Is(err, ErrNoConnectorDefinition) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if definition.Manifest.Channel == nil || definition.Manifest.Channel.Messages.IsZero() {
		return "", false, nil
	}
	if err := lockUntilCommit(ctx, tx, customerID+"/"+id, channelOwnerLockSeed); err != nil {
		return "", false, fmt.Errorf("store: channel connection owner: %w", err)
	}
	return connection.ConnectorID, true, nil
}

// AppConnectionByAccount returns the customer's live app-owned connection of a connector whose
// account id is accountID: the connection of the provider unit a provider app's event names,
// such as the bot token of the Slack workspace the app is installed in, whose identity is the
// team alone.
//
// A customer can hold more than one connection of the same account, such as a second connection
// to a Slack workspace that no agent uses yet (AI-3503). The answer is the connection that a live
// agent config binds, as AgentConfigsBindingConnection finds that config. Then a connected
// connection comes before one that is not, and a newer one before an older one. A connection that
// no config binds is the answer only when none is bound, and the caller then drops the message.
func (s *Store) AppConnectionByAccount(ctx context.Context, customerID, connectorID, accountID string) (ConnectorConnection, error) {
	if customerID == "" || connectorID == "" || accountID == "" {
		return ConnectorConnection{}, stack.Wrap(errors.New("store: a customer, a connector and an account id are required"))
	}
	bound := s.global.NewSelect().Model((*AgentConfig)(nil)).ColumnExpr("1").
		Where("ac.customer_id = cc.customer_id").
		Where("ac.deleted_at IS NULL").
		Where(notTestCopy).
		Where(bindsFixedConnection, bun.Ident("cc.id"))
	var connection ConnectorConnection
	err := s.global.NewSelect().Model(&connection).
		Where("customer_id = ?", customerID).
		Where("connector_id = ?", connectorID).
		Where("owner_type = ?", OwnerApp).
		Where("account_id = ?", accountID).
		Where("deleted_at IS NULL").
		OrderExpr("EXISTS (?) DESC", bound).
		OrderExpr("status = ? DESC", ConnectionConnected).
		Order("created_at DESC", "id DESC").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorConnection{}, stack.Wrap(fmt.Errorf("%w: %s account %s", ErrNoConnectorConnection, connectorID, accountID))
	}
	if err != nil {
		return ConnectorConnection{}, stack.Wrap(fmt.Errorf("store: app connection by account: %w", err))
	}
	return connection, nil
}

// bindsFixedConnection is the WHERE clause that finds the agent configs that bind a connection
// as their fixed connection, matched as boundByConfig matches them.
const bindsFixedConnection = "connectors @> jsonb_build_array(jsonb_build_object('connection', jsonb_build_object('type', 'fixed', 'connection_id', ?::text)))"

// AgentConfigsBindingConnection returns the customer's live agent configs that bind the
// connection as their fixed connection, oldest first: the agents that answer on the provider
// unit the connection is, matched as boundByConfig matches them. A test copy (DraftOfTag) is
// left out: it answers no channel message and subscribes to no event (AI-1049, AI-1048). The
// first is the connection's owner, which refuseSecondChannelAgent keeps the only one on a
// channel connection.
func (s *Store) AgentConfigsBindingConnection(ctx context.Context, customerID, connectionID string) ([]AgentConfig, error) {
	if customerID == "" || connectionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a connection id are required"))
	}
	var configs []AgentConfig
	err := s.global.NewSelect().Model(&configs).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Where(notTestCopy).
		Where(bindsFixedConnection, connectionID).
		Order("created_at", "id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: agent configs binding connection: %w", err))
	}
	return configs, nil
}

// ConfigOnChannel is the agent reachable on a number, and false when none is.
//
// It answers both questions there are about a line: which agent a message to it is for, and
// whether another agent has already claimed it. Two agents on one number would both answer
// every message, so the second one to ask for it is refused.
func (s *Store) ConfigOnChannel(ctx context.Context, customerID, kind, number string) (AgentConfig, bool, error) {
	if customerID == "" || kind == "" || number == "" {
		return AgentConfig{}, false, nil
	}

	var config AgentConfig
	err := s.global.NewSelect().Model(&config).
		Where("customer_id = ?", customerID).
		Where("channels_lines -> ? ->> 'number' = ?", kind, number).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentConfig{}, false, nil
	}
	if err != nil {
		return AgentConfig{}, false, stack.Wrap(fmt.Errorf("store: config on channel: %w", err))
	}
	return config, true, nil
}
