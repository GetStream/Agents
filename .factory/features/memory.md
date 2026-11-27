# Memory

[Sprint 4](../sprint4.md), "Memory", asked for again in [sprint 9](../sprint9.md). What an
agent looks up rather than remembers is [knowledge](knowledge.md).

## Asked for

Implement mem0 so agents have memory. Scope it by the API key or `app_id` using the backend
plus the `customer_id`. For calls without auth, do not store memory for now.

## What exists

[internal/memory](../../acceleration/internal/memory) is the contract — recall and remember —
and [mem0](../../acceleration/internal/memory/mem0) is the one provider. With `MEM0_API_KEY`
set, an agent recalls what it knows about the customer when it joins and prepends it to its
instructions, then hands each finished exchange over to be learned from.

Scoping is as asked: `app_id` is the deployment and `user_id` is the customer, so two
deployments sharing one mem0 account do not read each other's memories. Without an app id
nothing is stored, which is the "calls without auth" case. An end user's device may only open
its own memories and an anonymous caller none; a backend names any of its users.

There is one `MEM0_API_KEY`, never a pool of them: a mem0 key is scoped to one project, so
keys taken in turn would spread a user's memories across projects. The live suites run in the
router's integration workflow with that secret.

## Both directions stay off the conversation's path

- **Recall is bounded, and failing means starting empty.** An agent that cannot reach mem0
  starts the call knowing nothing rather than not taking the call. Memory improves a
  conversation; it is not a precondition for one.
- **Remembering is queued and dropped under backpressure.** The same rule as turns and
  transcripts: losing a memory costs the next call some context, while blocking costs this
  caller a silence.

Every call is recorded as a `requests` row with modality `memory`, so what memory costs shows
up in the same statistics as everything else. It is recorded but not routed: there is one store,
so the provider and route paths do not serve the modality while the statistics paths do.

## An edit forgets what the turns it takes back taught mem0

Each exchange is written with its turn id as metadata, `agent_turn_id`, beside the caller's
labels. Recall never filters on it, and a caller's memory filter may not name it (a 400). An edit
reads the turns it takes back from the stored conversation (`Responses.TakenBackTurnIDs`): those
of every answer to the edited question, its regenerations included, and of every response after
any of them, shown or hidden. Once the switch has moved the active path, the agent's memory
writer deletes what those turns taught mem0. It lists the session's memories once and deletes
those naming one of the turns, so a mem0 that ignored a filter could not delete more.

mem0 extracts after the add returned, sometimes ten minutes later. The writer therefore:

- drops an exchange of a forgotten turn still in its queue;
- deletes again 12 minutes later;
- deletes once more when the session closes, in place of that second pass.

A regenerate and a branch switch forget nothing. A branch brought back does not bring its
memories back.

## Not done

Only mem0 is implemented, which is what was asked; the contract is what a second provider
would implement.

- **What mem0 extracts of a forgotten turn after its session closed is kept.** The second
  deletion is the session's; a sweep that outlives it, from a durable queue of turns to forget,
  would find it.
- **Truncating a user leaves what mem0 filed under the agent.** mem0 attributes each fact to
  whoever stated it, so what the agent said carries the agent and run ids but no user id. It is
  never recalled, and deleting the session forgets it. Accepted.
- **Claiming a guest does not move its memories** to the account that claimed it.
  `TestAClaimedGuestsMemoriesMoveToTheUserWhoClaimedThem` is skipped in CI until it does.
