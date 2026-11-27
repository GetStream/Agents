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

## A claimed guest's memories move to the account

Claiming a guest (`POST /v1/agents/guests/claim`) moves what mem0 learned about them to the
account that claimed them. mem0 cannot change a memory's `user_id`, so each memory is added
again under the account as it is (`infer: false`), with its metadata, run and expiry, and the
original is deleted once its copy is listed: mem0 writes an add after answering it, and an
original deleted first would be lost if the add failed. Each copy names its original in
`moved_from`, so a claim that stopped part way and is sent again copies only what is missing
and deletes what is copied. An original whose copy is not listed within 30 seconds is kept
under the guest and logged.

The claim succeeds whatever mem0 does: a failure is logged, as joining the guest's channels
is, since the conversations have already moved. The copies are billed as adds through the
customer's metered store. What mem0 filed under the agent has no user id and is not moved: it
goes with its session.

## Not done

Only mem0 is implemented, which is what was asked; the contract is what a second provider
would implement.

- **Truncating a user leaves what mem0 filed under the agent.** mem0 attributes each fact to
  whoever stated it, so what the agent said carries the agent and run ids but no user id. It is
  never recalled, and deleting the session forgets it. Accepted.
- **A claim whose memory move failed is not retried.** The claim is finished once its rows and
  channels moved, so a sent-again claim does nothing more, and memories mem0 failed to move stay
  under the guest.
