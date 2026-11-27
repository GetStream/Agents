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

Claiming a guest (`POST /v1/agents/guests/claim`) no longer waits on mem0. It queues a move of
what mem0 learned about the guest to the account that claimed them, and returns. The claim
succeeds whatever mem0 does. The move is a row of `guest_memory_moves`, one per guest of a
customer however often the claim is sent. Every router's sweep (`Manager.MoveGuestMemories`,
every 5 seconds) claims due moves one at a time, so one router runs a move at a time, and each
pass gets 5 minutes inside a 10-minute lease.

A move runs in passes. mem0 extracts what the guest said last after the claim, so passes run at
once, two minutes later and fifteen minutes after the claim, and every fifteen minutes after
that until a pass finds nothing under the guest. A session of the claimed guest that was still
open at the claim queues the move again when it ends, with its passes started over. A failed
pass is tried again after 30 seconds, doubling up to a day; a router stopping mid-pass makes it
due again without counting a failure. A move still failing, or still finding memories, a week
after the claim is given up with a warning and `router_guest_memory_moves_given_up_total`; what
is left stays under the guest.

mem0 cannot change a memory's `user_id`, so a pass adds each of the guest's memories under the
account as it is (`infer: false`), with its metadata, run and expiry. It deletes the original
once mem0 answers that it stored the copy (`status: SUCCEEDED`). An add without extraction
answers that once the memory is written, or once mem0 found the same memory there. Each copy
names its original in `moved_from`, a label a caller's memory filter may not name, so a pass
run again copies only what has no copy and deletes the originals of the copies already made.
The copies are billed as adds through the customer's metered store, under the claim's API key;
a pass that finds nothing writes no row. What mem0 filed under the agent has no user id and is
not moved: it goes with its session.

Erasure reaches the moves too:

- Truncating a user's memories deletes the moves naming them, as guest or as account, and
  truncates those guests' memories with theirs.
- Forgetting a user (`router usage forget-user`) deletes those moves first and truncates the
  guests' memories, which needs `MEM0_API_KEY`.
- A pass asks whether it still holds its move before it copies and before each deletion. A pass
  whose move was deleted under it takes back the copies it made.

## Not done

Only mem0 is implemented, which is what was asked; the contract is what a second provider
would implement.

- **Truncating a user leaves what mem0 filed under the agent.** mem0 attributes each fact to
  whoever stated it, so what the agent said carries the agent and run ids but no user id. It is
  never recalled, and deleting the session forgets it. Accepted.
- **What mem0 extracts for a guest more than about fifteen minutes after the claim, once a pass
  has found nothing left, stays under the guest.**
