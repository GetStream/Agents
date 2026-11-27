# internal/chatinbound

Stream Chat's own delivery of messages to the router. `chatinbound.go` checks a delivery's signature. The endpoint (`api.receiveChatInbound`) deduplicates by `<app>:message:<id>` in `deliveries_seen` and enqueues a `chatinbound:dispatch_message` task onto the shard's `events` queue via `internal/async` (`internal/chatinbound/tasks`).

## Flow

```
endpoint                                    api.receiveChatInbound
  signature, app served here, message.new, addressed, fresh (app mode)
  deliveries_seen hook <app>:message:<id>   (dedupes with webhook + retries)
  enqueue chatinbound:dispatch_message      (shard `events` queue, UniqueKey)
  enqueued: 200
  key recorded before: 200, nothing queued  (webhook or earlier delivery owns it)
  store / enqueue error: 503                (Stream Chat delivers it again)

worker (router / router async-worker)       api.AnswerAsyncChatInbound -> routeArrivingMessage
  thread channel     answerThread claims the turn
  session here       answerOnce: Redis/in-memory fence, then answer
  worker here        answerOnce: Redis/in-memory fence, then AssignMessage
  session or worker  hand over; the peer runs the same steps
    on a peer
  error: asynq retries up to 5 times with backoff (5m timeout)
```

## Rules

- **200 means queued.** The endpoint answers 200 only after the delivery key is recorded and the `chatinbound:dispatch_message` task is enqueued, and 503 when either fails so Stream Chat retries.
- **One key per message, whoever brings it.** The endpoint and the message webhook record the same key, `<app>:message:<id>`, so the second of the two acts on nothing.
- **At-most-once answer settlement.** `answerOnce` claims the message fence (`chatInboundSettledKey`) in Redis (or in-memory when running without Redis) before handing to a session or dispatch worker. `unanswered` releases the fence only on failures proving nothing took the message (e.g., no dispatch worker connected).
- **The thread path is claimed by its turn.** `answerThread` claims the message's turn (`store.ChannelThreadMessages.Claim`, `store.ClaimTurn`).

## Tests

From `acceleration/`:

```bash
go test -race ./internal/chatinbound/...
go test -tags integration -run 'TestChatInbound' ./internal/api
```
