# Logging

Read before you log a value, or read one back out of a log.

## When to log

- Log only what lets someone act or debug a real case. No "starting X" or "done X" for routine work; a count is a metric.
- `Error`: the router failed, by its own fault or by losing work, and someone must act. A 5xx (`withRequestLog`), `could not claim the scheduled tasks due`.
- `Warn`: the router coped, by giving up on another party's failure or by serving degraded. `gave up an event forward`.
- `Info`: normal work, one line per request, session or turn. `served a request` (a 4xx too), `voice turn timing`.
- `Debug`: how one decision was reached; off unless `ROUTER_LOG_LEVEL=debug`. `transcribed`.
- One line per failure. An in-process retry loop logs no attempt, only the final outcome, at warn or error. A queued job's retry is info at most (`an event forward failed and is sent again`).
- Nothing per frame, chunk or token, at any level. Count it (`heardAudio` only counts) and log a change of state: once per reply (`leftNoted`), once per new judgement (`worthReporting`).
- A line that can repeat per key logs at most once a minute per key: `quota.Throttle` (`Due(key, time.Minute)`). Its map never shrinks: key it by customer or organization, never by a value a caller picks.

## What to log

- Name the ids that find the line `customer`, `session`, `call`, `turn` (on a job `task`, `run`) and the failure `error`. Not `customer_id`, `session_id`, `call_id` or `err`, which a few lines still use.
- Do not repeat what the logger carries: `customer`, `session`, `call` on the agent's (`agent.New`); `provider`, `model` on a provider's; `app_id`, `api_key` on a line logged with a request's context.
- Name any other field after what it holds (`connector`, not `id`) and keep its value short. There is no `component` field: the message names the thing.
- Never a payload, header or body, nor a whole map or struct. A transcript or prompt only at debug.

## Redaction

- Build every logger with `ReplaceAttr: store.RedactLogAttr`, as `newLogger` does (`cmd/router/main.go`). The price commands (`cmd/router/prices.go`) still build theirs without it: switch one to `newLogger` when you touch it, and do not copy them. `RedactLogAttr` redacts credential shapes (`internal/store/logs.go`) and masks phone numbers (`logMasks`, `logmask.go`) in the message and every value.
- It knows shapes only: `sk-...`, `ghp_...`, a JWT, `Bearer x`, `password=x`, a `+` and 7 digits. Log ids, never a secret.
- A number without `+` is masked only as the whole value of a `number`, `caller`, `to` or `from` attribute: log one under those names.
- The logger redacts only what is logged: a provider's error stored in a row or sent to a client goes through `store.SafeLogText`. Log a connector's error as `core.LoggedMessage(err, exchange.Redact)`.

## A value a tool reads back

- Redaction rewrites JSON logged as text. Hex-encode it: no rule matches `0-9a-f`, while base64 can spell `AKIA...` or `+1555...`. Example: `row_hex`, read by `router usage replay`.
- Mask before you hex-encode: encoding hides the value from the masks. Put each text field and map value through `store.RedactLogAttr` under its own name, as `loggable` does (`internal/routing/writer.go`).
- Leave out a value the reader acts on when masking changes it: `loggable` empties such a user id, so the replayed row bills its app with no user, not a wrong one.
- Anchor the reader on the field: the writer logs it before any text a customer wrote, and the reader matches it there, not the first `row_hex=` in the line, which a tag or an error can hold too (`lostFields`, `cmd/router/usage.go`).

## Correlation

- A request's id is `RequestIDFrom(ctx)` in `internal/api`, `core.CorrelationOf(ctx).RequestID` outside it. Every response sends it as `X-Request-Id`.
- Only the access and panic lines (`withRequestLog`) log it, as `request_id`. Add it to any other line a caller's report must find.
- Log with the request's context (`InfoContext`): `api.NewLogHandler` then adds `app_id` and `api_key`.

## Erasure

- `router usage forget-user` does not reach logs. Log ids, never a name, an email or a phone number.
