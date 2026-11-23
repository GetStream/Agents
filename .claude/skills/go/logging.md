# Logging

Read before you log a value, or read one back out of a log.

## When to log

- Log only what lets someone act or debug a real case. No "starting X" or "done X" for routine work; a count is a metric.
- `Error`: the router failed, by its own fault or by losing work, and someone must act. A 5xx (`withRequestLog`), `usage row lost`.
- `Warn`: the router coped, by giving up on another party's failure or by serving degraded. `gave up an event forward`.
- `Info`: normal work, one line per request, session or turn. `served a request` (a 4xx too), `voice turn timing`.
- `Debug`: how one decision was reached; off unless `ROUTER_LOG_LEVEL=debug`. `transcribed`.
- One line per failure. An in-process retry loop logs no attempt, only the final outcome, at warn or error. A queued job's retry is info at most (`an event forward failed and is sent again`).
- Nothing per frame, chunk or token, at any level. Count it (`heardAudio` only counts) and log a change of state: once per reply (`leftNoted`), once per new judgement (`worthReporting`).
- A line that can repeat logs at most once a minute per key. Use `quota.Throttle`: `Due(key, time.Minute)`.
- The throttle's map never shrinks. Key it by customer or organization, never by a value a caller picks.

## What to log

- The ids that find a line are `customer`, `session`, `call` and `turn`. A job's are `task` and `run`. The failure is `error`.
- Some lines still use `customer_id`, `session_id`, `call_id` or `err`. Do not copy them.
- An end user's id is `user`, a call participant's `participant` (`bound` for the one a speech-to-speech model hears); `user_id` is the old name. The customer picks it and it is often an email, or `phone:` and a number for a channel user. Under these names the logger masks everything before an email's `@`, and it masks a `phone:` number anywhere. A bare numeric id is kept: its shape cannot tell it from an opaque one. A route's `{user_id}` segment is logged as `{user_id}`. Find a user's lines by `session`, `call` or `request_id`.
- Do not repeat what the logger carries: `customer`, `session`, `call` on the agent's (`agent.New`); `provider`, `model` on a provider's; `app_id`, `api_key` on a line logged with a request's context.
- Name any other field after what it holds (`connector`, not `id`) and keep its value short. There is no `component` field: the message names the thing.
- Never a request body, nor a header that can hold a secret. Never a whole map or struct, unless a tool reads it back (below).
- A connector logs the body of a provider's answer one way: `core.LoggedBody(exchange.ErrorBody())`, the cut, redacted start of an error body.
- Only at debug: a transcript, a prompt, a provider's frame that did not decode (`undecodable frame`).

## Redaction

- Build every logger with `ReplaceAttr: store.RedactLogAttr`, as every `newLogger` does. A JSON handler takes `store.RedactJSONLogAttr` (`cmd/phonecheck/main.go`).
- The price commands (`cmd/router/prices.go`) do not do it yet. Switch one to `newLogger` when you touch it.
- `RedactLogAttr` redacts credential shapes (`internal/store/logs.go`) and masks phone numbers (`logMasks`, `logmask.go`), in the message and every value.
- It knows shapes only: `sk-...`, `ghp_...`, a JWT, `Bearer x`, `password=x`, a `+` and 7 digits, `phone:` and 7 digits, and an email under an end user's id (below). Log ids, never a secret.
- A number without `+` is masked only after `phone:`, or as the whole value of a `number`, `caller`, `to` or `from` attribute: log one under those names.
- The logger redacts only what is logged. `errors.md` says what text may be stored or reach a caller.
- Text you store goes through `store.SafeLogText`, which also cuts it (past 8192 bytes, to 2048 runes).
- Text you must send whole, such as a refusal the caller can correct, goes through `store.RedactLogText`.
- Log a connector's error as `core.LoggedMessage(err, exchange.Redact)`.

## A value a tool reads back

- Redaction rewrites JSON logged as text. Hex-encode it: no rule matches `0-9a-f`, while base64 can spell `AKIA...` or `+1555...`. Example: `row_hex`, read by `router usage replay`.
- Mask before you hex-encode. Encoding hides the value from the masks.
- Put each text field and map value through `store.RedactLogAttr` under its own name, as `loggable` does (`internal/routing/writer.go`).
- Leave out a value the reader acts on when masking changes it: `loggable` empties such a user id, so the replayed row bills its app with no user, not a wrong one.
- Anchor the reader on the field. The writer logs it before any text a customer wrote.
- The reader matches the field there, not the first `row_hex=` in the line. A tag or an error can hold one too (`lostFields`, `cmd/router/usage.go`).

## Correlation

- A request's id is `RequestIDFrom(ctx)` in `internal/api`, `core.CorrelationOf(ctx).RequestID` outside it. Every response sends it as `X-Request-Id`.
- The access and panic lines (`withRequestLog`) log it, as `request_id`. Add it to any other line a caller's report must find.
- Log with the request's context (`InfoContext`): `api.NewLogHandler` then adds `app_id` and `api_key`.

## Erasure

- `router usage forget-user` does not reach logs. Log ids, never a name, an email or a phone number.
- A `phone:` id, an email under `user`, `user_id`, `participant`, `bound` or `id` (an audit resource such as `user:<app>/<user id>`), a route's `{user_id}` segment and a customer policy's `{id}` are masked before they are written, so they are never in a log to forget. Any other id, a bare number among them, stays until log retention ends.
- An email under any other name is not masked: log an end user's id only under those names.
