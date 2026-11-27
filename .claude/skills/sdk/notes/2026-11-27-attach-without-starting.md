Swift's `attach(sessionID:tools:start:)` can return the session before its socket opens (`start: false`), and `start()` then follows it, with `failure` saying why it could not. What the other client SDKs need to check:

- **Watching an ended conversation is slow.** The router carries a stopped text conversation on when it is watched again, and the upgrade waits for that: 3.5 to 4.8 s on staging. Leaving a chat stops its session, so reopening any conversation from a list waits that long.
- **A conversation's history does not wait for its socket.** An app that reads the channel only once `attach` returns shows an empty conversation for those seconds, and none at all when the socket is refused, since `attach` throws then. Kotlin's `attach(sessionId, tools)`, JavaScript's and Dart's equivalents should let a caller get the session unstarted, read the channel, and start the socket alongside, as Swift's `AgentChat` in `swift_assistant` does.
- **The default stays as it was.** `attach` without the option still returns once the socket is open and throws, leaving the session running, when it is refused.

The Go SDK has no client session to attach to, so there is no Go change to follow.
