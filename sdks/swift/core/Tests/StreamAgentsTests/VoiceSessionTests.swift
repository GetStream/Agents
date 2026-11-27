import Foundation
import Testing

@testable import StreamAgents

/// Joins and leaves that overlap, caught at the first thing `join()` waits for: the token Stream
/// Video connects with, which the gate holds until the test lets it go. It lets go by failing,
/// since a token let through builds a `StreamVideo`, and that connects to Stream at once.
@MainActor
@Suite struct VoiceSessionTests {
    @Test func aJoinWhileOneIsUnderWayReturnsAtOnce() async throws {
        let gate = TokenGate()
        let voice = Self.voice(gate)
        let first = Task { await voice.join() }
        try #require(try await eventually { await gate.asked == 1 })

        let second = Returned()
        let joining = Task {
            await voice.join()
            second.yes = true
        }
        let returned = try await eventually { second.yes }
        await gate.fail()
        await first.value
        await joining.value

        #expect(returned)
        #expect(await gate.asked == 1)
        #expect(voice.call == nil)
        #expect(voice.failure is Refused)
    }

    @Test func leavingWhileAJoinIsUnderWayLeavesItWithNothingJoinedOrFailed() async throws {
        let gate = TokenGate()
        let voice = Self.voice(gate)
        let joining = Task { await voice.join() }
        try #require(try await eventually { await gate.asked == 1 })

        await voice.leave()
        await gate.fail()
        await joining.value

        #expect(voice.call == nil)
        #expect(voice.failure == nil)
        #expect(!voice.isCameraEnabled)
    }

    @Test func aJoinAfterLeavingAsksForNothing() async throws {
        let gate = TokenGate()
        let voice = Self.voice(gate)
        await voice.leave()

        let joined = Returned()
        let joining = Task {
            await voice.join()
            joined.yes = true
        }
        let returned = try await eventually { joined.yes }
        await gate.fail()
        await joining.value

        #expect(returned)
        #expect(await gate.asked == 0)
        #expect(voice.call == nil)
    }

    @Test func aJoinThatFailedCanBeTriedAgain() async throws {
        let gate = TokenGate()
        let voice = Self.voice(gate)
        let first = Task { await voice.join() }
        try #require(try await eventually { await gate.asked == 1 })
        await gate.fail()
        await first.value
        #expect(voice.failure is Refused)

        let again = Task { await voice.join() }
        let askedAgain = try await eventually { await gate.asked == 2 }
        await gate.fail()
        await again.value

        #expect(askedAgain)
        #expect(voice.call == nil)
    }

    /// A device that attached to a voice session and leaves stops following its conversation,
    /// so its socket closes, and leaves the session running for whoever started it.
    @Test func leavingASessionThisDeviceAttachedToClosesItsSocketAndLeavesItRunning() async throws {
        let server = try RouterServer()
        defer { server.listener.cancel() }
        let backend = Backend(url: try await server.url(), customerID: "acme")
        let session = AgentSession(
            backend: backend,
            session: Session(
                .init(
                    agentId: "a1", callId: "s1", callType: "agent", createdAt: Date(), id: "s1",
                    modality: .text, state: .live, text: true, userId: "jlahey")),
            tools: [])
        await session.start()
        try #require(session.isConnected)
        let voice = VoiceSession(agents: StreamAgents(backend: backend), session: session, createdLocally: false)

        await voice.leave()
        _ = try await StreamAgents(backend: backend).sessions.get("s1")

        #expect(!session.isConnected)
        let carried = try #require(try await server.closes.first { @Sendable _ in true }, "the socket is closed")
        #expect(carried.isEmpty, "nothing, such as a close command, is sent to end the session")
        var requests = server.requests.makeAsyncIterator()
        #expect(try await requests.next()?.path == "/v1/agents/sessions/s1/events")
        let next = try #require(try await requests.next())
        #expect(next.method == "GET", "the session is not stopped")
        #expect(next.path == "/v1/agents/sessions/s1")
    }

    /// A voice session on a session this device attached to, so leaving closes nothing, whose
    /// Stream Video token comes from `gate`.
    private static func voice(_ gate: TokenGate) -> VoiceSession {
        let backend = Backend(url: URL(string: "http://127.0.0.1:9")!, customerID: "test", apiKey: "key")
        backend.setUser(User(id: "john")) { try await gate.token() }
        let session = AgentSession(
            backend: backend,
            session: Session(
                .init(
                    agentId: "test", callId: "test", callType: "agent", createdAt: Date(), id: "test",
                    modality: .text, state: .live, text: true, userId: "john")),
            tools: [])
        return VoiceSession(agents: StreamAgents(backend: backend), session: session, createdLocally: false)
    }

    /// Waits until `done` holds, for five seconds at most, and says whether it did.
    private func eventually(_ done: () async -> Bool) async throws -> Bool {
        let deadline = ContinuousClock.now + .seconds(5)
        while ContinuousClock.now < deadline {
            if await done() { return true }
            try await Task.sleep(for: .milliseconds(10))
        }
        return false
    }
}

/// Holds every Stream Video token asked for until the test fails them, counting the asks.
private actor TokenGate {
    private(set) var asked = 0
    private var waiting: [CheckedContinuation<String, any Error>] = []

    func token() async throws -> String {
        asked += 1
        return try await withCheckedThrowingContinuation { waiting.append($0) }
    }

    func fail() {
        for token in waiting {
            token.resume(throwing: Refused())
        }
        waiting = []
    }
}

private struct Refused: Error {}

/// Whether a join has returned, read while it may still be held.
@MainActor
private final class Returned {
    var yes = false
}
